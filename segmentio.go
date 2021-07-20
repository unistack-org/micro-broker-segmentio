// Package kafka provides a kafka broker using segmentio
package segmentio

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
)

type kBroker struct {
	addrs []string

	readerConfig kafka.ReaderConfig
	writerConfig kafka.WriterConfig

	writer    *kafka.Writer
	connected bool
	init      bool
	sync.RWMutex
	opts     broker.Options
	messages []kafka.Message
}

type subscriber struct {
	topic      string
	opts       broker.SubscribeOptions
	closed     bool
	group      *kafka.ConsumerGroup
	cgcfg      kafka.ConsumerGroupConfig
	brokerOpts broker.Options
	sync.RWMutex
}

type publication struct {
	topic      string
	partition  int
	offset     int64
	err        error
	ackErr     atomic.Value
	msg        *broker.Message
	ackCh      chan map[string]map[int]int64
	readerDone *int32
	ackChMu    sync.Mutex
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.msg
}

func (p *publication) Ack() error {
	if atomic.LoadInt32(p.readerDone) == 1 {
		return fmt.Errorf("kafka reader done")
	}
	p.ackChMu.Lock()
	p.ackCh <- map[string]map[int]int64{p.topic: {p.partition: p.offset}}
	p.ackChMu.Unlock()
	if cerr := p.ackErr.Load(); cerr != nil {
		return cerr.(error)
	}
	return nil
}

func (p *publication) Error() error {
	return p.err
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe(ctx context.Context) error {
	var err error
	s.Lock()
	s.closed = true
	group := s.group
	s.Unlock()

	//fmt.Printf("unsub start\n")
	if group != nil {
		err = group.Close()
	}
	//fmt.Printf("unsub end\n")
	return err
}

func (k *kBroker) Address() string {
	if len(k.addrs) > 0 {
		return k.addrs[0]
	}
	return "127.0.0.1:9092"
}

func (k *kBroker) Name() string {
	return k.opts.Name
}

func (k *kBroker) Connect(ctx context.Context) error {
	k.RLock()
	if k.connected {
		k.RUnlock()
		return nil
	}
	k.RUnlock()
	dialCtx := k.opts.Context
	if ctx != nil {
		dialCtx = ctx
	}
	kaddrs := make([]string, 0, len(k.addrs))
	for _, addr := range k.addrs {
		conn, err := kafka.DialContext(dialCtx, "tcp", addr)
		if err != nil {
			continue
		}
		if _, err = conn.Brokers(); err != nil {
			if err = conn.Close(); err != nil {
				return err
			}
			continue
		}
		kaddrs = append(kaddrs, addr)
		if err = conn.Close(); err != nil {
			return err
		}
	}

	if len(kaddrs) == 0 {
		return fmt.Errorf("no available brokers: %v", k.addrs)
	}

	k.Lock()
	k.addrs = kaddrs
	k.readerConfig.Brokers = k.addrs
	k.writerConfig.Brokers = k.addrs
	k.connected = true
	k.Unlock()

	td := DefaultStatsInterval
	if v, ok := k.opts.Context.Value(statsIntervalKey{}).(time.Duration); ok && td > 0 {
		td = v
	}

	go writerStats(k.opts.Context, k.writer, td, k.opts.Meter)

	if k.writer.Async {
		go k.writeLoop()
	}

	return nil
}

func (k *kBroker) writeLoop() {
	var err error

	ticker := time.NewTicker(k.writer.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-k.opts.Context.Done():
			return
		case <-ticker.C:
			k.RLock()
			if len(k.messages) != 0 {
				err = k.writer.WriteMessages(k.opts.Context, k.messages...)
			}
			k.RUnlock()
			if err == nil {
				k.Lock()
				k.messages = k.messages[0:0]
				k.Unlock()
			} else {
				if k.opts.Logger.V(logger.ErrorLevel) {
					k.opts.Logger.Errorf(k.opts.Context, "[segmentio] publish error %v", err)
				}
			}
		}
	}
}

func (k *kBroker) Disconnect(ctx context.Context) error {
	k.RLock()
	if !k.connected {
		k.RUnlock()
		return nil
	}
	k.RUnlock()

	k.Lock()
	defer k.Unlock()
	if err := k.writer.Close(); err != nil {
		return err
	}

	k.connected = false
	return nil
}

func (k *kBroker) Init(opts ...broker.Option) error {
	if len(opts) == 0 && k.init {
		return nil
	}
	return k.configure(opts...)
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	var val []byte
	var err error

	options := broker.NewPublishOptions(opts...)

	if options.BodyOnly {
		val = msg.Body
	} else {
		val, err = k.opts.Codec.Marshal(msg)
		if err != nil {
			return err
		}
	}
	kmsg := kafka.Message{Topic: topic, Value: val}
	if options.Context != nil {
		if key, ok := options.Context.Value(publishKey{}).([]byte); ok && len(key) > 0 {
			kmsg.Key = key
		}
	}

	if k.writer.Async {
		k.Lock()
		k.messages = append(k.messages, kmsg)
		k.Unlock()
		return nil
	}

	wCtx := k.opts.Context
	if ctx != nil {
		wCtx = ctx
	}
	return k.writer.WriteMessages(wCtx, kmsg)
}

func (k *kBroker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	opt := broker.NewSubscribeOptions(opts...)

	if opt.Group == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		opt.Group = id.String()
	}

	cgcfg := kafka.ConsumerGroupConfig{
		ID:                    opt.Group,
		WatchPartitionChanges: true,
		Brokers:               k.readerConfig.Brokers,
		Topics:                []string{topic},
		GroupBalancers:        k.readerConfig.GroupBalancers,
		StartOffset:           k.readerConfig.StartOffset,
		Logger:                k.readerConfig.Logger,
		ErrorLogger:           k.readerConfig.ErrorLogger,
		Dialer:                k.readerConfig.Dialer,
	}
	if err := cgcfg.Validate(); err != nil {
		return nil, err
	}
	gCtx := k.opts.Context
	if ctx != nil {
		gCtx = ctx
	}

	sub := &subscriber{brokerOpts: k.opts, opts: opt, topic: topic, cgcfg: cgcfg}
	sub.createGroup(gCtx)

	go func() {
		defer func() {
			sub.RLock()
			closed := sub.closed
			sub.RUnlock()
			if !closed {
				if err := sub.group.Close(); err != nil {
					k.opts.Logger.Errorf(k.opts.Context, "[segmentio] consumer group close error %v", err)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				sub.RLock()
				closed := sub.closed
				sub.RUnlock()
				if closed {
					return
				}
				if k.opts.Context.Err() != nil && k.opts.Logger.V(logger.ErrorLevel) {
					k.opts.Logger.Errorf(k.opts.Context, "[segmentio] subscribe context closed %v", k.opts.Context.Err())
				}
				return
			case <-k.opts.Context.Done():
				sub.RLock()
				closed := sub.closed
				sub.RUnlock()
				if closed {
					return
				}
				if k.opts.Context.Err() != nil && k.opts.Logger.V(logger.ErrorLevel) {
					k.opts.Logger.Errorf(k.opts.Context, "[segmentio] broker context closed error %v", k.opts.Context.Err())
				}
				return
			default:
				sub.RLock()
				closed := sub.closed
				sub.RUnlock()
				if closed {
					return
				}
				generation, err := sub.group.Next(gCtx)
				switch err {
				case nil:
					// normal execution
				case kafka.ErrGroupClosed:
					k.opts.Logger.Tracef(k.opts.Context, "group closed %v", err)
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if closed {
						return
					}
					if k.opts.Logger.V(logger.ErrorLevel) {
						k.opts.Logger.Errorf(k.opts.Context, "[segmentio] recreate consumer group, as it closed by kafka %v", k.opts.Context.Err())
					}
					sub.createGroup(gCtx)
					continue
				default:
					k.opts.Logger.Tracef(k.opts.Context, "some error: %v", err)
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if closed {
						return
					}
					if k.opts.Logger.V(logger.TraceLevel) {
						k.opts.Logger.Tracef(k.opts.Context, "[segmentio] recreate consumer group, as unexpected consumer error %T %v", err, err)
					}
					sub.createGroup(gCtx)
					continue
				}

				ackCh := make(chan map[string]map[int]int64, DefaultCommitQueueSize)
				errChLen := 0
				for _, assignments := range generation.Assignments {
					errChLen += len(assignments)
				}
				errChs := make([]chan error, 0, errChLen)

				commitDoneCh := make(chan bool)
				readerDone := int32(0)
				cntWait := int32(0)

				for topic, assignments := range generation.Assignments {
					if k.opts.Logger.V(logger.TraceLevel) {
						k.opts.Logger.Tracef(k.opts.Context, "topic: %s assignments: %v", topic, assignments)
					}
					for _, assignment := range assignments {
						cfg := k.readerConfig
						cfg.Topic = topic
						cfg.Partition = assignment.ID
						cfg.GroupID = ""
						reader := kafka.NewReader(cfg)

						if err := reader.SetOffset(assignment.Offset); err != nil {
							if k.opts.Logger.V(logger.ErrorLevel) {
								k.opts.Logger.Errorf(k.opts.Context, "assignments offset %d can be set by reader: %v", assignment.Offset, err)
							}
							if err = reader.Close(); err != nil {
								if k.opts.Logger.V(logger.ErrorLevel) {
									k.opts.Logger.Errorf(k.opts.Context, "reader close err: %v", err)
								}
							}
							continue
						}
						errCh := make(chan error)
						errChs = append(errChs, errCh)
						cgh := &cgHandler{
							brokerOpts:   k.opts,
							subOpts:      opt,
							reader:       reader,
							handler:      handler,
							ackCh:        ackCh,
							errCh:        errCh,
							cntWait:      &cntWait,
							readerDone:   &readerDone,
							commitDoneCh: commitDoneCh,
						}
						atomic.AddInt32(cgh.cntWait, 1)
						generation.Start(cgh.run)
					}
				}
				if k.opts.Logger.V(logger.TraceLevel) {
					k.opts.Logger.Trace(k.opts.Context, "start async commit loop")
				}
				// run async commit loop
				go k.commitLoop(generation, k.readerConfig.CommitInterval, ackCh, errChs, &readerDone, commitDoneCh, &cntWait)
			}
		}
	}()

	return sub, nil
}

type cgHandler struct {
	brokerOpts   broker.Options
	subOpts      broker.SubscribeOptions
	reader       *kafka.Reader
	handler      broker.Handler
	ackCh        chan map[string]map[int]int64
	errCh        chan error
	readerDone   *int32
	commitDoneCh chan bool
	cntWait      *int32
}

func (k *kBroker) commitLoop(generation *kafka.Generation, commitInterval time.Duration, ackCh chan map[string]map[int]int64, errChs []chan error, readerDone *int32, commitDoneCh chan bool, cntWait *int32) {

	if k.opts.Logger.V(logger.TraceLevel) {
		k.opts.Logger.Trace(k.opts.Context, "start commit loop")
	}

	td := DefaultCommitInterval

	if commitInterval > 0 {
		td = commitInterval
	}

	if v, ok := k.opts.Context.Value(commitIntervalKey{}).(time.Duration); ok && td > 0 {
		td = v
	}

	var mapMu sync.Mutex
	offsets := make(map[string]map[int]int64, 4)

	go func() {
		defer func() {
			for _, errCh := range errChs {
				close(errCh)
			}
			close(commitDoneCh)
		}()

		for {
			select {
			case ack := <-ackCh:
				if k.opts.Logger.V(logger.TraceLevel) {
					k.opts.Logger.Tracef(k.opts.Context, "new commit offsets: %v", ack)
				}
				switch td {
				case 0: // sync commits as CommitInterval == 0
					if len(ack) > 0 {
						err := generation.CommitOffsets(ack)
						if err != nil {
							for _, errCh := range errChs {
								errCh <- err
							}
							return
						}
					}
				default: // async commits as CommitInterval > 0
					mapMu.Lock()
					for t, p := range ack {
						if _, ok := offsets[t]; !ok {
							offsets[t] = make(map[int]int64, 4)
						}
						for k, v := range p {
							offsets[t][k] = v
						}
					}
					mapMu.Unlock()
				}
				// check for readers done and commit offsets
				if atomic.LoadInt32(cntWait) == 0 {
					//fmt.Printf("cntWait IS 0\n")
					mapMu.Lock()
					if len(offsets) > 0 {
						if err := generation.CommitOffsets(offsets); err != nil {
							for _, errCh := range errChs {
								errCh <- err
							}
							return
						}
					}
					mapMu.Unlock()
					if k.opts.Logger.V(logger.TraceLevel) {
						k.opts.Logger.Trace(k.opts.Context, "stop commit loop")
					}

					return
				}
				//fmt.Printf("cntWait NOT 0\n")
			}
		}
	}()

	// async commit loop
	if td > 0 {
		ticker := time.NewTicker(td)
		doneTicker := time.NewTicker(300 * time.Millisecond)
		defer doneTicker.Stop()

		for {
			select {
			case <-doneTicker.C:
				if atomic.LoadInt32(readerDone) == 1 {
					mapMu.Lock()
					if len(offsets) == 0 {
						//fmt.Printf("close all on <-readerDoneCh\n")
						defer ticker.Stop()
						return
					}
					ticker.Stop()
				}
			case <-ticker.C:
				mapMu.Lock()
				if len(offsets) == 0 {
					mapMu.Unlock()
					continue
				}
				if k.opts.Logger.V(logger.TraceLevel) {
					k.opts.Logger.Tracef(k.opts.Context, "async commit offsets: %v", offsets)
				}
				err := generation.CommitOffsets(offsets)
				if err != nil {
					for _, errCh := range errChs {
						errCh <- err
					}
					mapMu.Unlock()
					return
				}
				offsets = make(map[string]map[int]int64, 4)
				mapMu.Unlock()
				if atomic.LoadInt32(readerDone) == 1 && atomic.LoadInt32(cntWait) == 0 {
					//fmt.Printf("close all on <-ticker.C\n")
					return
				}
			}
		}
	}
}

func (h *cgHandler) run(ctx context.Context) {
	if h.brokerOpts.Logger.V(logger.TraceLevel) {
		h.brokerOpts.Logger.Trace(ctx, "start partition reader")
	}

	var ackChMu sync.Mutex

	td := DefaultStatsInterval
	if v, ok := h.brokerOpts.Context.Value(statsIntervalKey{}).(time.Duration); ok && td > 0 {
		td = v
	}

	// start stats loop
	go readerStats(ctx, h.reader, td, h.brokerOpts.Meter)

	var commitErr atomic.Value

	defer func() {
		atomic.AddInt32(h.cntWait, -1)

		ackChMu.Lock()
		if atomic.CompareAndSwapInt32(h.readerDone, 0, 1) {
			close(h.ackCh)
		}
		ackChMu.Unlock()
		if err := h.reader.Close(); err != nil && h.brokerOpts.Logger.V(logger.ErrorLevel) {
			h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio] reader close error: %v", err)
		}
		<-h.commitDoneCh
		//fmt.Printf("<-h.commitDoneCh\n")
		if h.brokerOpts.Logger.V(logger.TraceLevel) {
			h.brokerOpts.Logger.Trace(ctx, "stop partition reader")
		}
	}()

	go func() {
		for {
			select {
			case err := <-h.errCh:
				if err != nil {
					commitErr.Store(err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		msg, err := h.reader.ReadMessage(ctx)
		switch err {
		default:
			if h.brokerOpts.Logger.V(logger.ErrorLevel) {
				h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio] unexpected error type: %T err: %v", err, err)
			}
			//fmt.Printf("exit from readMessage loop\n")
			return
		case kafka.ErrGenerationEnded:
			// generation has ended
			if h.brokerOpts.Logger.V(logger.TraceLevel) {
				h.brokerOpts.Logger.Trace(h.brokerOpts.Context, "[segmentio] generation ended, rebalance or close")
			}
			//fmt.Printf("exit from readMessage loop\n")
			return
		case nil:
			if cerr := commitErr.Load(); cerr != nil {
				if h.brokerOpts.Logger.V(logger.ErrorLevel) {
					h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio] commit error: %v", cerr)
				}
				//fmt.Printf("exit from readMessage loop\n")
				return
			}

			eh := h.brokerOpts.ErrorHandler

			if h.subOpts.ErrorHandler != nil {
				eh = h.subOpts.ErrorHandler
			}
			p := &publication{ackCh: h.ackCh, partition: msg.Partition, offset: msg.Offset + 1, topic: msg.Topic, msg: &broker.Message{}, readerDone: h.readerDone, ackChMu: ackChMu}

			if h.subOpts.BodyOnly {
				p.msg.Body = msg.Value
			} else {
				if err := h.brokerOpts.Codec.Unmarshal(msg.Value, p.msg); err != nil {
					p.err = err
					p.msg.Body = msg.Value
					if eh != nil {
						_ = eh(p)
					} else {
						if h.brokerOpts.Logger.V(logger.ErrorLevel) {
							h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: failed to unmarshal: %v", err)
						}
					}
					continue
				}
			}
			if cerr := commitErr.Load(); cerr != nil {
				p.ackErr.Store(cerr.(bool))
			}
			err = h.handler(p)
			if err == nil && h.subOpts.AutoAck {
				if err = p.Ack(); err != nil {
					if h.brokerOpts.Logger.V(logger.ErrorLevel) {
						h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: message ack error: %v", err)
						return
					}
				}
			} else if err != nil {
				p.err = err
				if eh != nil {
					_ = eh(p)
				} else {
					if h.brokerOpts.Logger.V(logger.ErrorLevel) {
						h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: subscriber error: %v", err)
					}
				}
			}
		}
	}
}

func (sub *subscriber) createGroup(ctx context.Context) {
	sub.RLock()
	cgcfg := sub.cgcfg
	sub.RUnlock()

	for {
		select {
		case <-ctx.Done():
			// closed
			return
		default:
			cgroup, err := kafka.NewConsumerGroup(cgcfg)
			if err != nil {
				if sub.brokerOpts.Logger.V(logger.ErrorLevel) {
					sub.brokerOpts.Logger.Errorf(sub.brokerOpts.Context, "[segmentio]: consumer group error %v", err)
				}
				continue
			}
			sub.Lock()
			sub.group = cgroup
			sub.Unlock()
			// return
			return
		}
	}
}

func (k *kBroker) String() string {
	return "kafka"
}

func (k *kBroker) configure(opts ...broker.Option) error {
	for _, o := range opts {
		o(&k.opts)
	}

	if err := k.opts.Register.Init(); err != nil {
		return err
	}
	if err := k.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := k.opts.Logger.Init(); err != nil {
		return err
	}
	if err := k.opts.Meter.Init(); err != nil {
		return err
	}

	var cAddrs []string
	for _, addr := range k.opts.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{"127.0.0.1:9092"}
	}

	readerConfig := DefaultReaderConfig
	if cfg, ok := k.opts.Context.Value(readerConfigKey{}).(kafka.ReaderConfig); ok {
		readerConfig = cfg
	}
	if len(readerConfig.Brokers) == 0 {
		readerConfig.Brokers = cAddrs
	}
	readerConfig.WatchPartitionChanges = true

	writerConfig := DefaultWriterConfig
	if cfg, ok := k.opts.Context.Value(writerConfigKey{}).(kafka.WriterConfig); ok {
		writerConfig = cfg
	}
	if len(writerConfig.Brokers) == 0 {
		writerConfig.Brokers = cAddrs
	}
	k.addrs = cAddrs
	k.readerConfig = readerConfig
	k.writer = &kafka.Writer{
		Addr:         kafka.TCP(k.addrs...),
		Balancer:     writerConfig.Balancer,
		MaxAttempts:  writerConfig.MaxAttempts,
		BatchSize:    writerConfig.BatchSize,
		BatchBytes:   int64(writerConfig.BatchBytes),
		BatchTimeout: writerConfig.BatchTimeout,
		ReadTimeout:  writerConfig.ReadTimeout,
		WriteTimeout: writerConfig.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(writerConfig.RequiredAcks),
		Async:        writerConfig.Async,
		//Completion:   writerConfig.Completion,
		//Compression:  writerConfig.Compression,
		Logger:      writerConfig.Logger,
		ErrorLogger: writerConfig.ErrorLogger,
		//Transport:    writerConfig.Transport,
	}
	if id, ok := k.opts.Context.Value(clientIDKey{}).(string); ok {
		if k.readerConfig.Dialer == nil {
			k.readerConfig.Dialer = kafka.DefaultDialer
		}
		k.readerConfig.Dialer.ClientID = id
	}

	if fn, ok := k.opts.Context.Value(writerCompletionFunc{}).(func([]kafka.Message, error)); ok {
		k.writer.Completion = fn
	}

	k.init = true
	return nil
}

func NewBroker(opts ...broker.Option) broker.Broker {
	return &kBroker{
		opts: broker.NewOptions(opts...),
	}
}
