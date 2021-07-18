// Package kafka provides a kafka broker using segmentio
package segmentio

import (
	"context"
	"fmt"
	"sync"
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
	done       chan struct{}
	group      *kafka.ConsumerGroup
	cgcfg      kafka.ConsumerGroupConfig
	brokerOpts broker.Options
	sync.RWMutex
}

type publication struct {
	topic     string
	partition int
	offset    int64
	err       error
	ackErr    *error
	msg       *broker.Message
	ackCh     chan map[string]map[int]int64
	sync.Mutex
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.msg
}

func (p *publication) Ack() error {
	p.ackCh <- map[string]map[int]int64{p.topic: {p.partition: p.offset}}
	return *p.ackErr
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
	close(s.done)
	s.Unlock()

	if group != nil {
		err = group.Close()
	}
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
	}
	if err := cgcfg.Validate(); err != nil {
		return nil, err
	}

	cgroup, err := kafka.NewConsumerGroup(cgcfg)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{brokerOpts: k.opts, opts: opt, topic: topic, group: cgroup, cgcfg: cgcfg, done: make(chan struct{})}
	go func() {
		for {
			select {
			case <-sub.done:
				return
			case <-ctx.Done():
				// unexpected context closed
				if k.opts.Context.Err() != nil && k.opts.Logger.V(logger.ErrorLevel) {
					k.opts.Logger.Errorf(k.opts.Context, "[segmentio] context closed unexpected %v", k.opts.Context.Err())
				}
				return
			case <-k.opts.Context.Done():
				sub.RLock()
				closed := sub.closed
				sub.RUnlock()
				if closed {
					// unsubcribed and closed
					return
				}
				// unexpected context closed
				if k.opts.Context.Err() != nil && k.opts.Logger.V(logger.ErrorLevel) {
					k.opts.Logger.Errorf(k.opts.Context, "[segmentio] context closed unexpected %v", k.opts.Context.Err())
				}
				return
			default:
				sub.RLock()
				group := sub.group
				closed := sub.closed
				sub.RUnlock()
				if closed {
					return
				}
				gCtx := k.opts.Context
				if ctx != nil {
					gCtx = ctx
				}
				generation, err := group.Next(gCtx)
				switch err {
				case nil:
					// normal execution
				case kafka.ErrGroupClosed:
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if !closed {
						if k.opts.Logger.V(logger.ErrorLevel) {
							k.opts.Logger.Errorf(k.opts.Context, "[segmentio] recreate consumer group, as it closed by kafka %v", k.opts.Context.Err())
						}
						if err = group.Close(); err != nil && k.opts.Logger.V(logger.ErrorLevel) {
							k.opts.Logger.Errorf(k.opts.Context, "[segmentio] consumer group close error %v", err)
							continue
						}
						sub.createGroup(gCtx)
						continue
					}
					return
				default:
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if !closed {
						if k.opts.Logger.V(logger.TraceLevel) {
							k.opts.Logger.Tracef(k.opts.Context, "[segmentio] recreate consumer group, as unexpected consumer error %T %v", err, err)
						}
					}
					if err = group.Close(); err != nil && k.opts.Logger.V(logger.ErrorLevel) {
						k.opts.Logger.Errorf(k.opts.Context, "[segmentio] consumer group close error %v", err)
					}
					sub.createGroup(gCtx)
					continue
				}

				var wg sync.WaitGroup

				ackCh := make(chan map[string]map[int]int64, DefaultCommitQueueSize)
				errChLen := 0
				for _, assignments := range generation.Assignments {
					errChLen += len(assignments)
				}
				errChs := make([]chan error, errChLen)

				for topic, assignments := range generation.Assignments {
					if k.opts.Logger.V(logger.TraceLevel) {
						k.opts.Logger.Tracef(k.opts.Context, "topic: %s assignments: %v", topic, assignments)
					}
					for _, assignment := range assignments {
						errCh := make(chan error)
						cfg := k.readerConfig
						cfg.Topic = topic
						cfg.Partition = assignment.ID
						cfg.GroupID = ""
						reader := kafka.NewReader(cfg)

						if err := reader.SetOffset(assignment.Offset); err != nil {
							if k.opts.Logger.V(logger.ErrorLevel) {
								k.opts.Logger.Errorf(k.opts.Context, "assignments offset %d can be set by reader: %v", assignment.Offset, err)
							}
							continue
						}
						cgh := &cgHandler{brokerOpts: k.opts, subOpts: opt, reader: reader, handler: handler, ackCh: ackCh, errCh: errCh, wg: &wg}
						generation.Start(cgh.run)
					}
				}
				if k.opts.Logger.V(logger.TraceLevel) {
					k.opts.Logger.Trace(k.opts.Context, "start async commit loop")
				}
				// run async commit loop
				go k.commitLoop(generation, ackCh, errChs, &wg)
			}
		}
	}()

	return sub, nil
}

type cgHandler struct {
	brokerOpts broker.Options
	subOpts    broker.SubscribeOptions
	reader     *kafka.Reader
	handler    broker.Handler
	ackCh      chan map[string]map[int]int64
	errCh      chan error
	wg         *sync.WaitGroup
}

func (k *kBroker) commitLoop(generation *kafka.Generation, ackCh chan map[string]map[int]int64, errChs []chan error, wg *sync.WaitGroup) {
	var mapMu sync.Mutex

	td := DefaultCommitInterval
	if v, ok := k.opts.Context.Value(commitIntervalKey{}).(time.Duration); ok && td > 0 {
		td = v
	}

	ticker := time.NewTicker(td)
	defer ticker.Stop()

	offsets := make(map[string]map[int]int64, 4)

	for {
		select {
		default:
			wg.Wait()
			if k.opts.Logger.V(logger.TraceLevel) {
				k.opts.Logger.Trace(k.opts.Context, "all readers are done, return from commit loop")
			}
			return
		case ack := <-ackCh:
			mapMu.Lock()
			for k, v := range ack {
				if _, ok := offsets[k]; !ok {
					offsets[k] = make(map[int]int64, 4)
				}
				for p, o := range v {
					offsets[k][p] = o + 1
				}
			}
			mapMu.Unlock()
		case <-ticker.C:
			mapMu.Lock()
			if len(offsets) == 0 {
				mapMu.Unlock()
				continue
			}
			if k.opts.Logger.V(logger.TraceLevel) {
				k.opts.Logger.Tracef(k.opts.Context, "commit offsets: %v", offsets)
			}
			err := generation.CommitOffsets(offsets)
			if err != nil {
				for _, errCh := range errChs {
					errCh <- err
					close(errCh)
				}
				mapMu.Unlock()
				return
			}
			mapMu.Unlock()
			offsets = make(map[string]map[int]int64, 4)
		}
	}
}

func (h *cgHandler) run(ctx context.Context) {
	td := DefaultStatsInterval
	if v, ok := h.brokerOpts.Context.Value(statsIntervalKey{}).(time.Duration); ok && td > 0 {
		td = v
	}

	go readerStats(ctx, h.reader, td, h.brokerOpts.Meter)

	commitDuration := DefaultCommitInterval
	if v, ok := h.brokerOpts.Context.Value(commitIntervalKey{}).(time.Duration); ok && td > 0 {
		commitDuration = v
	}

	var commitErr error

	h.wg.Add(1)

	defer func() {
		h.wg.Done()
		if err := h.reader.Close(); err != nil && h.brokerOpts.Logger.V(logger.ErrorLevel) {
			h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio] reader close error: %v", err)
		}
	}()

	go func() {
		for {
			select {
			case err := <-h.errCh:
				commitErr = err
			case <-ctx.Done():
				time.Sleep(commitDuration)
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
			return
		case kafka.ErrGenerationEnded:
			// generation has ended
			if h.brokerOpts.Logger.V(logger.TraceLevel) {
				h.brokerOpts.Logger.Trace(h.brokerOpts.Context, "[segmentio] generation ended, rebalance")
			}
			return
		case nil:
			eh := h.brokerOpts.ErrorHandler

			if h.subOpts.ErrorHandler != nil {
				eh = h.subOpts.ErrorHandler
			}
			p := &publication{ackCh: h.ackCh, partition: msg.Partition, offset: msg.Offset, topic: msg.Topic, msg: &broker.Message{}}

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
			p.Lock()
			p.ackErr = &commitErr
			p.Unlock()
			err = h.handler(p)
			if err == nil && h.subOpts.AutoAck {
				if err = p.Ack(); err != nil {
					if h.brokerOpts.Logger.V(logger.ErrorLevel) {
						h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: unable to commit msg: %v", err)
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
