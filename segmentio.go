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

	writers map[string]*kafka.Writer

	connected bool
	init      bool
	sync.RWMutex
	opts broker.Options
}

type subscriber struct {
	k          *kBroker
	topic      string
	opts       broker.SubscribeOptions
	offset     int64
	gen        *kafka.Generation
	partition  int
	handler    broker.Handler
	reader     *kafka.Reader
	closed     bool
	done       chan struct{}
	group      *kafka.ConsumerGroup
	cgcfg      kafka.ConsumerGroupConfig
	brokerOpts broker.Options
	sync.RWMutex
}

type publication struct {
	topic      string
	err        error
	m          *broker.Message
	opts       broker.Options
	ctx        context.Context
	generation *kafka.Generation
	reader     *kafka.Reader
	km         kafka.Message
	offsets    map[string]map[int]int64 // for commit offsets
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *publication) Ack() error {
	if p.opts.Logger.V(logger.TraceLevel) {
		p.opts.Logger.Tracef(p.opts.Context, "commit offset %#+v\n", p.offsets)
	}
	return p.generation.CommitOffsets(p.offsets)
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
	defer s.Unlock()
	if s.group != nil {
		err = s.group.Close()
	}
	s.closed = true
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

	return nil
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
	for _, writer := range k.writers {
		if err := writer.Close(); err != nil {
			return err
		}
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
	var cached bool

	options := broker.NewPublishOptions(opts...)

	val, err := k.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	kmsg := kafka.Message{Value: val}
	if options.Context != nil {
		if key, ok := options.Context.Value(publishKey{}).([]byte); ok && len(key) > 0 {
			kmsg.Key = key
		}
	}

	k.Lock()
	writer, ok := k.writers[topic]
	if !ok {
		cfg := k.writerConfig
		cfg.Topic = topic
		if err = cfg.Validate(); err != nil {
			k.Unlock()
			return err
		}
		writer = kafka.NewWriter(cfg)
		k.writers[topic] = writer
	} else {
		cached = true
	}
	k.Unlock()
	wCtx := k.opts.Context
	if ctx != nil {
		wCtx = ctx
	}
	err = writer.WriteMessages(wCtx, kmsg)
	if err != nil {
		if k.opts.Logger.V(logger.TraceLevel) {
			k.opts.Logger.Tracef(k.opts.Context, "write message err: %v", err)
		}
		switch cached {
		case false:
			// non cached case, we can try to wait on some errors, but not timeout
			if kerr, ok := err.(kafka.Error); ok {
				if kerr.Temporary() && !kerr.Timeout() {
					// additional chanse to publish message
					time.Sleep(200 * time.Millisecond)
					err = writer.WriteMessages(wCtx, kmsg)
				}
			}
		case true:
			// cached case, try to recreate writer and try again after that
			k.Lock()
			// close older writer to free memory
			if err = writer.Close(); err != nil {
				k.Unlock()
				return err
			}
			delete(k.writers, topic)
			k.Unlock()

			cfg := k.writerConfig
			cfg.Topic = topic
			if err = cfg.Validate(); err != nil {
				return err
			}
			writer := kafka.NewWriter(cfg)
			if err = writer.WriteMessages(wCtx, kmsg); err == nil {
				k.Lock()
				k.writers[topic] = writer
				k.Unlock()
			}
		}
	}

	return err
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
		GroupBalancers:        []kafka.GroupBalancer{kafka.RangeGroupBalancer{}},
	}
	if err := cgcfg.Validate(); err != nil {
		return nil, err
	}

	cgroup, err := kafka.NewConsumerGroup(cgcfg)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{brokerOpts: k.opts, opts: opt, topic: topic, group: cgroup, cgcfg: cgcfg}
	go func() {
		for {
			select {
			case <-ctx.Done():
				sub.RLock()
				closed := sub.closed
				sub.RUnlock()
				if closed {
					// unsubcribed and closed
					return
				}
				// unexpected context closed
				if k.opts.Context.Err() != nil {
					if k.opts.Logger.V(logger.TraceLevel) {
						k.opts.Logger.Tracef(k.opts.Context, "[segmentio] context closed unexpected %v", k.opts.Context.Err())
					}
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
				if k.opts.Context.Err() != nil {
					if k.opts.Logger.V(logger.TraceLevel) {
						k.opts.Logger.Tracef(k.opts.Context, "[segmentio] context closed unexpected %v", k.opts.Context.Err())
					}
				}
				return
			default:
				sub.RLock()
				group := sub.group
				sub.RUnlock()
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
						if k.opts.Logger.V(logger.TraceLevel) {
							k.opts.Logger.Tracef(k.opts.Context, "[segmentio] recreate consumer group, as it closed %v", k.opts.Context.Err())
						}
						if err = group.Close(); err != nil {
							if k.opts.Logger.V(logger.TraceLevel) {
								k.opts.Logger.Tracef(k.opts.Context, "[segmentio] consumer group close error %v", err)
							}
							continue
						}
						sub.createGroup(gCtx)
					}
					continue
				default:
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if !closed {
						if k.opts.Logger.V(logger.TraceLevel) {
							k.opts.Logger.Tracef(k.opts.Context, "[segmentio] recreate consumer group, as unexpected consumer error %v", err)
						}
					}
					if err = group.Close(); err != nil {
						if k.opts.Logger.V(logger.ErrorLevel) {
							k.opts.Logger.Tracef(k.opts.Context, "[segmentio] consumer group close error %v", err)
						}
					}
					sub.createGroup(k.opts.Context)
					continue
				}

				for _, t := range cgcfg.Topics {
					assignments := generation.Assignments[t]
					for _, assignment := range assignments {
						cfg := k.readerConfig
						cfg.Topic = t
						cfg.Partition = assignment.ID
						cfg.GroupID = ""
						// break reading
						reader := kafka.NewReader(cfg)
						if k.opts.Logger.V(logger.TraceLevel) {
							k.opts.Logger.Tracef(k.opts.Context, "[segmentio] reader current offset: %v new offset: %v", reader.Offset(), assignment.Offset)
						}
						reader.SetOffset(assignment.Offset)

						cgh := &cgHandler{generation: generation, brokerOpts: k.opts, subOpts: opt, reader: reader, handler: handler}
						generation.Start(cgh.run)
					}
				}

			}
		}
	}()

	return sub, nil
}

type cgHandler struct {
	topic      string
	generation *kafka.Generation
	brokerOpts broker.Options
	subOpts    broker.SubscribeOptions
	reader     *kafka.Reader
	handler    broker.Handler
}

func (h *cgHandler) run(ctx context.Context) {
	offsets := make(map[string]map[int]int64)
	offsets[h.reader.Config().Topic] = make(map[int]int64)

	defer h.reader.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := h.reader.ReadMessage(ctx)
			switch err {
			default:
				if h.brokerOpts.Logger.V(logger.TraceLevel) {
					h.brokerOpts.Logger.Tracef(h.brokerOpts.Context, "[segmentio] unexpected error: %v", err)
				}
				return
			case kafka.ErrGenerationEnded:
				// generation has ended
				if h.brokerOpts.Logger.V(logger.TraceLevel) {
					h.brokerOpts.Logger.Trace(h.brokerOpts.Context, "[segmentio] generation ended")
				}
				return
			case nil:
				eh := h.brokerOpts.ErrorHandler

				if h.subOpts.ErrorHandler != nil {
					eh = h.subOpts.ErrorHandler
				}
				offsets[msg.Topic][msg.Partition] = msg.Offset + 1
				// github.com/segmentio/kafka-go/commit.go makeCommit builds commit message with offset + 1
				// zookeeper store offset which needs to be sent on new consumer, so current + 1
				p := &publication{topic: msg.Topic, opts: h.brokerOpts, generation: h.generation, m: &broker.Message{}, offsets: offsets}

				if h.subOpts.BodyOnly {
					p.m.Body = msg.Value
				} else {
					if err := h.brokerOpts.Codec.Unmarshal(msg.Value, p.m); err != nil {
						p.err = err
						p.m.Body = msg.Value
						if eh != nil {
							eh(p)
						} else {
							if h.brokerOpts.Logger.V(logger.ErrorLevel) {
								h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: failed to unmarshal: %v", err)
							}
						}
						continue
					}
				}
				err = h.handler(p)
				if err == nil && h.subOpts.AutoAck {
					if err = p.Ack(); err != nil {
						if h.brokerOpts.Logger.V(logger.ErrorLevel) {
							h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: unable to commit msg: %v", err)
						}
					}
				} else if err != nil {
					p.err = err
					if eh != nil {
						eh(p)
					} else {
						if h.brokerOpts.Logger.V(logger.ErrorLevel) {
							h.brokerOpts.Logger.Errorf(h.brokerOpts.Context, "[segmentio]: subscriber error: %v", err)
						}
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

	readerConfig := kafka.ReaderConfig{}
	if cfg, ok := k.opts.Context.Value(readerConfigKey{}).(kafka.ReaderConfig); ok {
		readerConfig = cfg
	}
	if len(readerConfig.Brokers) == 0 {
		readerConfig.Brokers = cAddrs
	}
	readerConfig.WatchPartitionChanges = true

	writerConfig := kafka.WriterConfig{CompressionCodec: nil, BatchSize: 1}
	if cfg, ok := k.opts.Context.Value(writerConfigKey{}).(kafka.WriterConfig); ok {
		writerConfig = cfg
	}
	if len(writerConfig.Brokers) == 0 {
		writerConfig.Brokers = cAddrs
	}
	k.addrs = cAddrs
	k.writerConfig = writerConfig
	k.readerConfig = readerConfig

	k.init = true
	return nil
}

func NewBroker(opts ...broker.Option) broker.Broker {
	return &kBroker{
		writers: make(map[string]*kafka.Writer),
		opts:    broker.NewOptions(opts...),
	}
}
