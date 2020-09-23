// Package kafka provides a kafka broker using segmentio
package segmentio

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/codec/json"
	"github.com/unistack-org/micro/v3/logger"
)

type kBroker struct {
	addrs []string

	readerConfig kafka.ReaderConfig
	writerConfig kafka.WriterConfig

	writers map[string]*kafka.Writer

	connected bool
	sync.RWMutex
	opts broker.Options
}

type subscriber struct {
	k         *kBroker
	topic     string
	opts      broker.SubscribeOptions
	offset    int64
	gen       *kafka.Generation
	partition int
	handler   broker.Handler
	reader    *kafka.Reader
	closed    bool
	done      chan struct{}
	group     *kafka.ConsumerGroup
	cgcfg     kafka.ConsumerGroupConfig
	sync.RWMutex
}

type publication struct {
	topic      string
	err        error
	m          *broker.Message
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
	if logger.V(logger.TraceLevel) {
		logger.Tracef("commit offset %#+v\n", p.offsets)
	}
	return p.generation.CommitOffsets(p.offsets)
}

func (p *publication) Error() error {
	return p.m.Error
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe() error {
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

func (k *kBroker) Connect() error {
	k.RLock()
	if k.connected {
		k.RUnlock()
		return nil
	}
	k.RUnlock()

	kaddrs := make([]string, 0, len(k.addrs))
	for _, addr := range k.addrs {
		conn, err := kafka.DialContext(k.opts.Context, "tcp", addr)
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
		return errors.New("no available brokers")
	}

	k.Lock()
	k.addrs = kaddrs
	k.readerConfig.Brokers = k.addrs
	k.writerConfig.Brokers = k.addrs
	k.connected = true
	k.Unlock()

	return nil
}

func (k *kBroker) Disconnect() error {
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
	return k.configure(opts...)
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	var cached bool

	buf, err := k.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	kmsg := kafka.Message{Value: buf}

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

	err = writer.WriteMessages(k.opts.Context, kmsg)
	if logger.V(logger.TraceLevel) {
		logger.Tracef("write message err: %v", err)
	}
	if err != nil {
		switch cached {
		case false:
			// non cached case, we can try to wait on some errors, but not timeout
			if kerr, ok := err.(kafka.Error); ok {
				if kerr.Temporary() && !kerr.Timeout() {
					// additional chanse to publish message
					time.Sleep(200 * time.Millisecond)
					err = writer.WriteMessages(k.opts.Context, kmsg)
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
			if err = writer.WriteMessages(k.opts.Context, kmsg); err == nil {
				k.Lock()
				k.writers[topic] = writer
				k.Unlock()
			}
		}
	}

	return err
}

func (k *kBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	opt := broker.SubscribeOptions{}
	for _, o := range opts {
		o(&opt)
	}

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

	sub := &subscriber{opts: opt, topic: topic, group: cgroup, cgcfg: cgcfg}

	go func() {
		for {
			select {
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
					if logger.V(logger.TraceLevel) {
						logger.Tracef("[segmentio] context closed unexpected %v", k.opts.Context.Err())
					}
				}
				return
			default:
				sub.RLock()
				group := sub.group
				sub.RUnlock()
				generation, err := group.Next(k.opts.Context)
				switch err {
				case nil:
					// normal execution
				case kafka.ErrGroupClosed:
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if !closed {
						if logger.V(logger.TraceLevel) {
							logger.Tracef("[segmentio] recreate consumer group, as it closed %v", k.opts.Context.Err())
						}
						if err = group.Close(); err != nil {
							if logger.V(logger.TraceLevel) {
								logger.Tracef("[segmentio] consumer group close error %v", err)
							}
							continue
						}
						sub.createGroup(k.opts.Context)
					}
					continue
				default:
					sub.RLock()
					closed := sub.closed
					sub.RUnlock()
					if !closed {
						if logger.V(logger.TraceLevel) {
							logger.Tracef("[segmentio] recreate consumer group, as unexpected consumer error %v", err)
						}
					}
					if err = group.Close(); err != nil {
						if logger.V(logger.ErrorLevel) {
							logger.Tracef("[segmentio] consumer group close error %v", err)
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
						if logger.V(logger.TraceLevel) {
							logger.Tracef("[segmentio] reader current offset: %v new offset: %v", reader.Offset(), assignment.Offset)
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
				if logger.V(logger.TraceLevel) {
					logger.Tracef("[segmentio] unexpected error: %v", err)
				}
				return
			case kafka.ErrGenerationEnded:
				// generation has ended
				if logger.V(logger.TraceLevel) {
					logger.Tracef("[segmentio] generation ended")
				}
				return
			case nil:
				var m broker.Message
				eh := h.brokerOpts.ErrorHandler

				if h.subOpts.ErrorHandler != nil {
					eh = h.subOpts.ErrorHandler
				}
				offsets[msg.Topic][msg.Partition] = msg.Offset
				p := &publication{topic: msg.Topic, generation: h.generation, m: &m, offsets: offsets}

				if err := h.brokerOpts.Codec.Unmarshal(msg.Value, &m); err != nil {
					p.m.Error = err
					p.m.Body = msg.Value
					if eh != nil {
						eh(p)
					} else {
						if logger.V(logger.ErrorLevel) {
							logger.Errorf("[segmentio]: failed to unmarshal: %v", err)
						}
					}
					continue
				}
				err = h.handler(p)
				if err == nil && h.subOpts.AutoAck {
					if err = p.Ack(); err != nil {
						if logger.V(logger.ErrorLevel) {
							logger.Errorf("[segmentio]: unable to commit msg: %v", err)
						}
					}
				} else if err != nil {
					p.m.Error = err
					if eh != nil {
						eh(p)
					} else {
						if logger.V(logger.ErrorLevel) {
							logger.Errorf("[segmentio]: subscriber error: %v", err)
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
				if logger.V(logger.ErrorLevel) {
					logger.Errorf("[segmentio]: consumer group error %v", err)
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

	if k.opts.Codec == nil {
		k.opts.Codec = json.Marshaler{}
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

	return nil
}

func NewBroker(opts ...broker.Option) broker.Broker {
	return &kBroker{
		writers: make(map[string]*kafka.Writer),
		opts:    broker.NewOptions(opts...),
	}
}
