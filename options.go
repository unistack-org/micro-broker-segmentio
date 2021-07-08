package segmentio

import (
	"context"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/client"
)

var (
	DefaultReaderConfig  = kafka.ReaderConfig{}
	DefaultWriterConfig  = kafka.WriterConfig{}
	DefaultStatsInterval = time.Second * 10
)

type readerConfigKey struct{}
type writerConfigKey struct{}

func ReaderConfig(c kafka.ReaderConfig) broker.Option {
	return broker.SetOption(readerConfigKey{}, c)
}

func WriterConfig(c kafka.WriterConfig) broker.Option {
	return broker.SetOption(writerConfigKey{}, c)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeContextKey{}, ctx)
}

type subscribeReaderConfigKey struct{}

func SubscribeReaderConfig(c kafka.ReaderConfig) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeReaderConfigKey{}, c)
}

type subscribeWriterConfigKey struct{}

func SubscribeWriterConfig(c kafka.WriterConfig) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeWriterConfigKey{}, c)
}

type publishKey struct{}

func PublishKey(key []byte) broker.PublishOption {
	return broker.SetPublishOption(publishKey{}, key)
}

func ClientPublishKey(key []byte) client.PublishOption {
	return client.SetPublishOption(publishKey{}, key)
}

type statsIntervalKey struct{}

func StatsInterval(td time.Duration) broker.Option {
	return broker.SetOption(statsIntervalKey{}, td)
}

type writerCompletionFunc struct{}

func WriterCompletionFunc(fn func([]kafka.Message, error)) broker.Option {
	return broker.SetOption(writerCompletionFunc{}, fn)
}
