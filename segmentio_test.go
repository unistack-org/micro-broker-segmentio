package segmentio_test

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"

	segmentio "github.com/unistack-org/micro-broker-segmentio"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
	//	segjson "github.com/micro/go-plugins/codec/segmentio/v2"
)

func TestSegmentioSubscribe(t *testing.T) {
	logger.DefaultLogger = logger.NewHelper(logger.NewLogger(logger.WithLevel(logger.TraceLevel)))
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:9092"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	brk := segmentio.NewBroker(broker.Addrs(addrs...))
	if err := brk.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := brk.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}()

	fn := func(msg broker.Event) error {
		err := msg.Ack()
		t.Fatalf("msg %#+v\n", msg)
		return err
	}

	sub, err := brk.Subscribe("test_topic", fn, broker.SubscribeGroup("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}()
	select {}
}

func BenchmarkSegmentioCodecJsonPublish(b *testing.B) {
	//	b.Skip()
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		b.Skip()
	}

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:9092"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	brk := segmentio.NewBroker(broker.Addrs(addrs...))
	if err := brk.Connect(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := brk.Disconnect(); err != nil {
			b.Fatal(err)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := brk.Publish("test_topic", bm); err != nil {
			b.Fatal(err)
		}
	}
}

/*
func BenchmarkSegmentioCodecSegmentioPublish(b *testing.B) {
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		b.Skip()
	}

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:9092"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	brk := segmentio.NewBroker(broker.Codec(segjson.Marshaler{}), broker.Addrs(addrs...))
	if err := brk.Connect(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := brk.Disconnect(); err != nil {
			b.Fatal(err)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := brk.Publish("test_topic", bm); err != nil {
			b.Fatal(err)
		}
	}

}
*/
func BenchmarkSegmentioCodecJsonSubscribe(b *testing.B) {
	b.Skip()
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		b.Skip()
	}

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:9092"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	brk := segmentio.NewBroker(broker.Addrs(addrs...))
	if err := brk.Connect(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := brk.Disconnect(); err != nil {
			b.Fatal(err)
		}
	}()

	cnt := 0
	var done atomic.Value
	done.Store(false)
	exit := make(chan struct{})
	fn := func(msg broker.Event) error {
		if cnt == 0 {
			b.ResetTimer()
		}
		cnt++
		if cnt == b.N {
			if v := done.Load().(bool); !v {
				done.Store(true)
				close(exit)
			}
		}
		return msg.Ack()
	}

	go func() {
		for i := 0; i < b.N; i++ {
			if v, ok := done.Load().(bool); ok && v {
				return
			}
			if err := brk.Publish("test_topic", bm); err != nil {
				b.Fatal(err)
			}
		}
	}()

	sub, err := brk.Subscribe("test_topic", fn, broker.SubscribeGroup("test"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			b.Fatal(err)
		}
	}()
	<-exit
}

/*
func BenchmarkSegmentioCodecSegmentioSubscribe(b *testing.B) {
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		b.Skip()
	}

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:9092"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	brk := segmentio.NewBroker(broker.Codec(segjson.Marshaler{}), broker.Addrs(addrs...))
	if err := brk.Connect(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := brk.Disconnect(); err != nil {
			b.Fatal(err)
		}
	}()

	cnt := 0
	var done atomic.Value
	done.Store(false)
	exit := make(chan struct{})
	fn := func(msg broker.Event) error {
		if cnt == 0 {
			b.ResetTimer()
		}
		cnt++
		if cnt == b.N {
			if v, ok := done.Load().(bool); ok && !v {
				done.Store(true)
				close(exit)
			}
		}
		return msg.Ack()
	}

	go func() {
		for i := 0; i < b.N; i++ {
			if v := done.Load().(bool); v {
				return
			}
			if err := brk.Publish("test_topic", bm); err != nil {
				b.Fatal(err)
			}
		}
	}()

	sub, err := brk.Subscribe("test_topic", fn, broker.Queue("test"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			b.Fatal(err)
		}
	}()
	<-exit
}
*/
