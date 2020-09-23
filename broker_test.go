package segmentio_test

import (
	"os"
	"strings"
	"testing"

	segmentio "github.com/unistack-org/micro-broker-segmentio"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
)

var (
	bm = &broker.Message{
		Header: map[string]string{"hkey": "hval"},
		Body:   []byte("body"),
	}
)

func TestPubSub(t *testing.T) {
	t.Skip()
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

	b := segmentio.NewBroker(broker.Addrs(addrs...))
	if err := b.Init(); err != nil {
		t.Fatal(err)
	}

	if err := b.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := b.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}()

	done := make(chan bool, 1)
	fn := func(msg broker.Event) error {
		done <- true
		return msg.Ack()
	}

	sub, err := b.Subscribe("test_topic", fn, broker.SubscribeGroup("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}()
	if err := b.Publish("test_topic", bm); err != nil {
		t.Fatal(err)
	}
	<-done
}
