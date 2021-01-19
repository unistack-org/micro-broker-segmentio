module github.com/micro/go-plugins/broker/segmentio/v2

go 1.13

require (
	github.com/google/uuid v1.1.1
	github.com/micro/go-micro/v2 v2.9.1-0.20200716153311-f9bf56239306
	github.com/micro/go-plugins/broker/kafka/v2 v2.9.2-0.20200716222928-fe493b686068
	github.com/micro/go-plugins/codec/segmentio/v2 v2.9.2-0.20200716222928-fe493b686068
	github.com/segmentio/kafka-go v0.3.7
)

replace github.com/micro/go-plugins/codec/segmentio/v2 => ../../codec/segmentio

replace github.com/micro/go-plugins/broker/kafka/v2 => ../kafka

replace github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible
