module github.com/unistack-org/micro-broker-segmentio/v3

go 1.16

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0
	github.com/klauspost/compress v1.13.1 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/segmentio/kafka-go v0.4.17
	github.com/unistack-org/micro/v3 v3.5.3
)

//replace github.com/unistack-org/micro/v3 => ../micro
