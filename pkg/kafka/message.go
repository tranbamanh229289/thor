package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

func toKafkaMessage(msg *Message) *kafka.Message {
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:           []byte(msg.Key),
		Value:         msg.Value,
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
	}
	for k, v := range msg.Headers {
		km.Headers = append(km.Headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return km
}
