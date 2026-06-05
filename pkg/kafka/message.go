package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type SendMessage struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Partition int32
}

type ReceiveMessage struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
	Timestamp int64
}

func toKafkaMessage(msg *SendMessage) *kafka.Message {
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:           msg.Key,
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

func fromKafkaMessage(km *kafka.Message) *ReceiveMessage {
	msg := &ReceiveMessage{
		Topic:     *km.TopicPartition.Topic,
		Key:       km.Key,
		Value:     km.Value,
		Partition: km.TopicPartition.Partition,
		Offset:    int64(km.TopicPartition.Offset),
		Timestamp: km.Timestamp.UnixMilli(),
	}
	var headers = make(map[string]string)
	for _, header := range km.Headers {
		headers[header.Key] = string(header.Value)
	}
	msg.Headers = headers
	return msg
}
