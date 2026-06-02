package kafka

import (
	"thor/pkg/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	consumer *kafka.Consumer
}

func NewConsumer() (*Consumer, error) {

	return nil, nil
}

func getConsumerConfigMap(cfg *config.KafkaConfig) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		"bootstrap.servers":       c.Brokers,
		"acks":                    c.RequiredAcks,
		"retries":                 c.RetryMax,
		"retry.backoff.ms":        int(c.RetryBackoff.Milliseconds()),
		"linger.ms":               c.LingerMs,
		"batch.size":              c.BatchSize,
		"compression.type":        c.CompressionType,
		"enable.idempotence":      c.RequiredAcks == "all", // idempotent when acks=all
		"delivery.timeout.ms":     30_000,
		"socket.keepalive.enable": true,
	}
	c.applySecurityToMap(cm)
	return cm
}
