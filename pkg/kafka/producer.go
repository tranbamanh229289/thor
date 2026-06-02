package kafka

import (
	"thor/pkg/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer *kafka.Producer
}

func NewProducer() (*Producer, error) {
	return nil, nil
}

func getProducerConfigMap(cfg *config.KafkaConfig) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstraps.servers": cfg.Producer.BootstrapServers,
	}
}
