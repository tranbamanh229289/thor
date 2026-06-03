package kafka

import (
	"context"
	"fmt"
	"strings"
	"thor/pkg/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type Producer struct {
	producer *kafka.Producer
	log      *zap.Logger
	cfg      *config.KafkaProducerConfig
}

func (p *Producer) SendMessageAsync(ctx context.Context, msg Message) error {
	km := toKafkaMessage(&msg)
	if err := p.producer.Produce(km, nil); err != nil {
		p.log.Error("Kafka send async message error", zap.Error(err))
		return fmt.Errorf("Kafka send async message error: %w", err)
	}
	return nil
}

func (p *Producer) SendMessage(ctx context.Context, msg Message) error {
	km := toKafkaMessage(&msg)
	deliveryChan := make(chan kafka.Event, 1)
	if err := p.producer.Produce(km, nil); err != nil {

	}
}

func (p *Producer) SendBatchMessage(ctx context.Context, msg []Message) error {

}

func NewProducer(cfg *config.KafkaConfig, log *zap.Logger) (*Producer, error) {
	producer, err := kafka.NewProducer(getProducerConfigMap(cfg))
	if err != nil {
		log.Error("Producer create failed", zap.Error(err))
		return nil, fmt.Errorf("Confluent New Producer: %w", err)
	}
	log.Info("Producer created successfully")
	return &Producer{producer: producer, log: log, cfg: &cfg.Producer}, nil
}

func getProducerConfigMap(cfg *config.KafkaConfig) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		// connect
		"bootstraps.servers": cfg.Producer.BootstrapServers,
		"acks":               cfg.Producer.Acks,
		// retry
		"retries":             cfg.Producer.Retries,
		"retry.backoff.ms":    cfg.Producer.RetryBackoffMs,
		"enable.idempotence":  cfg.Producer.Acks == "all",
		"delivery.timeout.ms": cfg.Producer.DeliveryTimeoutMs,
		// batch process
		"linger.ms":     cfg.Producer.LingerMs,
		"batch.size":    cfg.Producer.BatchSize,
		"compress.type": cfg.Producer.CompressType,
		// network
		"socket.keepalive.enable": cfg.Producer.SocketKeepAliveEnable,
	}

	_ = cm.SetKey("security.protocol", cfg.Security.SecurityProtocol)
	if strings.HasPrefix(cfg.Security.SecurityProtocol, "SASL") {
		_ = cm.SetKey("sasl.mechanism", cfg.Security.SaslMechanism)
		_ = cm.SetKey("sasl.username", cfg.Security.SaslUsername)
		_ = cm.SetKey("sasl_password", cfg.Security.SaslPassword)
	}
	return cm
}
