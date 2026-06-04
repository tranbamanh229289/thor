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

func (p *Producer) Publish(ctx context.Context, msg Message) error {
	km := toKafkaMessage(&msg)
	if err := p.producer.Produce(km, nil); err != nil {
		p.log.Error("Kafka send async message error", zap.Error(err))
		return fmt.Errorf("Kafka send async message error: %w", err)
	}
	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, msgs []Message) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) PublishSync(ctx context.Context, msg Message) error {
	deliveryChan := make(chan kafka.Event, 1)
	km := toKafkaMessage(&msg)

	if err := p.producer.Produce(km, deliveryChan); err != nil {
		p.log.Error("Kafka send message error", zap.Error(err))
		return fmt.Errorf("produce enqueue: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("Kafka send timeout: %w", ctx.Err())
	case e := <-deliveryChan:
		m, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpect event type %T", e)
		}
		if m.TopicPartition.Error != nil {
			p.log.Error("Delivery failed", zap.Error(m.TopicPartition.Error))
			return fmt.Errorf("delivery failed topic %s:%w", msg.Topic, m.TopicPartition.Error)
		}
		return nil
	}
}

func NewProducer(cfg *config.KafkaConfig, log *zap.Logger) (*Producer, error) {
	producer, err := kafka.NewProducer(getProducerConfigMap(cfg))
	if err != nil {
		log.Error("Producer create failed", zap.Error(err))
		return nil, fmt.Errorf("Confluent New Producer: %w", err)
	}
	log.Info("Producer created successfully")
	p := &Producer{producer: producer, log: log, cfg: &cfg.Producer}
	go p.handleDeliveryReports()
	return p, nil
}

func getProducerConfigMap(cfg *config.KafkaConfig) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		// connect
		"bootstraps.servers": cfg.Producer.BootstrapServers,
		// retry
		"retries":             cfg.Producer.Retries,
		"retry.backoff.ms":    cfg.Producer.RetryBackoffMs,
		"acks":                cfg.Producer.Acks,
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
		_ = cm.SetKey("sasl.password", cfg.Security.SaslPassword)
	}
	return cm
}

func (p *Producer) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.log.Error("Delivery failed", zap.Error(ev.TopicPartition.Error))
			}
		}

	}
}
