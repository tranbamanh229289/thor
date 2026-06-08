package kafka

import (
	"context"
	"fmt"
	"strings"
	"thor/pkg/config"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type IProducer interface {
	Publish(ctx context.Context, event *Event) error
	PublishSync(ctx context.Context, event *Event) error
	PublishBatch(ctx context.Context, events []*Event) error
	Flush(timeout time.Duration) int
	Close() error
}

type Producer struct {
	producer *kafka.Producer
	log      *zap.Logger
	cfg      *config.KafkaProducerConfig
}

func (p *Producer) Publish(ctx context.Context, event *Event) error {
	km, err := encodeEvent(event)
	if err != nil {
		p.log.Error("Kafka decode event error", zap.Error(err))
		return fmt.Errorf("kafka encode event error: %w", err)
	}

	p.log.Info("Producer is starting send message", zap.String("clientID", p.cfg.ClientID))
	if err := p.producer.Produce(km, nil); err != nil {
		p.log.Error("Kafka send async message error", zap.Error(err))
		return fmt.Errorf("Kafka send async message error: %w", err)
	}
	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, events []*Event) error {
	p.log.Info("Producer is starting send batch message", zap.String("clientID", p.cfg.ClientID))
	for _, event := range events {
		if err := p.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) PublishSync(ctx context.Context, event *Event) error {
	deliveryChan := make(chan kafka.Event, 1)
	km, err := encodeEvent(event)
	if err != nil {
		return fmt.Errorf("kafka encode event error: %w", err)
	}
	p.log.Info("Producer is starting send message", zap.String("clientID", p.cfg.ClientID))

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
			return fmt.Errorf("delivery failed topic %s:%w", event.Topic, m.TopicPartition.Error)
		}
		return nil
	}
}

func (p *Producer) Flush(timeout time.Duration) int {
	if p.producer != nil {
		return p.producer.Flush(int(timeout.Milliseconds()))
	}
	return 0
}

func (p *Producer) Close() {
	if p.producer != nil {
		p.producer.Flush(p.cfg.FlushTimeout)
		p.Close()
	}
}

func (p *Producer) handleDeliveryReports() {
	for e := range p.producer.Events() {
		msg, ok := e.(*kafka.Message)
		if !ok || msg.TopicPartition.Error == nil {
			continue
		}
		p.log.Error("Delivery failed", zap.String("topic", *msg.TopicPartition.Topic), zap.Error(msg.TopicPartition.Error))
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
		"bootstrap.servers": cfg.Producer.BootstrapServers,
		"client.id":         cfg.Producer.ClientID,
		// retry
		"retries":             cfg.Producer.Retries,
		"retry.backoff.ms":    cfg.Producer.RetryBackoffMs,
		"acks":                cfg.Producer.Acks,
		"enable.idempotence":  cfg.Producer.Acks == "all",
		"delivery.timeout.ms": cfg.Producer.DeliveryTimeoutMs,
		// batch process
		"linger.ms":        cfg.Producer.LingerMs,
		"batch.size":       cfg.Producer.BatchSize,
		"compression.type": cfg.Producer.CompressionType,
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
