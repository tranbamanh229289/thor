package kafka2

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// EventPublisher publishes domain events to Kafka with high throughput.
type EventPublisher interface {
	Publish(ctx context.Context, event *Event) error
	PublishSync(ctx context.Context, event *Event) error
	PublishToTopic(ctx context.Context, topic string, event *Event, headers map[string]string) error
	PublishToTopicSync(ctx context.Context, topic string, event *Event, headers map[string]string) error
	PublishBatch(ctx context.Context, events []*Event) error
	Flush(timeout time.Duration) int
	Close() error
}

// PublisherOption configures Publisher construction.
type PublisherOption func(*Publisher)

// WithPublisherMetrics sets the metrics collector for produce operations.
func WithPublisherMetrics(m MetricsCollector) PublisherOption {
	return func(p *Publisher) { p.metrics = m }
}

// Publisher implements EventPublisher using confluent-kafka-go.
type Publisher struct {
	producer *kafka.Producer
	log      *zap.Logger
	cfg      Config
	metrics  MetricsCollector
	closed   bool
}

// NewPublisher creates a high-throughput Kafka event publisher.
func NewPublisher(cfg Config, log *zap.Logger, opts ...PublisherOption) (*Publisher, error) {
	if log == nil {
		log = zap.NewNop()
	}
	p := &Publisher{
		log:     log,
		cfg:     cfg,
		metrics: NoopMetrics{},
	}
	for _, opt := range opts {
		opt(p)
	}

	producer, err := kafka.NewProducer(producerConfigMap(cfg))
	if err != nil {
		return nil, fmt.Errorf("kafka2: new producer: %w", err)
	}
	p.producer = producer
	go p.deliveryLoop()
	log.Info("kafka2 publisher created", zap.String("client_id", cfg.Producer.ClientID))
	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, event *Event) error {
	return p.PublishToTopic(ctx, event.TopicName(), event, nil)
}

func (p *Publisher) PublishSync(ctx context.Context, event *Event) error {
	return p.PublishToTopicSync(ctx, event.TopicName(), event, nil)
}

func (p *Publisher) PublishToTopic(ctx context.Context, topic string, event *Event, headers map[string]string) error {
	km, err := encodeEvent(event)
	if err != nil {
		return err
	}
	applyHeaderMap(km, headers)
	km.TopicPartition.Topic = &topic
	return p.produce(ctx, km, event.EventType, false)
}

func (p *Publisher) PublishToTopicSync(ctx context.Context, topic string, event *Event, headers map[string]string) error {
	km, err := encodeEvent(event)
	if err != nil {
		return err
	}
	applyHeaderMap(km, headers)
	km.TopicPartition.Topic = &topic
	return p.produce(ctx, km, event.EventType, true)
}

func (p *Publisher) produce(ctx context.Context, km *kafka.Message, eventType string, sync bool) error {
	if p.closed {
		return fmt.Errorf("kafka2: publisher is closed")
	}
	start := time.Now()
	topic := ""
	if km.TopicPartition.Topic != nil {
		topic = *km.TopicPartition.Topic
	}

	if !sync {
		if err := p.producer.Produce(km, nil); err != nil {
			p.metrics.IncProduce(topic, eventType, "error")
			return fmt.Errorf("kafka2: produce: %w", err)
		}
		p.metrics.IncProduce(topic, eventType, "enqueued")
		p.metrics.ObserveProduceLatency(topic, time.Since(start))
		return nil
	}

	delivery := make(chan kafka.Event, 1)
	if err := p.producer.Produce(km, delivery); err != nil {
		p.metrics.IncProduce(topic, eventType, "error")
		return fmt.Errorf("kafka2: produce: %w", err)
	}

	select {
	case <-ctx.Done():
		p.metrics.IncProduce(topic, eventType, "timeout")
		return fmt.Errorf("kafka2: publish sync: %w", ctx.Err())
	case e := <-delivery:
		msg, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("kafka2: unexpected delivery event %T", e)
		}
		if msg.TopicPartition.Error != nil {
			p.metrics.IncProduce(topic, eventType, "error")
			return fmt.Errorf("kafka2: delivery failed: %w", msg.TopicPartition.Error)
		}
		p.metrics.IncProduce(topic, eventType, "success")
		p.metrics.ObserveProduceLatency(topic, time.Since(start))
		return nil
	}
}

func (p *Publisher) PublishBatch(ctx context.Context, events []*Event) error {
	for _, event := range events {
		if err := p.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) Flush(timeout time.Duration) int {
	if p.producer == nil {
		return 0
	}
	return p.producer.Flush(int(timeout.Milliseconds()))
}

func (p *Publisher) Close() error {
	if p.closed || p.producer == nil {
		return nil
	}
	p.closed = true
	timeout := time.Duration(p.cfg.Producer.FlushTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	remaining := p.producer.Flush(int(timeout.Milliseconds()))
	if remaining > 0 {
		p.log.Warn("kafka2 producer flush incomplete", zap.Int("remaining", remaining))
	}
	p.producer.Close()
	p.log.Info("kafka2 publisher closed")
	return nil
}

func (p *Publisher) deliveryLoop() {
	for e := range p.producer.Events() {
		msg, ok := e.(*kafka.Message)
		if !ok || msg.TopicPartition.Error == nil {
			continue
		}
		topic := ""
		if msg.TopicPartition.Topic != nil {
			topic = *msg.TopicPartition.Topic
		}
		p.log.Error("kafka2 async delivery failed",
			zap.String("topic", topic),
			zap.Error(msg.TopicPartition.Error),
		)
		p.metrics.IncProduce(topic, "", "error")
	}
}

func producerConfigMap(cfg Config) *kafka.ConfigMap {
	p := cfg.Producer
	cm := &kafka.ConfigMap{
		"bootstrap.servers":       p.BootstrapServers,
		"client.id":               p.ClientID,
		"acks":                    p.Acks,
		"retries":                 p.Retries,
		"retry.backoff.ms":        p.RetryBackoffMs,
		"linger.ms":               p.LingerMs,
		"batch.size":              p.BatchSize,
		"compression.type":        p.CompressionType,
		"delivery.timeout.ms":     p.DeliveryTimeoutMs,
		"socket.keepalive.enable": p.SocketKeepAliveEnable,
	}
	if p.Acks == "all" {
		_ = cm.SetKey("enable.idempotence", true)
		_ = cm.SetKey("max.in.flight.requests.per.connection", 5)
	}
	applySecurity(cm, cfg.Security)
	return cm
}

func applySecurity(cm *kafka.ConfigMap, sec SecurityConfig) {
	if sec.SecurityProtocol == "" {
		return
	}
	_ = cm.SetKey("security.protocol", sec.SecurityProtocol)
	if strings.HasPrefix(sec.SecurityProtocol, "SASL") {
		_ = cm.SetKey("sasl.mechanism", sec.SaslMechanism)
		_ = cm.SetKey("sasl.username", sec.SaslUser)
		_ = cm.SetKey("sasl.password", sec.SaslPassword)
	}
}
