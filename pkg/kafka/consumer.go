package kafka

import (
	"context"
	"fmt"
	"strings"
	"thor/pkg/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type Consumer struct {
	consumer *kafka.Consumer
	log      *zap.Logger
	cfg      *config.KafkaConsumerConfig
}

type Handler func(ctx context.Context, msg *ReceiveMessage) error

func NewConsumer(cfg *config.KafkaConfig, log *zap.Logger) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(getConsumerConfigMap(cfg))
	if err != nil {
		log.Error("Consumer create failed", zap.Error(err))
		return nil, fmt.Errorf("Confluent New Consumer: %w", err)
	}
	log.Info("Consumer created successfully")
	return &Consumer{consumer: consumer, log: log, cfg: &cfg.Consumer}, nil
}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, handler Handler) error {
	if err := c.consumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("subscribe topics %v:%w", topics, err)
	}
	c.log.Info("Subscribe to topics", zap.Strings("topics", topics))

	defer func() {
		c.log.Info("Consumer is closing, committing offsets...")
		if err := c.consumer.Close(); err != nil {
			c.log.Error("Consumer closed is failed", zap.Error(err))
		}
	}()

	for {
		ev := c.consumer.Poll(c.cfg.PollIntervalMs)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			c.processMessage(ctx, e, handler)
		case kafka.Error:
			c.log.Error("Consumer error", zap.Error(e))
			return fmt.Errorf("consumer error:%w", e)
		case kafka.OffsetsCommitted:
			c.log.Warn("Offset commit failed", zap.Error(e.Error))
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, km *kafka.Message, handler Handler) {
	msg := fromKafkaMessage(km)

	if err := handler(ctx, msg); err != nil {
		c.log.Error("Message processing failed", zap.Error(err))

	}
	if _, err := c.consumer.CommitMessage(km); err != nil {
		c.log.Error("Message commit offset failed", zap.Error(err))

	}

}

func (c *Consumer) Close(ctx context.Context) error {
	if _, err := c.consumer.Commit(); err != nil {
		return fmt.Errorf("Final commit failed: %w", err)
	}
	return c.consumer.Close()
}

func getConsumerConfigMap(cfg *config.KafkaConfig) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		// connect
		"bootstrap.servers": cfg.Consumer.BootstrapServers,
		"group.id":          cfg.Consumer.GroupID,
		// offset
		"auto.offset.reset":  cfg.Consumer.AutoOffsetReset,
		"enable.auto.commit": cfg.Consumer.EnableAutoCommit,
		// heartbeat
		"session.timeout.ms":    cfg.Consumer.SessionTimeoutMs,
		"heartbeat.interval.ms": cfg.Consumer.HeartBeatIntervalMs,
		"max.poll.interval.ms":  cfg.Consumer.MaxPollIntervalMs,
		// fetch
		"fetch.min.bytes":   cfg.Consumer.FetchMinBytes,
		"fetch.wait.max.ms": cfg.Consumer.FetchWaitMaxMs,
		// partition strategy
		"partition.assignment.strategy": cfg.Consumer.PartitionAssignmentStrategy,
		// network
		"socket.keepalive.enable": cfg.Consumer.SocketKeepAliveEnable,
	}

	_ = cm.SetKey("security.protocol", cfg.Security.SecurityProtocol)
	if strings.HasPrefix(cfg.Security.SecurityProtocol, "SASL") {
		_ = cm.SetKey("sasl.mechanism", cfg.Security.SaslMechanism)
		_ = cm.SetKey("sasl.username", cfg.Security.SaslUsername)
		_ = cm.SetKey("sasl.password", cfg.Security.SaslPassword)
	}
	return cm
}
