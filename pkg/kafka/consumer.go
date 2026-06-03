package kafka

import (
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

func NewConsumer(cfg *config.KafkaConfig, log *zap.Logger) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(getConsumerConfigMap(cfg))
	if err != nil {
		log.Error("Consumer create failed", zap.Error(err))
		return nil, fmt.Errorf("Confluent New Consumer: %w", err)
	}
	log.Info("Consumer created successfully")
	return &Consumer{consumer: consumer, log: log, cfg: &cfg.Consumer}, nil
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
		_ = cm.SetKey("sasl_password", cfg.Security.SaslPassword)
	}
	return cm
}
