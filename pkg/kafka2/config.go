package kafka2

import "time"

// Config holds Kafka settings for a single microservice.
type Config struct {
	ServiceName string         `mapstructure:"service_name"`
	Producer    ProducerConfig `mapstructure:"producer"`
	Consumer    ConsumerConfig `mapstructure:"consumer"`
	Retry       RetryConfig    `mapstructure:"retry"`
	Outbox      OutboxConfig   `mapstructure:"outbox"`
	Idempotency IdempotencyConfig `mapstructure:"idempotency"`
	Security    SecurityConfig `mapstructure:"security"`
}

type ProducerConfig struct {
	BootstrapServers      string `mapstructure:"bootstrap_servers"`
	ClientID              string `mapstructure:"client_id"`
	Acks                  string `mapstructure:"acks"`
	Retries               int    `mapstructure:"retries"`
	RetryBackoffMs        int    `mapstructure:"retry_backoff_ms"`
	LingerMs              int    `mapstructure:"linger_ms"`
	BatchSize             int    `mapstructure:"batch_size"`
	CompressionType       string `mapstructure:"compression_type"`
	DeliveryTimeoutMs     int    `mapstructure:"delivery_timeout_ms"`
	FlushTimeoutMs        int    `mapstructure:"flush_timeout_ms"`
	SocketKeepAliveEnable bool   `mapstructure:"socket_keep_alive_enable"`
}

type ConsumerConfig struct {
	BootstrapServers            string `mapstructure:"bootstrap_servers"`
	GroupID                     string `mapstructure:"group_id"`
	AutoOffsetReset             string `mapstructure:"auto_offset_reset"`
	EnableAutoCommit            bool   `mapstructure:"enable_auto_commit"`
	SessionTimeoutMs            int    `mapstructure:"session_timeout_ms"`
	HeartbeatIntervalMs         int    `mapstructure:"heartbeat_interval_ms"`
	MaxPollIntervalMs           int    `mapstructure:"max_poll_interval_ms"`
	PollTimeoutMs               int    `mapstructure:"poll_timeout_ms"`
	FetchMinBytes               int    `mapstructure:"fetch_min_bytes"`
	FetchMaxBytes               int    `mapstructure:"fetch_max_bytes"`
	PartitionAssignmentStrategy string `mapstructure:"partition_assignment_strategy"`
	WorkerBufferSize            int    `mapstructure:"worker_buffer_size"`
	SocketKeepAliveEnable       bool   `mapstructure:"socket_keep_alive_enable"`
}

type RetryConfig struct {
	MaxAttempts      int `mapstructure:"max_attempts"`
	InitialBackoffMs int `mapstructure:"initial_backoff_ms"`
	MaxBackoffMs     int `mapstructure:"max_backoff_ms"`
}

type OutboxConfig struct {
	Enabled        bool `mapstructure:"enabled"`
	PollIntervalMs int  `mapstructure:"poll_interval_ms"`
	BatchSize      int  `mapstructure:"batch_size"`
}

type IdempotencyConfig struct {
	Enabled bool          `mapstructure:"enabled"`
	Store   string        `mapstructure:"store"` // redis | postgres
	TTL     time.Duration `mapstructure:"ttl"`
}

type SecurityConfig struct {
	SecurityProtocol string `mapstructure:"security_protocol"`
	SaslMechanism    string `mapstructure:"sasl_mechanism"`
	SaslUser         string `mapstructure:"sasl_user"`
	SaslPassword     string `mapstructure:"sasl_password"`
}

// DefaultConfig returns production-oriented defaults. Services override as needed.
func DefaultConfig(serviceName string) Config {
	return Config{
		ServiceName: serviceName,
		Producer: ProducerConfig{
			ClientID:              serviceName + "-producer",
			Acks:                  "all",
			Retries:               5,
			RetryBackoffMs:        100,
			LingerMs:              20,
			BatchSize:             65536,
			CompressionType:       "lz4",
			DeliveryTimeoutMs:     120000,
			FlushTimeoutMs:        10000,
			SocketKeepAliveEnable: true,
		},
		Consumer: ConsumerConfig{
			GroupID:                     serviceName,
			AutoOffsetReset:             "earliest",
			EnableAutoCommit:            false,
			SessionTimeoutMs:            45000,
			HeartbeatIntervalMs:         15000,
			MaxPollIntervalMs:           300000,
			PollTimeoutMs:               100,
			FetchMinBytes:               1024,
			FetchMaxBytes:               52428800,
			PartitionAssignmentStrategy: "cooperative-sticky",
			WorkerBufferSize:            256,
			SocketKeepAliveEnable:       true,
		},
		Retry: RetryConfig{
			MaxAttempts:      5,
			InitialBackoffMs: 1000,
			MaxBackoffMs:     60000,
		},
		Outbox: OutboxConfig{
			Enabled:        true,
			PollIntervalMs: 500,
			BatchSize:      100,
		},
		Idempotency: IdempotencyConfig{
			Enabled: true,
			Store:   "redis",
			TTL:     168 * time.Hour,
		},
		Security: SecurityConfig{
			SecurityProtocol: "PLAINTEXT",
		},
	}
}
