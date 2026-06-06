package kafka2

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// LoadConfig reads Kafka config from a YAML file and merges with defaults.
func LoadConfig(path, serviceName string) (Config, error) {
	cfg := DefaultConfig(serviceName)

	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return cfg, fmt.Errorf("kafka2: read config: %w", err)
	}

	// Support both root-level and nested "kafka" key.
	if v.IsSet("kafka") {
		if err := v.UnmarshalKey("kafka", &cfg); err != nil {
			return cfg, fmt.Errorf("kafka2: unmarshal kafka config: %w", err)
		}
	} else if err := v.Unmarshal(&cfg); err != nil {
		return cfg, fmt.Errorf("kafka2: unmarshal config: %w", err)
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = serviceName
	}
	if cfg.Producer.ClientID == "" {
		cfg.Producer.ClientID = serviceName + "-producer"
	}
	if cfg.Consumer.GroupID == "" {
		cfg.Consumer.GroupID = serviceName
	}
	return cfg, nil
}

// MergeConfig overlays overrides onto base config.
func MergeConfig(base Config, overrides Config) Config {
	if overrides.ServiceName != "" {
		base.ServiceName = overrides.ServiceName
	}
	if overrides.Producer.BootstrapServers != "" {
		base.Producer = overrides.Producer
	}
	if overrides.Consumer.BootstrapServers != "" {
		base.Consumer = overrides.Consumer
	}
	if overrides.Retry.MaxAttempts > 0 {
		base.Retry = overrides.Retry
	}
	if overrides.Outbox.PollIntervalMs > 0 {
		base.Outbox = overrides.Outbox
	}
	if overrides.Idempotency.Store != "" {
		base.Idempotency = overrides.Idempotency
	}
	if overrides.Security.SecurityProtocol != "" {
		base.Security = overrides.Security
	}
	return base
}
