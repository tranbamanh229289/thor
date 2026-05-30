package config

import "time"

type Config struct {
	App           AppConfig
	HTTPServer    HTTPServerConfig
	GRPC          GRPCServerConfig
	GraphQL       GraphQLServerConfig
	TSL           TSLConfig
	Postgres      PostgresConfig
	Redis         RedisConfig
	Cassandra     CassandraConfig
	Elasticsearch ElasticsearchConfig
	Kafka         KafkaConfig
	Zap           ZapConfig
	Loki          LokiConfig
	JWT           JWTConfig
}

type AppConfig struct {
	Name    string
	Env     string
	Version string
}

type HTTPServerConfig struct {
	Host            string
	HTTPPort        int
	GRPCPort        int
	GraphQLPort     int
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

type TSLConfig struct {
	Enable   bool
	CertFile string
	KeyFile  string
}

type PostgresConfig struct {
	Host            string
	Port            int
	Database        string
	User            string
	Password        string
	SSLMode         string
	MaxConns        int32
	MinConns        int32
	MaxConnIdleTime time.Duration
	MaxConnLifeTime time.Duration
	Timeout         time.Duration
}

type ElasticsearchConfig struct {
	Host string
}

type RedisConfig struct {
	Host     string
	Port     int32
	Password string
}

type CassandraConfig struct {
}

type ZapConfig struct {
}

type KafkaConfig struct {
}

type LokiConfig struct{}

type GRPCServerConfig struct {
}

type GraphQLServerConfig struct {
}

type JWTConfig struct {
	Secret          string
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}
