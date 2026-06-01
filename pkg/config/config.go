package config

import (
	"log"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	App           AppConfig           `mapstructure:"app"`
	HTTPServer    HTTPServerConfig    `mapstructure:"http_server"`
	GRPCServer    GRPCServerConfig    `mapstructure:"grpc_server"`
	GraphQLServer GraphQLServerConfig `mapstructure:"graphql_server"`
	Postgres      PostgresConfig      `mapstructure:"postgres"`
	Redis         RedisConfig         `mapstructure:"redis"`
	Cassandra     CassandraConfig     `mapstructure:"cassandra"`
	Elasticsearch ElasticsearchConfig `mapstructure:"elasticsearch"`
	Kafka         KafkaConfig         `mapstructure:"kafka"`
	Zap           ZapConfig           `mapstructure:"zap"`
	JWT           JWTConfig           `mapstructure:"jwt"`
}

type AppConfig struct {
	Name    string `mapstructure:"name"`
	Env     string `mapstructure:"env"`
	Version string `mapstructure:"version"`
}

type HTTPServerConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	TLS             TLSConfig     `mapstructure:"tls"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	IdleTimeout     time.Duration `mapstructure:"idle_timeout"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
}
type TLSConfig struct {
	Enable   bool   `mapstructure:"enable"`
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
}
type PostgresConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	Database        string        `mapstructure:"database"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	SSLMode         string        `mapstructure:"ssl_mode"`
	MaxConns        int32         `mapstructure:"max_conns"`
	MinConns        int32         `mapstructure:"min_conns"`
	MaxConnIdleTime time.Duration `mapstructure:"max_conn_idle_time"`
	MaxConnLifeTime time.Duration `mapstructure:"max_conn_life_time"`
	Timeout         time.Duration `mapstructure:"timeout"`
}

type ElasticsearchConfig struct {
	Address  []string      `mapstructure:"address"`
	Username string        `mapstructure:"username"`
	Password string        `mapstructure:"password"`
	Timeout  time.Duration `mapstructure:"timeout"`
}

type RedisConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	PoolSize     int           `mapstructure:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type CassandraConfig struct {
	Hosts       []string      `mapstructure:"hosts"`
	Port        int           `mapstructure:"port"`
	Keyspace    string        `mapstructure:"keyspace"`
	Username    string        `mapstructure:"username"`
	Password    string        `mapstructure:"password"`
	LocalDC     string        `mapstructure:"local_dc"`
	Consistency string        `mapstructure:"consistency"`
	NumConns    int           `mapstructure:"num_conns"`
	Timeout     time.Duration `mapstructure:"timeout"`
}

type ZapConfig struct {
	Level             string `mapstructure:"level"`
	Development       bool   `mapstructure:"development"`
	DisableCaller     bool   `mapstructure:"disable_caller"`
	DisableStacktrace bool   `mapstructure:"disable_stacktrace"`
	Encoding          string `mapstructure:"encoding"`
	OutputPath        string `mapstructure:"output_path"`
	ErrorOutputPath   string `mapstructure:"error_output_path"`
}

type KafkaConfig struct {
}

type LokiConfig struct {
}

type GRPCServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type GraphQLServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type JWTConfig struct {
	Secret          string        `mapstructure:"secret"`
	AccessTokenTTL  time.Duration `mapstructure:"access_token_ttl"`
	RefreshTokenTTL time.Duration `mapstructure:"refresh_token_ttl"`
}

func New(path string) (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file: %w", err)
		return nil, err
	}

	viper.AddConfigPath(path)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("Failed to read config file: %w", err)
		return nil, err
	}

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatal("Error unmarshal config: %w", err)
		return nil, err
	}

	return &config, nil
}
