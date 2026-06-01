package config

import "time"

type Config struct {
	App        AppConfig
	HTTPServer HTTPServerConfig
}

type AppConfig struct {
	Name    string
	Env     string
	Version string
}

type HTTPServerConfig struct {
	Host            string
	Port            int
	TLSEnable       bool
	TLSCertFile     string
	TLSKeyFile      string
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
}
