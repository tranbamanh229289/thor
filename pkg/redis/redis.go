package cache

import (
	"context"
	"fmt"
	"loki/pkg/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Cache struct {
	client *redis.Client
	log    *zap.Logger
}

func New(ctx context.Context, cfg *config.RedisConfig, log *zap.Logger) (*Cache, error) {
	opts := &redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		log.Error("Redis connect failed", zap.Error(err))
		return nil, err
	} else {
		log.Info("Redis connected")
	}
	return &Cache{client: client, log: log}, nil
}
