package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"thor/pkg/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type ICache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	GetJSON(ctx context.Context, key string, des any) error
	SetJSON(ctx context.Context, key string, value any, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

type Cache struct {
	client *redis.Client
	log    *zap.Logger
	cfg    *config.RedisConfig
}

func (c *Cache) GetJSON(ctx context.Context, key string, des any) error {
	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, des)
}

func (c *Cache) SetJSON(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, ttl).Err()
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *Cache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

func New(ctx context.Context, cfg *config.RedisConfig, log *zap.Logger) (*Cache, error) {
	var err error
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

	retries := cfg.Retries
	if retries <= 0 {
		retries = 1
	}

	client := redis.NewClient(opts)

	for attempt := 0; attempt < retries; attempt++ {
		if err = client.Ping(ctx).Err(); err == nil {
			log.Info("Redis connected")
			return &Cache{client: client, log: log, cfg: cfg}, nil
		}
		log.Warn("Redis connect failed, retrying", zap.Int("attempt", attempt+1), zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(cfg.RetryBackoffMs) * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("Redis connect failed: %w", err)
}

func (c *Cache) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	defer cancel()

	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func (c *Cache) Close() {
	if c.client != nil {
		c.client.Close()
	}
}
