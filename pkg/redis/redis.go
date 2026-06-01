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

type Cache struct {
	client  *redis.Client
	log     *zap.Logger
	timeout time.Duration
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

	ctx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Error("Redis connect failed", zap.Error(err))
		_ = client.Close()
		return nil, err
	} else {
		log.Info("Redis connected")
	}
	return &Cache{client: client, log: log, timeout: cfg.DialTimeout}, nil
}

func (c *Cache) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
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
