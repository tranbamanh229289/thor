package kafka2

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const idempotencyKeyPrefix = "kafka2:idempotency:"

// RedisIdempotencyStore tracks processed event IDs in Redis.
type RedisIdempotencyStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisIdempotencyStore creates a Redis-backed idempotency store.
func NewRedisIdempotencyStore(client *redis.Client, ttl time.Duration) *RedisIdempotencyStore {
	if ttl <= 0 {
		ttl = 168 * time.Hour
	}
	return &RedisIdempotencyStore{client: client, ttl: ttl}
}

func (s *RedisIdempotencyStore) key(eventID string) string {
	return idempotencyKeyPrefix + eventID
}

// Seen reports whether the event ID was already processed.
func (s *RedisIdempotencyStore) Seen(ctx context.Context, eventID string) (bool, error) {
	n, err := s.client.Exists(ctx, s.key(eventID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis idempotency seen: %w", err)
	}
	return n > 0, nil
}

// Mark records the event ID as processed.
func (s *RedisIdempotencyStore) Mark(ctx context.Context, eventID string) error {
	if err := s.client.Set(ctx, s.key(eventID), "1", s.ttl).Err(); err != nil {
		return fmt.Errorf("redis idempotency mark: %w", err)
	}
	return nil
}
