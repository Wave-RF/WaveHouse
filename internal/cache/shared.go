package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// SharedCache is an L2 cache backed by Redis.
type SharedCache struct {
	client *redis.Client
}

// NewShared creates a Redis-backed shared cache.
func NewShared(url string) (*SharedCache, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &SharedCache{client: redis.NewClient(opts)}, nil
}

func (s *SharedCache) Get(ctx context.Context, key string) ([]byte, time.Duration, error) {
	val, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}

	ttl, err := s.client.TTL(ctx, key).Result()
	if err != nil {
		return val, 0, nil
	}
	if ttl < 0 {
		ttl = 0
	}
	return val, ttl, nil
}

func (s *SharedCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.client.Set(ctx, key, value, ttl).Err()
}

func (s *SharedCache) Close() error {
	return s.client.Close()
}
