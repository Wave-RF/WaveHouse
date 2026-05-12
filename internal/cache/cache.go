package cache

import (
	"context"
	"time"
)

// Cache provides key-value storage with TTL support.
type Cache interface {
	// Get retrieves a value and its remaining TTL. Returns nil, 0, nil on miss.
	Get(ctx context.Context, key string) ([]byte, time.Duration, error)

	// Set stores a value with the given TTL and tags.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration, tags []string) error

	// InvalidateByTags drops all keys associated with the provided tags.
	InvalidateByTags(ctx context.Context, tags []string) error

	// Close releases resources.
	Close() error
}
