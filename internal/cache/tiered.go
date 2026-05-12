package cache

import (
	"context"
	"time"

	"golang.org/x/sync/singleflight"
)

// TieredCache wraps L1 (local) + L2 (shared) caches with singleflight dedup.
type TieredCache struct {
	l1    Cache
	l2    Cache
	group singleflight.Group
}

// NewTiered creates a tiered cache. L2 can be nil for standalone mode.
func NewTiered(l1, l2 Cache) *TieredCache {
	return &TieredCache{l1: l1, l2: l2}
}

func (t *TieredCache) Get(ctx context.Context, key string) ([]byte, time.Duration, error) {
	// Check L1.
	val, ttl, err := t.l1.Get(ctx, key)
	if err == nil && val != nil {
		return val, ttl, nil
	}

	// Singleflight for L2 lookup.
	v, err, _ := t.group.Do(key, func() (interface{}, error) {
		if t.l2 != nil {
			val, ttl, err := t.l2.Get(ctx, key)
			if err == nil && val != nil {
				// Backfill L1 with remaining TTL from L2.
				// Note: We don't have the original tags here, but L2 is out-of-scope for v1 Standalone.
				_ = t.l1.Set(ctx, key, val, ttl, nil)
				return &cacheResult{data: val, ttl: ttl}, nil
			}
		}
		return nil, nil
	})
	if err != nil {
		return nil, 0, err
	}
	if v != nil {
		cr := v.(*cacheResult)
		return cr.data, cr.ttl, nil
	}
	return nil, 0, nil
}

// UPDATE: Accept tags and pass them down
func (t *TieredCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration, tags []string) error {
	if err := t.l1.Set(ctx, key, value, ttl, tags); err != nil {
		return err
	}
	if t.l2 != nil {
		return t.l2.Set(ctx, key, value, ttl, tags)
	}
	return nil
}

// UPDATE: Replace InvalidateByPrefix with InvalidateByTags
func (t *TieredCache) InvalidateByTags(ctx context.Context, tags []string) error {
	if err := t.l1.InvalidateByTags(ctx, tags); err != nil {
		return err
	}
	if t.l2 != nil {
		return t.l2.InvalidateByTags(ctx, tags)
	}
	return nil
}

func (t *TieredCache) Close() error {
	_ = t.l1.Close()
	if t.l2 != nil {
		_ = t.l2.Close()
	}
	return nil
}

type cacheResult struct {
	data []byte
	ttl  time.Duration
}
