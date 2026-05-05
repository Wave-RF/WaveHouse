package cache

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache *ristretto.Cache[string, []byte]
	ttls  sync.Map // key -> expiry time.Time
}

// NewLocal creates a new Ristretto-backed local cache.
func NewLocal(maxCost int64) (*LocalCache, error) {
	c, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &LocalCache{cache: c}, nil
}

func (l *LocalCache) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	val, found := l.cache.Get(key)
	if !found {
		return nil, 0, nil
	}

	var remaining time.Duration
	if expVal, ok := l.ttls.Load(key); ok {
		exp := expVal.(time.Time)
		// Only calculate remaining TTL if it was given an expiration (non-zero time)
		if !exp.IsZero() {
			remaining = time.Until(exp)
			if remaining <= 0 {
				l.cache.Del(key)
				l.ttls.Delete(key)
				return nil, 0, nil
			}
		}
	}
	return val, remaining, nil
}

func (l *LocalCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	l.cache.SetWithTTL(key, value, int64(len(value)), ttl)
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	l.ttls.Store(key, exp)
	return nil
}

func (l *LocalCache) InvalidateByPrefix(_ context.Context, prefix string) error {
	l.ttls.Range(func(key, value any) bool {
		k := key.(string)
		if strings.HasPrefix(k, prefix) {
			l.cache.Del(k)
			l.ttls.Delete(k)
		}
		return true // continue iteration
	})
	return nil
}

// Wait blocks until all buffered writes have been applied.
// Exposed for testing; production callers rarely need this.
func (l *LocalCache) Wait() {
	l.cache.Wait()
}

func (l *LocalCache) Close() error {
	l.cache.Wait()
	l.cache.Close()
	return nil
}
