package cache

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache   *ristretto.Cache[string, []byte]
	ttls    sync.Map // key -> expiry time.Time
	tagsMap sync.Map // tag (string) -> *sync.Map (key -> struct{})
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

func (l *LocalCache) Set(_ context.Context, key string, value []byte, ttl time.Duration, tags []string) error {
	l.cache.SetWithTTL(key, value, int64(len(value)), ttl)
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	l.ttls.Store(key, exp)

	// Register key under each tag
	for _, tag := range tags {
		m, _ := l.tagsMap.LoadOrStore(tag, &sync.Map{})
		m.(*sync.Map).Store(key, struct{}{})
	}
	return nil
}

func (l *LocalCache) InvalidateByTags(_ context.Context, tags []string) error {
	for _, tag := range tags {
		// Atomically remove the map from the index before iterating
		if keysMap, ok := l.tagsMap.LoadAndDelete(tag); ok {
			keysMap.(*sync.Map).Range(func(key, _ any) bool {
				k := key.(string)
				l.cache.Del(k)
				l.ttls.Delete(k)
				return true
			})
		}
	}
	return nil
}

func (l *LocalCache) Wait() {
	l.cache.Wait()
}

func (l *LocalCache) Close() error {
	l.cache.Wait()
	l.cache.Close()
	return nil
}
