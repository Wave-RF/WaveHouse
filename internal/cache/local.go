package cache

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache *ristretto.Cache[string, []byte]
	ttls  sync.Map // key -> expiry time.Time

	// tagsMu guards tagsMap and keyTags to prevent TOCTOU races during Set/Invalidate
	tagsMu  sync.RWMutex
	tagsMap map[string]map[string]struct{} // tag -> set of keys
	keyTags map[string][]string            // key -> list of tags (for leak cleanup)
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
	return &LocalCache{
		cache:   c,
		tagsMap: make(map[string]map[string]struct{}),
		keyTags: make(map[string][]string),
	}, nil
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
				l.tagsMu.Lock()

				l.cache.Del(key)
				l.ttls.Delete(key)

				if tags, exists := l.keyTags[key]; exists {
					for _, tag := range tags {
						if keys, ok := l.tagsMap[tag]; ok {
							delete(keys, key)
							if len(keys) == 0 {
								delete(l.tagsMap, tag)
							}
						}
					}
					delete(l.keyTags, key)
				}
				l.tagsMu.Unlock()

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

	// Acquire lock for the entire tag registration block
	l.tagsMu.Lock()
	defer l.tagsMu.Unlock()

	// Clean up old tags if we are overwriting an existing key
	if oldTags, exists := l.keyTags[key]; exists {
		for _, oldTag := range oldTags {
			if m, ok := l.tagsMap[oldTag]; ok {
				delete(m, key)
				if len(m) == 0 {
					delete(l.tagsMap, oldTag)
				}
			}
		}
	}

	l.keyTags[key] = tags
	for _, tag := range tags {
		if l.tagsMap[tag] == nil {
			l.tagsMap[tag] = make(map[string]struct{})
		}
		l.tagsMap[tag][key] = struct{}{}
	}
	return nil
}

func (l *LocalCache) InvalidateByTags(_ context.Context, tags []string) error {
	// Hold lock for the entire invalidation sweep to block concurrent Sets
	l.tagsMu.Lock()
	defer l.tagsMu.Unlock()

	for _, tag := range tags {
		if keys, ok := l.tagsMap[tag]; ok {
			for k := range keys {
				l.cache.Del(k)
				l.ttls.Delete(k)
				delete(l.keyTags, k)
			}
			delete(l.tagsMap, tag)
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
