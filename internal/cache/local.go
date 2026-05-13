package cache

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// cacheValue wraps the cached payload with its original string key so that
// Ristretto's OnEvict callback (which only has access to a hashed uint64 key)
// can still scrub the tag index.
type cacheValue struct {
	key  string
	data []byte
}

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache   *ristretto.Cache[string, *cacheValue]
	ttls    sync.Map
	tagsMu  sync.RWMutex
	tagsMap map[string]map[string]struct{}
	keyTags map[string][]string
}

// NewLocal creates a new Ristretto-backed local cache.
func NewLocal(maxCost int64) (*LocalCache, error) {
	l := &LocalCache{
		tagsMap: make(map[string]map[string]struct{}),
		keyTags: make(map[string][]string),
	}

	c, err := ristretto.NewCache(&ristretto.Config[string, *cacheValue]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
		OnEvict: func(item *ristretto.Item[*cacheValue]) {
			// Extract the original string key from the wrapped value payload
			if item.Value != nil {
				l.tagsMu.Lock()
				defer l.tagsMu.Unlock()
				l.removeKeyFromTagsLocked(item.Value.key)
				l.ttls.Delete(item.Value.key)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	l.cache = c
	return l, nil
}

// removeKeyFromTagsLocked handles cross-tag cleanup. MUST be called with tagsMu locked.
func (l *LocalCache) removeKeyFromTagsLocked(key string) {
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
}

func (l *LocalCache) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	val, found := l.cache.Get(key)
	if !found || val == nil {
		return nil, 0, nil
	}

	var remaining time.Duration
	if expVal, ok := l.ttls.Load(key); ok {
		exp := expVal.(time.Time)
		if !exp.IsZero() {
			remaining = time.Until(exp)
			if remaining <= 0 {
				l.tagsMu.Lock()
				expVal2, ok2 := l.ttls.Load(key)
				if !ok2 || time.Until(expVal2.(time.Time)) <= 0 {
					l.cache.Del(key)
					l.ttls.Delete(key)
					l.removeKeyFromTagsLocked(key)
					l.tagsMu.Unlock()
					return nil, 0, nil
				}
				// If we get here, a concurrent Set updated the key while we waited for the lock
				l.tagsMu.Unlock()
				val, _ = l.cache.Get(key) // Fetch the fresh value
				if val == nil {
					return nil, 0, nil
				}
				remaining = time.Until(expVal2.(time.Time))
			}
		}
	}
	// Return the unwrapped byte array to the caller
	return val.data, remaining, nil
}

func (l *LocalCache) Set(_ context.Context, key string, value []byte, ttl time.Duration, tags []string) error {
	admitted := l.cache.SetWithTTL(key, &cacheValue{key: key, data: value}, int64(len(value)), ttl)

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	l.tagsMu.Lock()
	defer l.tagsMu.Unlock()

	if ttl > 0 {
		l.ttls.Store(key, exp)
	}

	l.removeKeyFromTagsLocked(key)

	if admitted && len(tags) > 0 {
		l.keyTags[key] = tags
		for _, tag := range tags {
			if l.tagsMap[tag] == nil {
				l.tagsMap[tag] = make(map[string]struct{})
			}
			l.tagsMap[tag][key] = struct{}{}
		}
	}

	return nil
}

func (l *LocalCache) InvalidateByTags(_ context.Context, tags []string) error {
	l.tagsMu.Lock()
	defer l.tagsMu.Unlock()

	for _, tag := range tags {
		if keys, ok := l.tagsMap[tag]; ok {
			// Snapshot the keys first to avoid fragile iteration invariants
			// while deleting from the same interconnected maps
			toDelete := make([]string, 0, len(keys))
			for k := range keys {
				toDelete = append(toDelete, k)
			}

			for _, k := range toDelete {
				l.cache.Del(k)
				l.ttls.Delete(k)
				l.removeKeyFromTagsLocked(k)
			}
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
