package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// cacheValue wraps the cached payload with its original string key and a version nonce.
type cacheValue struct {
	key     string
	data    []byte
	version uint64
}

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache       *ristretto.Cache[string, *cacheValue]
	ttls        sync.Map
	tagsMu      sync.RWMutex
	tagsMap     map[string]map[string]struct{}
	keyTags     map[string][]string
	nextVersion atomic.Uint64
	keyVersion  map[string]uint64
}

// NewLocal creates a new Ristretto-backed local cache.
func NewLocal(maxCost int64) (*LocalCache, error) {
	l := &LocalCache{
		tagsMap:    make(map[string]map[string]struct{}),
		keyTags:    make(map[string][]string),
		keyVersion: make(map[string]uint64),
	}

	c, err := ristretto.NewCache(&ristretto.Config[string, *cacheValue]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
		OnEvict: func(item *ristretto.Item[*cacheValue]) {
			if item.Value == nil {
				return
			}
			key := item.Value.key
			version := item.Value.version

			l.tagsMu.Lock()
			defer l.tagsMu.Unlock()

			// If the key has been overwritten by a newer Set, ignore this old callback.
			if currentVer, exists := l.keyVersion[key]; exists && currentVer != version {
				return
			}

			l.removeKeyFromTagsLocked(key)
			l.ttls.Delete(key)
			delete(l.keyVersion, key)
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
				l.cache.Del(key)
				l.ttls.Delete(key)
				delete(l.keyVersion, key) // Clean up version tracker
				l.removeKeyFromTagsLocked(key)
				l.tagsMu.Unlock()
				return nil, 0, nil
			}
		}
	}
	// Return the unwrapped byte array to the caller
	return val.data, remaining, nil
}

func (l *LocalCache) Set(_ context.Context, key string, value []byte, ttl time.Duration, tags []string) error {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	l.tagsMu.Lock()
	defer l.tagsMu.Unlock()

	version := l.nextVersion.Add(1)

	admitted := l.cache.SetWithTTL(key, &cacheValue{key: key, data: value, version: version}, int64(len(value)), ttl)

	if admitted {
		l.keyVersion[key] = version
		l.removeKeyFromTagsLocked(key)

		if ttl > 0 {
			l.ttls.Store(key, exp)
		}

		if len(tags) > 0 {
			l.keyTags[key] = tags
			for _, tag := range tags {
				if l.tagsMap[tag] == nil {
					l.tagsMap[tag] = make(map[string]struct{})
				}
				l.tagsMap[tag][key] = struct{}{}
			}
		}
	}

	return nil
}

func (l *LocalCache) InvalidateByTags(_ context.Context, tags []string) error {
	// Flush Ristretto's async write buffer before deleting. SetWithTTL returns
	// admitted=true before the item reaches the store; without Wait(), Del may
	// be a no-op for recently-admitted keys, leaving stale untracked entries.
	l.cache.Wait()

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
				delete(l.keyVersion, k)
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
