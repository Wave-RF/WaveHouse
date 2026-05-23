package cache

import (
	"context"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache          *ristretto.Cache[string, []byte]
	versionManager *VersionManager
}

// NewLocal creates a new Ristretto-backed local cache.
func NewLocal(maxCost int64) (*LocalCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	vm := NewVersionManager(nil)
	return &LocalCache{cache: cache, versionManager: vm}, nil
}

func (l *LocalCache) Get(_ context.Context, key string, namespace string, scope string) ([]byte, time.Duration, error) {
	cacheKey := l.versionManager.GetCacheKey(key, namespace, scope)

	val, foundVal := l.cache.Get(cacheKey)
	remaining, foundRemaining := l.cache.GetTTL(cacheKey)
	if !foundVal || !foundRemaining {
		return nil, 0, nil
	}

	return val, remaining, nil
}

func (l *LocalCache) Set(_ context.Context, key string, namespace string, scope string, value []byte, ttl time.Duration) error {
	cacheKey := l.versionManager.GetCacheKey(key, namespace, scope)

	// set cost = 0 for dynamic cost evaluation
	l.cache.SetWithTTL(cacheKey, value, int64(len(value)), ttl)
	return nil
}

func (l *LocalCache) InvalidateCache(ctx context.Context, table string, scopes map[string]struct{}) (uint64, error) {
	keys := generateInvalidationKeys(table, scopes)

	for _, key := range keys {
		l.versionManager.IncrementVersion(key)
	}
	return uint64(len(keys)), nil
}

// Wait blocks until all buffered writes have been applied.
// Exposed for testing; production callers rarely need this.
func (l *LocalCache) Wait() {
	l.cache.Wait()
}

func (l *LocalCache) Close() error {
	l.Wait()
	l.cache.Close()
	return nil
}
