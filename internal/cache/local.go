package cache

import (
	"context"
	"fmt"
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
		NumCounters: max(maxCost/10, 1000),
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
	if !foundVal {
		return nil, 0, nil
	}
	remaining, _ := l.cache.GetTTL(cacheKey)

	return val, remaining, nil
}

func (l *LocalCache) Set(_ context.Context, key string, namespace string, scope string, value []byte, ttl time.Duration) error {
	cacheKey := l.versionManager.GetCacheKey(key, namespace, scope)

	// set cost = 0 for dynamic cost evaluation
	if ok := l.cache.SetWithTTL(cacheKey, value, int64(len(value)), ttl); !ok {
		return fmt.Errorf("cache admission rejected for key %q", cacheKey)
	}
	return nil
}

func (l *LocalCache) InvalidateCache(_ context.Context, versionKeys []string) (uint64, error) {
	for _, key := range versionKeys {
		l.versionManager.IncrementVersion(key)
	}
	return uint64(len(versionKeys)), nil
}

// GetQuery looks up a cached query RESULT by its sha (hash of SQL+params) and the
// namespaces it depends on. This is the general namespace-cache read, used by BOTH
// structured queries (which pass one Namespace) and pipes (which pass several).
// Returns nil, 0, nil on miss.
func (l *LocalCache) GetQuery(_ context.Context, sha string, deps []Namespace) ([]byte, time.Duration, error) {
	cacheKey := l.versionManager.QueryKey(sha, deps)

	val, found := l.cache.Get(cacheKey)
	if !found {
		return nil, 0, nil
	}
	remaining, _ := l.cache.GetTTL(cacheKey)
	return val, remaining, nil
}

// SetQuery stores a query result under the folded key for its dependency
// namespaces. General write used by both structured queries and pipes.
func (l *LocalCache) SetQuery(_ context.Context, sha string, deps []Namespace, value []byte, ttl time.Duration) error {
	cacheKey := l.versionManager.QueryKey(sha, deps)

	if ok := l.cache.SetWithTTL(cacheKey, value, int64(len(value)), ttl); !ok {
		return fmt.Errorf("cache admission rejected for key %q", cacheKey)
	}
	return nil
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
