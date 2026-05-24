package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/dgraph-io/ristretto/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// L1 cache attribute sets — pre-allocated once so the hot path on each Get
// stays free of per-call metric.WithAttributes/attribute.String slice allocs.
//
// Singleflight collapses are a SEPARATE concern from cache hits — the cache
// layer doesn't see singleflight (that's the handler's wrapping around the
// fill function). The cross-cutting singleflight counter
// (wavehouse_query_singleflight_shared_total) lives at the handler level
// in internal/api/structured_query.go and internal/api/pipes.go where shared
// is actually observable.
var (
	cacheL1HitAttrs  = metric.WithAttributes(attribute.String("tier", "L1"))
	cacheL1MissAttrs = metric.WithAttributes(attribute.String("tier", "L1"))
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

func (l *LocalCache) Get(ctx context.Context, key string, namespace string, scope string) ([]byte, time.Duration, error) {
	cacheKey := l.versionManager.GetCacheKey(key, namespace, scope)

	val, foundVal := l.cache.Get(cacheKey)
	if !foundVal {
		observability.CacheMisses.Add(ctx, 1, cacheL1MissAttrs)
		return nil, 0, nil
	}
	remaining, _ := l.cache.GetTTL(cacheKey)
	observability.CacheHits.Add(ctx, 1, cacheL1HitAttrs)

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

func (l *LocalCache) InvalidateCache(_ context.Context, table string, scopes map[string]struct{}) (uint64, error) {
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
