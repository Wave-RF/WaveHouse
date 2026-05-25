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

// cacheL1Attrs is pre-allocated so cache hot paths don't allocate per call.
// Singleflight collapses are tracked at the handler level (where shared is
// observable), not here.
var cacheL1Attrs = metric.WithAttributes(attribute.String("tier", "L1"))

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
		observability.CacheMisses.Add(ctx, 1, cacheL1Attrs)
		return nil, 0, nil
	}
	remaining, _ := l.cache.GetTTL(cacheKey)
	observability.CacheHits.Add(ctx, 1, cacheL1Attrs)

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
