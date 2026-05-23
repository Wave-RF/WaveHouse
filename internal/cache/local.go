package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	queryCache   *ristretto.Cache[string, []byte]
	pipeCache    *ristretto.Cache[string, []byte]
	versionTable *ristretto.Cache[string, uint64]
}

// NewLocal creates a new Ristretto-backed local cache.
func NewLocal(maxCost int64) (*LocalCache, error) {
	maxCost /= 3 // split cache space amongst the three evenly – eventually would like a better distribution but oh well

	qc, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	pc, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	vt, err := ristretto.NewCache(&ristretto.Config[string, uint64]{
		NumCounters: maxCost / 100 * 10,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &LocalCache{queryCache: qc, pipeCache: pc, versionTable: vt}, nil
}

func (l *LocalCache) GetQuery(_ context.Context, key string, table string, scope string) ([]byte, time.Duration, error) {
	cacheKey := l.getCacheKey(key, table, scope)

	val, foundVal := l.queryCache.Get(cacheKey)
	remaining, foundRemaining := l.queryCache.GetTTL(cacheKey)
	if !foundVal || !foundRemaining {
		return nil, 0, nil
	}

	return val, remaining, nil
}

func (l *LocalCache) SetQuery(_ context.Context, key string, table string, scope string, value []byte, ttl time.Duration) error {
	cacheKey := l.getCacheKey(key, table, scope)

	// set cost = 0 for dynamic cost evaluation
	l.queryCache.SetWithTTL(cacheKey, value, 0, ttl)
	return nil
}

func (l *LocalCache) GetPipe(_ context.Context, key string) ([]byte, time.Duration, error) {
	val, foundVal := l.pipeCache.Get(key)
	remaining, foundRemaining := l.pipeCache.GetTTL(key)
	if !foundVal || !foundRemaining {
		return nil, 0, nil
	}

	return val, remaining, nil
}

func (l *LocalCache) SetPipe(_ context.Context, key string, value []byte, ttl time.Duration) error {
	// set cost = 0 for dynamic cost evaluation
	l.pipeCache.SetWithTTL(key, value, 0, ttl)
	return nil
}

// Wait blocks until all buffered writes have been applied.
// Exposed for testing; production callers rarely need this.
func (l *LocalCache) Wait() {
	l.queryCache.Wait()
	l.pipeCache.Wait()
	l.versionTable.Wait()
}

func (l *LocalCache) InvalidateCache(ctx context.Context, table string, scopes map[string]struct{}) (uint64, error) {
	keys := generateInvalidationKeys(table, scopes)

	for _, key := range keys {
		val := l.getVersion(key)
		l.versionTable.Set(key, val+1, 0)
	}
	return uint64(len(keys)), nil
}

func (l *LocalCache) getVersion(key string) uint64 {
	version, found := l.versionTable.Get(key)
	if !found {
		return 0
	}
	return version
}

func (l *LocalCache) getCacheKey(key string, table string, scope string) string {
	versionKey := generateVersionKey(table, scope)
	version := l.getVersion(versionKey)

	return fmt.Sprintf("%s.%d:%s", versionKey, version, key)
}

func (l *LocalCache) Close() error {
	l.Wait()
	l.queryCache.Close()
	l.pipeCache.Close()
	l.versionTable.Close()
	return nil
}
