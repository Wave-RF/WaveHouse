package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memCache struct {
	mu    sync.RWMutex
	store map[string]cacheEntry
}

type cacheEntry struct {
	data []byte
	ttl  time.Duration
	tags []string
}

func newMemCache() *memCache { return &memCache{store: make(map[string]cacheEntry)} }

func (m *memCache) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	m.mu.RLock()
	e, ok := m.store[key]
	m.mu.RUnlock()
	if !ok {
		return nil, 0, nil
	}
	return e.data, e.ttl, nil
}

func (m *memCache) Set(_ context.Context, key string, value []byte, ttl time.Duration, tags []string) error {
	m.mu.Lock()
	m.store[key] = cacheEntry{data: value, ttl: ttl, tags: tags}
	m.mu.Unlock()
	return nil
}

func (m *memCache) InvalidateByTags(_ context.Context, tags []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, entry := range m.store {
		for _, targetTag := range tags {
			for _, entryTag := range entry.tags {
				if targetTag == entryTag {
					delete(m.store, k)
					goto NextKey
				}
			}
		}
	NextKey:
	}
	return nil
}

func (m *memCache) Close() error { return nil }

func TestTieredCache_L1Hit(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	l2 := newMemCache()
	tc := NewTiered(l1, l2)
	ctx := context.Background()
	require.NoError(t, l1.Set(ctx, "key", []byte("l1-value"), 5*time.Minute, nil))
	val, ttl, err := tc.Get(ctx, "key")
	require.NoError(t, err)
	assert.Equal(t, []byte("l1-value"), val)
	assert.Equal(t, 5*time.Minute, ttl)
}

func TestTieredCache_L1Miss_L2Hit(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	l2 := newMemCache()
	tc := NewTiered(l1, l2)
	ctx := context.Background()
	require.NoError(t, l2.Set(ctx, "key", []byte("l2-value"), 3*time.Minute, nil))
	val, ttl, err := tc.Get(ctx, "key")
	require.NoError(t, err)
	assert.Equal(t, []byte("l2-value"), val)
	assert.Equal(t, 3*time.Minute, ttl)
	// L1 should now have the value (backfill).
	l1val, _, _ := l1.Get(ctx, "key")
	assert.Equal(t, []byte("l2-value"), l1val)
}

func TestTieredCache_BothMiss(t *testing.T) {
	t.Parallel()
	tc := NewTiered(newMemCache(), newMemCache())
	val, ttl, err := tc.Get(context.Background(), "miss")
	require.NoError(t, err)
	assert.Nil(t, val)
	assert.Zero(t, ttl)
}

func TestTieredCache_Set_WritesToBoth(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	l2 := newMemCache()
	tc := NewTiered(l1, l2)
	ctx := context.Background()
	require.NoError(t, tc.Set(ctx, "key", []byte("both"), 5*time.Minute, nil))
	l1val, _, _ := l1.Get(ctx, "key")
	l2val, _, _ := l2.Get(ctx, "key")
	assert.Equal(t, []byte("both"), l1val)
	assert.Equal(t, []byte("both"), l2val)
}

func TestTieredCache_Set_NilL2(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	tc := NewTiered(l1, nil)
	ctx := context.Background()
	require.NoError(t, tc.Set(ctx, "key", []byte("val"), time.Minute, nil))
	val, _, _ := l1.Get(ctx, "key")
	assert.Equal(t, []byte("val"), val)
}

func TestTieredCache_Close(t *testing.T) {
	t.Parallel()
	assert.NoError(t, NewTiered(newMemCache(), newMemCache()).Close())
}

func TestTieredCache_Close_NilL2(t *testing.T) {
	t.Parallel()
	assert.NoError(t, NewTiered(newMemCache(), nil).Close())
}

func TestTieredCache_SingleflightDedup(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	l2 := &slowCache{inner: newMemCache(), delay: 50 * time.Millisecond}
	tc := NewTiered(l1, l2)
	ctx := context.Background()
	require.NoError(t, l2.Set(ctx, "key", []byte("slow-val"), 5*time.Minute, nil))

	// Launch many concurrent Gets for the same key.
	const n = 20
	done := make(chan []byte, n)
	for i := 0; i < n; i++ {
		go func() {
			val, _, _ := tc.Get(ctx, "key")
			done <- val
		}()
	}

	for i := 0; i < n; i++ {
		val := <-done
		assert.Equal(t, []byte("slow-val"), val)
	}

	// Despite N concurrent calls, L2 should have been hit exactly once (singleflight).
	count := l2.getCount.Load()
	assert.Equal(t, int32(1), count, "expected singleflight to coalesce L2 Gets to 1")
}

func TestTieredCache_InvalidateByTags(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	l2 := newMemCache()
	tc := NewTiered(l1, l2)
	ctx := context.Background()

	require.NoError(t, tc.Set(ctx, "q1", []byte("val"), time.Minute, []string{"clicks"}))
	require.NoError(t, tc.InvalidateByTags(ctx, []string{"clicks"}))

	v1, _, _ := tc.Get(ctx, "q1")
	assert.Nil(t, v1)
}

// slowCache wraps memCache with a delay and an atomic access counter for singleflight testing.
type slowCache struct {
	inner    *memCache
	delay    time.Duration
	getCount atomic.Int32
}

func (s *slowCache) Get(ctx context.Context, key string) ([]byte, time.Duration, error) {
	s.getCount.Add(1)
	time.Sleep(s.delay)
	return s.inner.Get(ctx, key)
}

func (s *slowCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration, tags []string) error {
	return s.inner.Set(ctx, key, value, ttl, tags)
}

func (s *slowCache) InvalidateByTags(ctx context.Context, tags []string) error {
	return s.inner.InvalidateByTags(ctx, tags)
}

func (s *slowCache) Close() error { return nil }
