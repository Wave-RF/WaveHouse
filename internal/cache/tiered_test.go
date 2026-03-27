package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memCache struct {
	store map[string]cacheEntry
}

type cacheEntry struct {
	data []byte
	ttl  time.Duration
}

func newMemCache() *memCache { return &memCache{store: make(map[string]cacheEntry)} }

func (m *memCache) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	e, ok := m.store[key]
	if !ok {
		return nil, 0, nil
	}
	return e.data, e.ttl, nil
}

func (m *memCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.store[key] = cacheEntry{data: value, ttl: ttl}
	return nil
}

func (m *memCache) Close() error { return nil }

func TestTieredCache_L1Hit(t *testing.T) {
	t.Parallel()
	l1 := newMemCache()
	l2 := newMemCache()
	tc := NewTiered(l1, l2)
	ctx := context.Background()
	require.NoError(t, l1.Set(ctx, "key", []byte("l1-value"), 5*time.Minute))
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
	require.NoError(t, l2.Set(ctx, "key", []byte("l2-value"), 3*time.Minute))
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
	require.NoError(t, tc.Set(ctx, "key", []byte("both"), 5*time.Minute))
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
	require.NoError(t, tc.Set(ctx, "key", []byte("val"), time.Minute))
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
