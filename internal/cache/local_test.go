package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalCache_GetMiss(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20) // 1 MB
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	val, ttl, err := c.Get(context.Background(), "missing", "", "")
	assert.NoError(t, err)
	assert.Nil(t, val)
	assert.Zero(t, ttl)
}

func TestLocalCache_SetAndGet(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	err = c.Set(ctx, "key1", "table", "scope", []byte("hello"), 10*time.Second)
	assert.NoError(t, err)

	// Ristretto uses async admission — wait briefly for it to be admitted.
	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.Get(ctx, "key1", "table", "scope")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), val)
	assert.True(t, ttl > 0, "expected positive remaining TTL")
}

func TestLocalCache_ExpiredKey(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	// Set with very short TTL.
	err = c.Set(ctx, "expires", "table", "", []byte("data"), 1*time.Millisecond)
	assert.NoError(t, err)

	// Ensure async admission completes, then wait for expiry.
	c.Wait()
	time.Sleep(50 * time.Millisecond)

	val, _, err := c.Get(ctx, "expires", "table", "")
	assert.NoError(t, err)
	assert.Nil(t, val, "expected nil for expired key")
}

func TestLocalCache_Overwrite(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	require.NoError(t, c.Set(ctx, "key", "table", "", []byte("v1"), 10*time.Second))
	c.Wait()
	require.NoError(t, c.Set(ctx, "key", "table", "", []byte("v2"), 10*time.Second))
	c.Wait()

	val, _, err := c.Get(ctx, "key", "table", "")
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestLocalCache_ZeroTTL(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	err = c.Set(ctx, "notimed", "table", "", []byte("data"), 0)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.Get(ctx, "notimed", "table", "")
	assert.NoError(t, err)
	if val != nil {
		assert.Equal(t, []byte("data"), val)
		assert.Zero(t, ttl, "expected zero remaining TTL for key without TTL")
	}
}

func TestLocalCache_InvalidateCache(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()

	// Set value
	err = c.Set(ctx, "queryHash", "users", "org_1", []byte("my_data"), 10*time.Second)
	assert.NoError(t, err)
	c.Wait()
	time.Sleep(10 * time.Millisecond) // wait for admission

	// Ensure readable
	val, _, err := c.Get(ctx, "queryHash", "users", "org_1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my_data"), val)

	// Invalidate cache for the scope
	scopes := map[string]struct{}{"org_1": {}}
	count, err := c.InvalidateCache(ctx, "users", scopes)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count) // Returns len(keys): "users" and "users.org_1"

	// Try fetching again
	// Since the underlying cache key includes the version (which was just incremented), this should result in a cache miss
	valAfter, ttlAfter, errAfter := c.Get(ctx, "queryHash", "users", "org_1")
	assert.NoError(t, errAfter)
	assert.Nil(t, valAfter)
	assert.Zero(t, ttlAfter)
}
