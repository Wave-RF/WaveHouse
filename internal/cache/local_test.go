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

	val, ttl, err := c.Get(context.Background(), "missing")
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
	err = c.Set(ctx, "key1", []byte("hello"), 10*time.Second, nil)
	assert.NoError(t, err)

	c.cache.Wait()

	val, ttl, err := c.Get(ctx, "key1")
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
	err = c.Set(ctx, "expires", []byte("data"), 1*time.Millisecond, nil)
	assert.NoError(t, err)

	// Ensure async admission completes, then wait for expiry.
	c.Wait()
	time.Sleep(50 * time.Millisecond)

	val, _, err := c.Get(ctx, "expires")
	assert.NoError(t, err)
	assert.Nil(t, val, "expected nil for expired key")
}

func TestLocalCache_Overwrite(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	require.NoError(t, c.Set(ctx, "key", []byte("v1"), 10*time.Second, nil))
	c.Wait()
	require.NoError(t, c.Set(ctx, "key", []byte("v2"), 10*time.Second, nil))
	c.Wait()

	val, _, err := c.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestLocalCache_ZeroTTL(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	err = c.Set(ctx, "notimed", []byte("data"), 0, nil)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.Get(ctx, "notimed")
	assert.NoError(t, err)
	if val != nil {
		assert.Equal(t, []byte("data"), val)
		assert.Zero(t, ttl, "expected zero remaining TTL for key without TTL")
	}
}

func TestLocalCache_InvalidateByTags(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()

	// Pass the table names as the tags slice
	require.NoError(t, c.Set(ctx, "query:clicks:123", []byte("val1"), 10*time.Second, []string{"clicks"}))
	require.NoError(t, c.Set(ctx, "query:clicks:456", []byte("val2"), 0, []string{"clicks"}))

	// This key belongs to 'users', so it won't be wiped when we invalidate 'clicks'
	require.NoError(t, c.Set(ctx, "query:users:789", []byte("val3"), 10*time.Second, []string{"users"}))

	c.Wait()

	// Wipe out the clicks table cache
	err = c.InvalidateByTags(ctx, []string{"clicks"})
	assert.NoError(t, err)

	// Verify the target keys are gone
	val1, _, _ := c.Get(ctx, "query:clicks:123")
	assert.Nil(t, val1)

	val2, _, _ := c.Get(ctx, "query:clicks:456")
	assert.Nil(t, val2)

	// Verify the innocent key is still there
	val3, _, _ := c.Get(ctx, "query:users:789")
	assert.Equal(t, []byte("val3"), val3)
}
