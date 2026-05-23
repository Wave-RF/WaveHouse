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

	val, ttl, err := c.GetQuery(context.Background(), "missing", "", "")
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
	err = c.SetQuery(ctx, "key1", "table", "scope", []byte("hello"), 10*time.Second)
	assert.NoError(t, err)

	// Ristretto uses async admission — wait briefly for it to be admitted.
	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.GetQuery(ctx, "key1", "table", "scope")
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
	err = c.SetQuery(ctx, "expires", "table", "", []byte("data"), 1*time.Millisecond)
	assert.NoError(t, err)

	// Ensure async admission completes, then wait for expiry.
	c.Wait()
	time.Sleep(50 * time.Millisecond)

	val, _, err := c.GetQuery(ctx, "expires", "table", "")
	assert.NoError(t, err)
	assert.Nil(t, val, "expected nil for expired key")
}

func TestLocalCache_Overwrite(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	require.NoError(t, c.SetQuery(ctx, "key", "table", "", []byte("v1"), 10*time.Second))
	c.Wait()
	require.NoError(t, c.SetQuery(ctx, "key", "table", "", []byte("v2"), 10*time.Second))
	c.Wait()

	val, _, err := c.GetQuery(ctx, "key", "table", "")
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestLocalCache_ZeroTTL(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	err = c.SetQuery(ctx, "notimed", "table", "", []byte("data"), 0)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.GetQuery(ctx, "notimed", "table", "")
	assert.NoError(t, err)
	if val != nil {
		assert.Equal(t, []byte("data"), val)
		assert.Zero(t, ttl, "expected zero remaining TTL for key without TTL")
	}
}
