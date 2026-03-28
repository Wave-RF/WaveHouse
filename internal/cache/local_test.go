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
	defer c.Close()

	val, ttl, err := c.Get(context.Background(), "missing")
	assert.NoError(t, err)
	assert.Nil(t, val)
	assert.Zero(t, ttl)
}

func TestLocalCache_SetAndGet(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	err = c.Set(ctx, "key1", []byte("hello"), 10*time.Second)
	assert.NoError(t, err)

	// Ristretto uses async admission — wait briefly for it to be admitted.
	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.Get(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), val)
	assert.True(t, ttl > 0, "expected positive remaining TTL")
}

func TestLocalCache_ExpiredKey(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	// Set with very short TTL.
	err = c.Set(ctx, "expires", []byte("data"), 1*time.Millisecond)
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
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "key", []byte("v1"), 10*time.Second)
	time.Sleep(10 * time.Millisecond)
	_ = c.Set(ctx, "key", []byte("v2"), 10*time.Second)
	time.Sleep(10 * time.Millisecond)

	val, _, err := c.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestLocalCache_ZeroTTL(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	err = c.Set(ctx, "notimed", []byte("data"), 0)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	val, ttl, err := c.Get(ctx, "notimed")
	assert.NoError(t, err)
	if val != nil {
		assert.Equal(t, []byte("data"), val)
		assert.Zero(t, ttl, "expected zero remaining TTL for key without TTL")
	}
}
