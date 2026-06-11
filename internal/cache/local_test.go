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

	val, ttl, err := c.Get(context.Background(), "missing", []Namespace{{Table: "table"}})
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
	deps := []Namespace{{Table: "table", Scope: "scope"}}
	err = c.Set(ctx, "key1", deps, []byte("hello"), 10*time.Second)
	assert.NoError(t, err)

	// Ristretto uses async admission — wait briefly for it to be admitted.
	c.Wait()

	val, ttl, err := c.Get(ctx, "key1", deps)
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
	deps := []Namespace{{Table: "table"}}
	// Set with very short TTL.
	err = c.Set(ctx, "expires", deps, []byte("data"), 1*time.Millisecond)
	assert.NoError(t, err)

	// Ensure async admission completes, then wait for expiry.
	c.Wait()
	time.Sleep(50 * time.Millisecond)

	val, _, err := c.Get(ctx, "expires", deps)
	assert.NoError(t, err)
	assert.Nil(t, val, "expected nil for expired key")
}

func TestLocalCache_Overwrite(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	deps := []Namespace{{Table: "table"}}
	require.NoError(t, c.Set(ctx, "key", deps, []byte("v1"), 10*time.Second))
	c.Wait()
	require.NoError(t, c.Set(ctx, "key", deps, []byte("v2"), 10*time.Second))
	c.Wait()

	val, _, err := c.Get(ctx, "key", deps)
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestLocalCache_ZeroTTL(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	deps := []Namespace{{Table: "table"}}
	err = c.Set(ctx, "notimed", deps, []byte("data"), 0)
	assert.NoError(t, err)

	c.Wait()
	time.Sleep(10 * time.Millisecond) // arbitrary tiny sleep to see its still here after

	val, ttl, err := c.Get(ctx, "notimed", deps)
	assert.NoError(t, err)
	if val != nil {
		assert.Equal(t, []byte("data"), val)
		assert.Zero(t, ttl, "expected zero remaining TTL for key without TTL")
	}
}

func TestLocalCache_Invalidate(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	deps := []Namespace{{Table: "users", Scope: "org_1"}}

	// Set value
	err = c.Set(ctx, "queryHash", deps, []byte("my_data"), 10*time.Second)
	assert.NoError(t, err)
	c.Wait()

	// Ensure readable
	val, _, err := c.Get(ctx, "queryHash", deps)
	assert.NoError(t, err)
	assert.Equal(t, []byte("my_data"), val)

	// Invalidate the (users, org_1) namespace.
	count, err := c.Invalidate(ctx, deps)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	// The folded key embeds the namespace version, which was just bumped, so this
	// must now miss.
	valAfter, ttlAfter, errAfter := c.Get(ctx, "queryHash", deps)
	assert.NoError(t, errAfter)
	assert.Nil(t, valAfter)
	assert.Zero(t, ttlAfter)
}

// Invalidate with an empty-scope namespace bumps the whole table, which must
// orphan that table's scoped entries too — not just the whole-table view.
func TestLocalCache_Invalidate_WholeTable(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	scoped := []Namespace{{Table: "events", Scope: "org_1"}}

	require.NoError(t, c.Set(ctx, "q", scoped, []byte("v1"), 10*time.Second))
	c.Wait()
	val, _, err := c.Get(ctx, "q", scoped)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// Whole-table invalidation (empty scope) must orphan the scoped entry.
	_, err = c.Invalidate(ctx, []Namespace{{Table: "events"}})
	require.NoError(t, err)

	after, _, err := c.Get(ctx, "q", scoped)
	assert.NoError(t, err)
	assert.Nil(t, after, "whole-table bump must invalidate the scoped entry")
}
