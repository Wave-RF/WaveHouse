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

	val, ttl, err := c.Get(context.Background(), c.Key("missing", []Namespace{{Table: "table"}}))
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
	key := c.Key("key1", []Namespace{{Table: "table", Scope: "scope"}})
	err = c.Set(ctx, key, []byte("hello"), 10*time.Second)
	assert.NoError(t, err)

	// Ristretto uses async admission — wait briefly for it to be admitted.
	c.Wait()

	val, ttl, err := c.Get(ctx, key)
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
	key := c.Key("expires", []Namespace{{Table: "table"}})
	// Set with very short TTL.
	err = c.Set(ctx, key, []byte("data"), 1*time.Millisecond)
	assert.NoError(t, err)

	// Ensure async admission completes, then wait for expiry.
	c.Wait()
	time.Sleep(50 * time.Millisecond)

	val, _, err := c.Get(ctx, key)
	assert.NoError(t, err)
	assert.Nil(t, val, "expected nil for expired key")
}

func TestLocalCache_Overwrite(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	key := c.Key("key", []Namespace{{Table: "table"}})
	require.NoError(t, c.Set(ctx, key, []byte("v1"), 10*time.Second))
	c.Wait()
	require.NoError(t, c.Set(ctx, key, []byte("v2"), 10*time.Second))
	c.Wait()

	val, _, err := c.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestLocalCache_ZeroTTL(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	key := c.Key("notimed", []Namespace{{Table: "table"}})
	err = c.Set(ctx, key, []byte("data"), 0)
	assert.NoError(t, err)

	c.Wait()
	time.Sleep(10 * time.Millisecond) // arbitrary tiny sleep to see its still here after

	val, ttl, err := c.Get(ctx, key)
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
	err = c.Set(ctx, c.Key("queryHash", deps), []byte("my_data"), 10*time.Second)
	assert.NoError(t, err)
	c.Wait()

	// Ensure readable
	val, _, err := c.Get(ctx, c.Key("queryHash", deps))
	assert.NoError(t, err)
	assert.Equal(t, []byte("my_data"), val)

	// Invalidate the (users, org_1) namespace.
	count, err := c.Invalidate(ctx, deps)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	// The folded key embeds the namespace version, which was just bumped, so a
	// reader folding the key now must miss.
	valAfter, ttlAfter, errAfter := c.Get(ctx, c.Key("queryHash", deps))
	assert.NoError(t, errAfter)
	assert.Nil(t, valAfter)
	assert.Zero(t, ttlAfter)
}

// The Cache.Key contract: the folded key is snapshotted BEFORE the query runs,
// and an Invalidate landing mid-flight (between the snapshot and the Set) must
// orphan the entry — the Set lands under the pre-bump key, which no post-bump
// reader folds. Without the snapshot (folding versions at Set time instead), the
// pre-write result would be stored under the post-bump key and served as fresh
// for its whole TTL: a stale read no write could ever evict.
func TestLocalCache_MidFlightInvalidate_OrphansSet(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	deps := []Namespace{{Table: "events"}}

	// A read misses and snapshots its key, then starts executing the query...
	key := c.Key("sha", deps)

	// ...a write lands and invalidates while the query is in flight...
	_, err = c.Invalidate(ctx, deps)
	require.NoError(t, err)

	// ...and the (pre-write) result is stored under the snapshot key.
	require.NoError(t, c.Set(ctx, key, []byte("pre-write rows"), time.Hour))
	c.Wait()

	// A reader folding the key AFTER the write must not see the pre-write result.
	val, _, err := c.Get(ctx, c.Key("sha", deps))
	assert.NoError(t, err)
	assert.Nil(t, val, "a result computed before a mid-flight write must not be served after it")
}

// Any non-empty Invalidate batch also bumps the DATABASE version: an entry
// folding only DatabaseNamespace (the pipe dependency-resolution fallback) is
// evicted by a write to ANY table — including one no registry has discovered —
// mirroring how a whole-table bump subsumes its scopes, one level up.
func TestLocalCache_Invalidate_BumpsDatabaseVersion(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	deps := []Namespace{DatabaseNamespace()}

	require.NoError(t, c.Set(ctx, c.Key("fallback", deps), []byte("v1"), time.Hour))
	c.Wait()
	val, _, err := c.Get(ctx, c.Key("fallback", deps))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// A write to an arbitrary, never-registered table must evict it.
	_, err = c.Invalidate(ctx, []Namespace{{Table: "some_table"}})
	require.NoError(t, err)
	after, _, err := c.Get(ctx, c.Key("fallback", deps))
	assert.NoError(t, err)
	assert.Nil(t, after, "any write must evict a database-version-keyed entry")

	// A scoped write evicts it too — the database bump is per batch, not per kind.
	require.NoError(t, c.Set(ctx, c.Key("fallback", deps), []byte("v2"), time.Hour))
	c.Wait()
	_, err = c.Invalidate(ctx, []Namespace{{Table: "events", Scope: "org_1"}})
	require.NoError(t, err)
	after, _, err = c.Get(ctx, c.Key("fallback", deps))
	assert.NoError(t, err)
	assert.Nil(t, after, "a scoped write must also evict a database-version-keyed entry")
}

// SetDependents installs the base-table -> dependent-view cascade: a write to a
// base table must orphan an entry keyed by a VIEW namespace (what a pipe reading
// the view folds), even though the view itself is never written. A write to an
// unrelated table must not.
func TestLocalCache_Invalidate_CascadesToDependentViews(t *testing.T) {
	t.Parallel()
	c, err := NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	// A pipe reads a view, so its result is keyed by the VIEW's namespace.
	viewDeps := []Namespace{{Table: "page_views_mv"}}
	c.SetDependents(map[string][]string{"page_views": {"page_views_mv"}})

	require.NoError(t, c.Set(ctx, c.Key("pipe", viewDeps), []byte("v1"), 10*time.Second))
	c.Wait()
	val, _, err := c.Get(ctx, c.Key("pipe", viewDeps))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// A write to an unrelated table leaves the view-keyed entry intact.
	_, err = c.Invalidate(ctx, []Namespace{{Table: "other"}})
	require.NoError(t, err)
	still, _, err := c.Get(ctx, c.Key("pipe", viewDeps))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), still, "an unrelated write must not cascade")

	// A write to the BASE table cascades to the view and orphans the entry.
	_, err = c.Invalidate(ctx, []Namespace{{Table: "page_views"}})
	require.NoError(t, err)
	after, _, err := c.Get(ctx, c.Key("pipe", viewDeps))
	assert.NoError(t, err)
	assert.Nil(t, after, "a base-table write must cascade-invalidate the view-keyed entry")
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

	require.NoError(t, c.Set(ctx, c.Key("q", scoped), []byte("v1"), 10*time.Second))
	c.Wait()
	val, _, err := c.Get(ctx, c.Key("q", scoped))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// Whole-table invalidation (empty scope) must orphan the scoped entry.
	_, err = c.Invalidate(ctx, []Namespace{{Table: "events"}})
	require.NoError(t, err)

	after, _, err := c.Get(ctx, c.Key("q", scoped))
	assert.NoError(t, err)
	assert.Nil(t, after, "whole-table bump must invalidate the scoped entry")
}
