package cache_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil/cachetest"
)

const localMaxCost = 1 << 20

func newLocal(t *testing.T) cache.Cache {
	t.Helper()
	c, err := cache.NewLocal(localMaxCost)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestLocalCache_Conformance(t *testing.T) {
	t.Parallel()
	cachetest.Run(t, newLocal, cachetest.Options{MaxValueBytes: localMaxCost})
}

// A tenant that stops being served has its index dropped: what it cached is
// orphaned — it misses when served again — and a tenant still served keeps
// its entries.
func TestLocalCache_Prune(t *testing.T) {
	t.Parallel()
	c, err := cache.NewLocal(localMaxCost)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	ctx := t.Context()
	fill := func(id tenant.ID) {
		_, snap, err := c.Lookup(ctx, id, "q", []cache.Namespace{{Tenant: id, Table: "events"}})
		require.NoError(t, err)
		require.NoError(t, c.Set(ctx, snap, []byte("rows"), time.Minute))
	}
	get := func(id tenant.ID) []byte {
		e, _, err := c.Lookup(ctx, id, "q", []cache.Namespace{{Tenant: id, Table: "events"}})
		require.NoError(t, err)
		return e.Value
	}
	fill("acme")
	fill("globex")
	c.Wait()
	require.NotNil(t, get("acme"))
	require.NotNil(t, get("globex"))

	c.Prune(func(id tenant.ID) bool { return id == "acme" })
	assert.NotNil(t, get("acme"), "still served")
	assert.Nil(t, get("globex"), "pruned: orphaned, never revived")
}
