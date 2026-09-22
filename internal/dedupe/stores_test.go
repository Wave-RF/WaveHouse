package dedupe

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// pebbleStores is a Stores over a temp root, one directory per tenant, with
// the function naming each tenant's directory.
func pebbleStores(t *testing.T) (*Stores, func(tenant.ID) string) {
	t.Helper()
	root := t.TempDir()
	dir := func(id tenant.ID) string { return filepath.Join(root, id.String(), "dedupe") }
	s := NewStores(func(id tenant.ID) *Managed { return NewManaged(Embedded(dir(id))) })
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func TestStores_ForBuildsOneClosedStorePerTenant(t *testing.T) {
	t.Parallel()
	s, dir := pebbleStores(t)
	ctx := context.Background()

	acme := s.For("acme")
	assert.Same(t, acme, s.For("acme"), "one store per tenant, however often it is named")
	assert.NotSame(t, acme, s.For("globex"))
	assert.False(t, acme.Open(), "built closed: nothing opens until the tenant's switch is applied")
	_, err := acme.CheckAndMark(ctx, "e1")
	require.ErrorIs(t, err, ErrDisabled, "a store not yet applied answers as a disabled one, the reload-window case")
	assert.NoDirExists(t, dir("acme"))

	require.NoError(t, acme.Apply(true))
	assert.DirExists(t, dir("acme"))
	assert.NoDirExists(t, dir("globex"), "the tenant beside it is untouched")
}

func TestStores_TenantsDoNotShareSeenIDs(t *testing.T) {
	t.Parallel()
	s, _ := pebbleStores(t)
	ctx := context.Background()
	tenants := []tenant.ID{"acme", "globex"}
	for _, id := range tenants {
		require.NoError(t, s.For(id).Apply(true))
	}

	for _, id := range tenants {
		dup, err := s.For(id).CheckAndMark(ctx, "e1")
		require.NoError(t, err)
		assert.False(t, dup, "%s: the same event id is first seen in each tenant", id)
	}
	for _, id := range tenants {
		dup, err := s.For(id).CheckAndMark(ctx, "e1")
		require.NoError(t, err)
		assert.True(t, dup, "%s: and a duplicate within its own tenant", id)
	}
}

func TestStores_RetainClosesTheRestAndKeepsTheirData(t *testing.T) {
	t.Parallel()
	s, dir := pebbleStores(t)
	ctx := context.Background()
	acme, globex := s.For("acme"), s.For("globex")
	require.NoError(t, acme.Apply(true))
	require.NoError(t, globex.Apply(true))
	_, err := acme.CheckAndMark(ctx, "e1")
	require.NoError(t, err)

	require.NoError(t, s.Retain(func(id tenant.ID) bool { return id == "globex" }))
	assert.False(t, acme.Open(), "the tenant no longer served has its store closed")
	assert.True(t, globex.Open(), "the one still served is untouched")
	entries, err := os.ReadDir(dir("acme"))
	require.NoError(t, err, "the directory stays")
	assert.NotEmpty(t, entries, "with its files")

	// Naming the tenant again builds a fresh store over the same directory:
	// restoring the folder restores the seen ids.
	restored := s.For("acme")
	assert.NotSame(t, acme, restored, "the closed store was forgotten")
	require.NoError(t, restored.Apply(true))
	dup, err := restored.CheckAndMark(ctx, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "an id seen before the tenant was dropped is still seen")
}

func TestStores_StatsSumsTheOpenStores(t *testing.T) {
	t.Parallel()
	s, _ := pebbleStores(t)
	assert.Nil(t, s.Stats(), "no store open, no stats: the scraper skips the gauges")
	closed := s.For("initech")
	assert.Nil(t, s.Stats(), "a closed store adds nothing")

	require.NoError(t, s.For("acme").Apply(true))
	require.NoError(t, s.For("globex").Apply(true))
	want := map[string]int64{}
	for _, id := range []tenant.ID{"acme", "globex"} {
		for k, v := range s.For(id).Stats() {
			want[k] += v
		}
	}
	got := s.Stats()
	assert.Equal(t, want, got)
	assert.Contains(t, got, "pebble_wal_size")
	assert.Contains(t, got, "pebble_table_count")
	assert.Nil(t, closed.Stats())
}

func TestStores_CloseClosesEveryStore(t *testing.T) {
	t.Parallel()
	s, _ := pebbleStores(t)
	acme, globex := s.For("acme"), s.For("globex")
	require.NoError(t, acme.Apply(true))
	require.NoError(t, globex.Apply(true))

	require.NoError(t, s.Close())
	assert.False(t, acme.Open())
	assert.False(t, globex.Open())
	assert.Nil(t, s.Stats())
	require.NoError(t, s.Close(), "closing again is a no-op")
}
