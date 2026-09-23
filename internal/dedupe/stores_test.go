package dedupe

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// gatedDedup is a backend whose Close and Stats block until released: one
// tenant's slow I/O, as a Pebble close waiting on a compaction or an open
// replaying its log.
type gatedDedup struct{ entered, release chan struct{} }

func (g *gatedDedup) CheckAndMark(context.Context, string) (bool, error) { return false, nil }
func (g *gatedDedup) Stats() map[string]int64                            { g.wait(); return map[string]int64{"seen": 0} }
func (g *gatedDedup) Close() error                                       { g.wait(); return nil }
func (g *gatedDedup) wait()                                              { g.entered <- struct{}{}; <-g.release }

// One tenant's I/O is that tenant's wait alone: Retain edits the map under
// the lock and closes outside it, and Stats reads the stores outside it, so
// a dropped tenant's slow close or a scrape waiting on one store never
// stalls another tenant's lookup, which every dedupe-enabled record makes.
func TestStores_IOHappensOutsideTheLock(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*Stores, *gatedDedup) {
		t.Helper()
		g := &gatedDedup{entered: make(chan struct{}), release: make(chan struct{})}
		s := NewStores(func(tenant.ID) *Managed {
			return NewManaged(func() (Deduplicator, error) { return g, nil })
		})
		require.NoError(t, s.For("acme").Apply(true))
		return s, g
	}
	// forAnswers fails unless For answers while acme's gated call is in
	// progress, then releases it.
	forAnswers := func(t *testing.T, s *Stores, g *gatedDedup) {
		t.Helper()
		<-g.entered
		defer close(g.release)
		got := make(chan *Managed, 1)
		go func() { got <- s.For("globex") }()
		select {
		case m := <-got:
			assert.NotNil(t, m)
		case <-time.After(2 * time.Second):
			t.Fatal("For waited behind another tenant's I/O")
		}
	}
	t.Run("Retain", func(t *testing.T) {
		t.Parallel()
		s, g := setup(t)
		done := make(chan error, 1)
		go func() { done <- s.Retain(func(tenant.ID) bool { return false }) }()
		forAnswers(t, s, g)
		require.NoError(t, <-done)
	})
	t.Run("Stats", func(t *testing.T) {
		t.Parallel()
		s, g := setup(t)
		done := make(chan map[string]int64, 1)
		go func() { done <- s.Stats() }()
		forAnswers(t, s, g)
		assert.Equal(t, map[string]int64{"seen": 0}, <-done)
	})
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
