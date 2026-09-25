package dedupe

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// pebbleStores is a Stores over the embedded implementation in a temp
// data_dir, with the implementation itself.
func pebbleStores(t *testing.T) (*Stores, *Embedded) {
	t.Helper()
	e := NewEmbedded(t.TempDir())
	s := NewStores(e.Tenant)
	t.Cleanup(func() { _ = s.Close() })
	return s, e
}

func TestStores_ForBuildsOneClosedStorePerTenant(t *testing.T) {
	t.Parallel()
	s, e := pebbleStores(t)
	ctx := context.Background()

	acme := s.For("acme")
	assert.Same(t, acme, s.For("acme"), "one store per tenant, however often it is named")
	assert.NotSame(t, acme, s.For("globex"))
	assert.False(t, acme.Open(), "built closed: nothing opens until the tenant's switch is applied")
	_, err := mark(ctx, acme, "e1")
	require.ErrorIs(t, err, ErrDisabled, "a store not yet applied answers as a disabled one, the reload-window case")
	assert.NoDirExists(t, e.Dir())

	require.NoError(t, acme.Apply(true))
	assert.DirExists(t, e.Dir())
	assert.False(t, s.For("globex").Open(), "the tenant beside it is untouched")
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
		dup, err := mark(ctx, s.For(id), "e1")
		require.NoError(t, err)
		assert.False(t, dup, "%s: the same event id is first seen in each tenant", id)
	}
	for _, id := range tenants {
		dup, err := mark(ctx, s.For(id), "e1")
		require.NoError(t, err)
		assert.True(t, dup, "%s: and a duplicate within its own tenant", id)
	}
}

func TestStores_RetainClosesTheRestAndKeepsTheirData(t *testing.T) {
	t.Parallel()
	s, _ := pebbleStores(t)
	ctx := context.Background()
	acme, globex := s.For("acme"), s.For("globex")
	require.NoError(t, acme.Apply(true))
	require.NoError(t, globex.Apply(true))
	_, err := mark(ctx, acme, "e1")
	require.NoError(t, err)

	require.NoError(t, s.Retain(func(id tenant.ID) bool { return id == "globex" }))
	assert.False(t, acme.Open(), "the tenant no longer served has its store closed")
	assert.True(t, globex.Open(), "the one still served is untouched")

	// Naming the tenant again builds a fresh store over the same instance:
	// restoring the folder restores the seen ids.
	restored := s.For("acme")
	assert.NotSame(t, acme, restored, "the closed store was forgotten")
	require.NoError(t, restored.Apply(true))
	dup, err := mark(ctx, restored, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "an id seen before the tenant was dropped is still seen")
}

// gatedDedup is a backend whose Close blocks until released: one tenant's
// slow I/O, as the last Pebble close waiting on a compaction.
type gatedDedup struct{ entered, release chan struct{} }

func (g *gatedDedup) Reserve(context.Context, []Key, time.Duration) ([]Claim, error) {
	return nil, nil
}
func (g *gatedDedup) Commit(context.Context, []Claim, time.Duration) error { return nil }
func (g *gatedDedup) Release(context.Context, []Claim) error               { return nil }
func (g *gatedDedup) Close() error                                         { g.entered <- struct{}{}; <-g.release; return nil }

// One tenant's I/O is that tenant's wait alone: Retain edits the map under
// the lock and closes outside it, so a dropped tenant's slow close never
// stalls another tenant's lookup, which every dedupe-enabled record makes.
func TestStores_IOHappensOutsideTheLock(t *testing.T) {
	t.Parallel()
	g := &gatedDedup{entered: make(chan struct{}), release: make(chan struct{})}
	s := NewStores(func(tenant.ID) *Managed {
		return NewManaged(func() (Deduplicator, error) { return g, nil })
	})
	require.NoError(t, s.For("acme").Apply(true))
	done := make(chan error, 1)
	go func() { done <- s.Retain(func(tenant.ID) bool { return false }) }()

	<-g.entered
	got := make(chan *Managed, 1)
	go func() { got <- s.For("globex") }()
	select {
	case m := <-got:
		assert.NotNil(t, m)
	case <-time.After(2 * time.Second):
		t.Error("For waited behind another tenant's I/O")
	}
	close(g.release)
	require.NoError(t, <-done)
}

func TestStores_CloseClosesEveryStore(t *testing.T) {
	t.Parallel()
	s, e := pebbleStores(t)
	acme, globex := s.For("acme"), s.For("globex")
	require.NoError(t, acme.Apply(true))
	require.NoError(t, globex.Apply(true))

	require.NoError(t, s.Close())
	assert.False(t, acme.Open())
	assert.False(t, globex.Open())
	assert.False(t, e.Open(), "the instance closes with the last store")
	require.NoError(t, s.Close(), "closing again is a no-op")
}
