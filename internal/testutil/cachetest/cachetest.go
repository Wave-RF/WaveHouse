// Package cachetest is the conformance suite every cache.Cache backend runs:
// what a hit, a miss and an invalidation mean, independent of where the
// entries and versions live. A backend's own tests call Run with a factory.
package cachetest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Options describes what a backend can do beyond the Cache contract.
type Options struct {
	// MaxValueBytes is the largest value the backend keeps; 0 skips the
	// oversize case.
	MaxValueBytes int

	// NewPair returns two instances over one shared store, as two processes
	// see it; nil skips the cross-instance cases.
	NewPair func(t *testing.T) (a, b cache.Cache)
}

// Run runs the suite, each case on a fresh cache from newCache.
func Run(t *testing.T, newCache func(t *testing.T) cache.Cache, opts Options) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, c cache.Cache)
	}{
		{"miss", testMiss},
		{"set then hit", testSetThenHit},
		{"overwrite", testOverwrite},
		{"ttl expiry", testTTLExpiry},
		{"non-positive ttl stores nothing", testNonPositiveTTL},
		{"zero snapshot stores nothing", testZeroSnapshot},
		{"deps order does not matter", testDepsOrder},
		{"deps are part of the key", testDepsKeyed},
		{"tenant isolation", testTenantIsolation},
		{"foreign dependency refused", testForeignDependency},
		{"scope lattice", testScopeLattice},
		{"invalidate counts namespaces", testInvalidateCount},
		{"invalidate tenant orphans queries and pipes", testInvalidateTenant},
		{"bump during the query orphans the fill", testBumpDuringQuery},
		{"concurrent use", testConcurrent},
	}
	if opts.MaxValueBytes > 0 {
		cases = append(cases, struct {
			name string
			run  func(t *testing.T, c cache.Cache)
		}{"oversize value is not stored", func(t *testing.T, c cache.Cache) { testOversize(t, c, opts.MaxValueBytes) }})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newCache(t))
		})
	}
	if opts.NewPair != nil {
		t.Run("shared across instances", func(t *testing.T) {
			t.Parallel()
			a, b := opts.NewPair(t)
			testShared(t, a, b)
		})
	}
}

const ttl = time.Minute

var (
	acme   tenant.ID = "acme"
	globex tenant.ID = "globex"
)

func ns(id tenant.ID, table, scope string) cache.Namespace {
	return cache.Namespace{Tenant: id, Table: table, Scope: scope}
}

// settle waits out asynchronous admission, for a backend that has any.
func settle(c cache.Cache) {
	if w, ok := c.(interface{ Wait() }); ok {
		w.Wait()
	}
}

func lookup(t *testing.T, c cache.Cache, id tenant.ID, sha string, deps ...cache.Namespace) (cache.Entry, cache.Snapshot) {
	t.Helper()
	e, snap, err := c.Lookup(context.Background(), id, sha, deps)
	require.NoError(t, err)
	return e, snap
}

// fill stores value the way a handler does — Lookup, then Set under its
// snapshot — and checks it is then served.
func fill(t *testing.T, c cache.Cache, id tenant.ID, sha string, value string, deps ...cache.Namespace) {
	t.Helper()
	_, snap := lookup(t, c, id, sha, deps...)
	require.NoError(t, c.Set(context.Background(), snap, []byte(value), ttl))
	settle(c)
	requireHit(t, c, value, id, sha, deps...)
}

func requireHit(t *testing.T, c cache.Cache, want string, id tenant.ID, sha string, deps ...cache.Namespace) {
	t.Helper()
	e, _ := lookup(t, c, id, sha, deps...)
	require.Equal(t, want, string(e.Value), "%s %s %v", id, sha, deps)
}

func assertMiss(t *testing.T, c cache.Cache, id tenant.ID, sha string, deps ...cache.Namespace) {
	t.Helper()
	e, _ := lookup(t, c, id, sha, deps...)
	assert.Nil(t, e.Value, "%s %s %v: want a miss", id, sha, deps)
	assert.Zero(t, e.TTL)
}

func invalidate(t *testing.T, c cache.Cache, nss ...cache.Namespace) {
	t.Helper()
	_, err := c.Invalidate(context.Background(), nss)
	require.NoError(t, err)
}

func testMiss(t *testing.T, c cache.Cache) {
	assertMiss(t, c, acme, "q", ns(acme, "events", ""))
	assertMiss(t, c, acme, "q")
}

func testSetThenHit(t *testing.T, c cache.Cache) {
	deps := []cache.Namespace{ns(acme, "events", "org_1")}
	fill(t, c, acme, "q", "rows", deps...)
	e, _ := lookup(t, c, acme, "q", deps...)
	assert.Positive(t, e.TTL)
	assert.LessOrEqual(t, e.TTL, ttl)
}

func testOverwrite(t *testing.T, c cache.Cache) {
	deps := []cache.Namespace{ns(acme, "events", "")}
	fill(t, c, acme, "q", "v1", deps...)
	fill(t, c, acme, "q", "v2", deps...)
}

func testTTLExpiry(t *testing.T, c cache.Cache) {
	deps := []cache.Namespace{ns(acme, "events", "")}
	_, snap := lookup(t, c, acme, "q", deps...)
	require.NoError(t, c.Set(context.Background(), snap, []byte("rows"), time.Second))
	settle(c)
	requireHit(t, c, "rows", acme, "q", deps...)
	require.Eventually(t, func() bool {
		e, _ := lookup(t, c, acme, "q", deps...)
		return e.Value == nil
	}, 5*time.Second, 50*time.Millisecond)
}

func testNonPositiveTTL(t *testing.T, c cache.Cache) {
	for i, d := range []time.Duration{0, -time.Second} {
		sha := fmt.Sprintf("q%d", i)
		_, snap := lookup(t, c, acme, sha)
		require.NoError(t, c.Set(context.Background(), snap, []byte("rows"), d))
		settle(c)
		assertMiss(t, c, acme, sha)
	}
}

func testZeroSnapshot(t *testing.T, c cache.Cache) {
	require.NoError(t, c.Set(context.Background(), cache.Snapshot{}, []byte("rows"), ttl))
	settle(c)
	assertMiss(t, c, acme, "")
}

func testDepsOrder(t *testing.T, c cache.Cache) {
	a, b := ns(acme, "events", ""), ns(acme, "orders", "org_1")
	fill(t, c, acme, "q", "rows", a, b)
	requireHit(t, c, "rows", acme, "q", b, a)
}

func testDepsKeyed(t *testing.T, c cache.Cache) {
	a, b := ns(acme, "events", ""), ns(acme, "orders", "")
	fill(t, c, acme, "q", "rows", a)
	assertMiss(t, c, acme, "q", a, b)
	assertMiss(t, c, acme, "q", b)
	assertMiss(t, c, acme, "q")
}

// The same sha and table under two tenants are two entries, and a bump
// under one — scoped or whole-table — leaves the other's in place.
func testTenantIsolation(t *testing.T, c cache.Cache) {
	fill(t, c, acme, "q", "acme rows", ns(acme, "events", "org_1"))
	fill(t, c, globex, "q", "globex rows", ns(globex, "events", "org_1"))
	fill(t, c, acme, "pipe", "acme pipe")
	fill(t, c, globex, "pipe", "globex pipe")

	invalidate(t, c, ns(acme, "events", "org_1"))
	assertMiss(t, c, acme, "q", ns(acme, "events", "org_1"))
	requireHit(t, c, "globex rows", globex, "q", ns(globex, "events", "org_1"))

	invalidate(t, c, ns(acme, "events", ""))
	requireHit(t, c, "globex rows", globex, "q", ns(globex, "events", "org_1"))

	require.NoError(t, c.InvalidateTenant(context.Background(), acme))
	assertMiss(t, c, acme, "pipe")
	requireHit(t, c, "globex pipe", globex, "pipe")
}

// Every version an entry is filed under is its own tenant's, so a Lookup
// naming another tenant's namespace is refused, and its snapshot stores
// nothing.
func testForeignDependency(t *testing.T, c cache.Cache) {
	e, snap, err := c.Lookup(context.Background(), acme, "q", []cache.Namespace{ns(globex, "events", "")})
	require.ErrorIs(t, err, cache.ErrForeignDependency)
	assert.Nil(t, e.Value)
	require.NoError(t, c.Set(context.Background(), snap, []byte("rows"), ttl))
	settle(c)
	assertMiss(t, c, acme, "q", ns(acme, "events", ""))
	assertMiss(t, c, globex, "q", ns(globex, "events", ""))
}

// A scoped bump orphans that scope and the whole-table view; a scopeless
// (whole-table) bump orphans every scope of the table; neither reaches
// another table.
func testScopeLattice(t *testing.T, c cache.Cache) {
	org1, org2, whole, orders := ns(acme, "events", "org_1"), ns(acme, "events", "org_2"), ns(acme, "events", ""), ns(acme, "orders", "")
	for _, d := range []cache.Namespace{org1, org2, whole, orders} {
		fill(t, c, acme, "q", d.Table+"/"+d.Scope, d)
	}

	invalidate(t, c, org1)
	assertMiss(t, c, acme, "q", org1)
	assertMiss(t, c, acme, "q", whole)
	requireHit(t, c, "events/org_2", acme, "q", org2)
	requireHit(t, c, "orders/", acme, "q", orders)

	invalidate(t, c, whole)
	assertMiss(t, c, acme, "q", org2)
	requireHit(t, c, "orders/", acme, "q", orders)
}

func testInvalidateCount(t *testing.T, c cache.Cache) {
	n, err := c.Invalidate(context.Background(), []cache.Namespace{ns(acme, "events", ""), ns(globex, "events", "org_1")})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), n)
	n, err = c.Invalidate(context.Background(), nil)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// InvalidateTenant orphans every entry of the tenant — tables it never
// bumped, and results with no deps (pipes) — in one step, and entries
// filled after it are served again: a generation, not a lock.
func testInvalidateTenant(t *testing.T, c cache.Cache) {
	events, orders := ns(acme, "events", ""), ns(acme, "orders", "org_1")
	fill(t, c, acme, "q", "events", events)
	fill(t, c, acme, "q", "orders", orders)
	fill(t, c, acme, "pipe", "pipe")
	fill(t, c, globex, "pipe", "globex pipe")

	require.NoError(t, c.InvalidateTenant(context.Background(), acme))
	assertMiss(t, c, acme, "q", events)
	assertMiss(t, c, acme, "q", orders)
	assertMiss(t, c, acme, "pipe")
	requireHit(t, c, "globex pipe", globex, "pipe")

	fill(t, c, acme, "q", "events again", events)
	fill(t, c, acme, "pipe", "pipe again")
}

// #382: a fill is filed under the versions read before its query ran, so a
// bump that lands while the query runs orphans it instead of re-homing the
// pre-write rows under the post-bump versions.
func testBumpDuringQuery(t *testing.T, c cache.Cache) {
	ctx := context.Background()
	bumps := []struct {
		name string
		deps []cache.Namespace
		bump func()
	}{
		{"table", []cache.Namespace{ns(acme, "events", "")}, func() { invalidate(t, c, ns(acme, "events", "")) }},
		{"scope", []cache.Namespace{ns(acme, "events", "org_1")}, func() { invalidate(t, c, ns(acme, "events", "org_1")) }},
		{"tenant", nil, func() { require.NoError(t, c.InvalidateTenant(ctx, acme)) }},
	}
	for _, b := range bumps {
		sha := "q/" + b.name
		_, snap := lookup(t, c, acme, sha, b.deps...)
		b.bump()
		require.NoError(t, c.Set(ctx, snap, []byte("pre-write rows"), ttl))
		settle(c)
		assertMiss(t, c, acme, sha, b.deps...)
	}
}

func testOversize(t *testing.T, c cache.Cache, maxValue int) {
	_, snap := lookup(t, c, acme, "big")
	require.NoError(t, c.Set(context.Background(), snap, make([]byte, maxValue+1), ttl))
	settle(c)
	assertMiss(t, c, acme, "big")
}

// Lookups, fills and bumps from many goroutines at once, for -race; the
// last bump still orphans whatever was filled before it.
func testConcurrent(t *testing.T, c cache.Cache) {
	ctx := context.Background()
	deps := []cache.Namespace{ns(acme, "events", "")}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 50 {
				_, snap, err := c.Lookup(ctx, acme, "q", deps)
				assert.NoError(t, err)
				assert.NoError(t, c.Set(ctx, snap, []byte("rows"), ttl))
				switch (i + j) % 10 {
				case 0:
					_, err = c.Invalidate(ctx, deps)
					assert.NoError(t, err)
				case 5:
					assert.NoError(t, c.InvalidateTenant(ctx, acme))
				}
			}
		})
	}
	wg.Wait()
	settle(c)
	invalidate(t, c, deps...)
	assertMiss(t, c, acme, "q", deps...)
}

// Two instances over one store are one cache: a fill on one is served by the
// other, and a bump on either orphans it for both.
func testShared(t *testing.T, a, b cache.Cache) {
	deps := []cache.Namespace{ns(acme, "events", "")}
	fill(t, a, acme, "q", "rows", deps...)
	requireHit(t, b, "rows", acme, "q", deps...)
	invalidate(t, b, deps...)
	assertMiss(t, a, acme, "q", deps...)

	fill(t, a, acme, "pipe", "pipe")
	require.NoError(t, b.InvalidateTenant(context.Background(), acme))
	assertMiss(t, a, acme, "pipe")

	_, snap := lookup(t, a, acme, "q", deps...)
	invalidate(t, b, deps...)
	require.NoError(t, a.Set(context.Background(), snap, []byte("pre-write rows"), ttl))
	settle(a)
	assertMiss(t, b, acme, "q", deps...)
}
