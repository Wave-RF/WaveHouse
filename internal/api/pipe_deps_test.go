package api

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pipeDepNames(deps []cache.Namespace) []string {
	out := make([]string, len(deps))
	for i, d := range deps {
		out[i], _ = chsql.SafeDecodeNATS(d.Table)
	}
	sort.Strings(out)
	return out
}

// resolveDeps asks ClickHouse (EXPLAIN QUERY TREE) which tables a pipe reads —
// mocked here via resolveTablesFn so the unit test needs no server. It covers the
// fold, the per-pipe cache (EXPLAIN runs once), the all-tables over-resolve fallback
// when ClickHouse can't analyze the query, and ClearResolvedDeps re-resolution.
func TestResolveDeps_Explain(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest(
		[]*discovery.TableSchema{{Name: "events"}, {Name: "users"}, {Name: "orders"}}, nil)
	lc, _ := cache.NewLocal(1 << 20)
	defer func() { _ = lc.Close() }()

	calls := 0
	h := &PipesHandler{Registry: reg, Cache: lc, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	h.resolveTablesFn = func(_ context.Context, sql string) ([]string, error) {
		calls++
		switch sql {
		case "SELECT * FROM events":
			return []string{"events"}, nil
		case "SELECT * FROM events JOIN users ON 1":
			return []string{"events", "users"}, nil
		case "INSERT INTO events VALUES (1)": // a write → EXPLAIN errors
			return nil, errors.New("syntax error")
		case "SELECT * FROM mystery": // resolves to a name the registry doesn't know
			return []string{"mystery_view"}, nil
		default:
			return []string{}, nil
		}
	}

	// resolves to the reported tables; all known → not TTL-floored
	deps, unresolved := h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, []string{"events"}, pipeDepNames(deps))
	assert.False(t, unresolved, "a known base table must not floor the TTL")

	// cached: a second call with the same bound query doesn't re-run EXPLAIN
	before := calls
	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, before, calls, "resolveTablesFn must be cached per bound query")

	// multi-table
	deps, _ = h.resolveDeps(ctx, "SELECT * FROM events JOIN users ON 1")
	assert.Equal(t, []string{"events", "users"}, pipeDepNames(deps))

	// fallback: an un-analyzable query over-resolves to ALL base tables (never stale,
	// and every base table is version-maintained, so it is NOT TTL-floored)
	deps, unresolved = h.resolveDeps(ctx, "INSERT INTO events VALUES (1)")
	assert.Equal(t, []string{"events", "orders", "users"}, pipeDepNames(deps))
	assert.False(t, unresolved, "the all-base-tables over-resolve must not floor the TTL")

	// a resolved dep the registry doesn't know (an unfoldable view) → TTL-floored
	deps, unresolved = h.resolveDeps(ctx, "SELECT * FROM mystery")
	assert.Equal(t, []string{"mystery_view"}, pipeDepNames(deps))
	assert.True(t, unresolved, "an unknown/unfoldable dep must floor the TTL")

	// ClearResolvedDeps forces re-resolution
	callsBefore := calls
	h.ClearResolvedDeps()
	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Greater(t, calls, callsBefore, "ClearResolvedDeps must force re-resolution")

	// no registry → tracking disabled (TTL-only), no deps
	hNil := &PipesHandler{Cache: lc, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	nilDeps, nilUnresolved := hNil.resolveDeps(ctx, "SELECT 1")
	assert.Empty(t, nilDeps)
	assert.False(t, nilUnresolved)
}

// End-to-end through the real LocalCache: a write to a resolved table evicts the
// pipe; an unrelated write does not.
func TestResolveDeps_Explain_Invalidation(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest(
		[]*discovery.TableSchema{{Name: "events"}, {Name: "users"}}, nil)
	lc, _ := cache.NewLocal(1 << 20)
	defer func() { _ = lc.Close() }()
	lc.SetDependents(reg.Dependents())
	h := &PipesHandler{Registry: reg, Cache: lc, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	h.resolveTablesFn = func(_ context.Context, _ string) ([]string, error) { return []string{"events"}, nil }

	ns := func(n string) cache.Namespace { return cache.Namespace{Table: chsql.SafeEncodeNATS(n)} }
	evicts := func(write string) bool {
		deps, _ := h.resolveDeps(ctx, "SELECT * FROM events")
		require.NoError(t, lc.Set(ctx, "sha", deps, []byte("v"), time.Hour))
		lc.Wait()
		_, _ = lc.Invalidate(ctx, []cache.Namespace{ns(write)})
		d, _, _ := lc.Get(ctx, "sha", deps)
		return d == nil
	}
	assert.True(t, evicts("events"), "write to the resolved table evicts")
	assert.False(t, evicts("users"), "unrelated write does not evict")
}
