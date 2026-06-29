//go:build integration

package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// TestPipeCacheInvalidation drives the full pipe cache path against the shared
// ClickHouse: a pipe reading A NORMAL VIEW, caching, and write-driven invalidation
// via the cascade. The pipe folds the VIEW's own namespace; the registry maps the
// view to its base table at refresh (the view carries no dependencies_table edge,
// so this must come from parsing the view's own as_select), and installs a base->
// view cascade so a write to the base evicts the view-keyed entry. Pipes run the
// author SQL as-is (no row security), so any allowed role shares one cached result.
func TestPipeCacheInvalidation(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// Base table + a normal view over it. The pipe reads the VIEW; resolution
	// flattens it through the tree (built from the view's parsed definition) down
	// to the base table the registry knows, so a write to the base invalidates.
	base := createTable(t, "user_id UInt64, org_id UInt64", "ORDER BY user_id")
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO `%s` SELECT number, number%%3 FROM numbers(60)", base)))

	view := base + "_v"
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("CREATE VIEW `%s` AS SELECT user_id, org_id FROM `%s`", view, base)))
	t.Cleanup(func() { _ = e.chConn.Exec(context.Background(), "DROP VIEW IF EXISTS `"+view+"`") })

	// The view->base edge is discovered at refresh (createTable refreshed before
	// the view existed), so refresh again now that the view is created.
	require.NoError(t, e.registry.Refresh(ctx))

	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })
	// Install the registry's view->source cascade (normally pushed by main on each
	// refresh): a pipe folds a VIEW's own namespace, so a write to the view's base
	// table only evicts it via this cascade. The registry was refreshed above.
	localCache.SetDependents(e.registry.Dependents())

	store := pipes.NewMemoryStore(&pipes.NamedQuery{
		Name: "agg", SQL: fmt.Sprintf("SELECT sum(user_id) AS s FROM `%s`.`%s`", testCHDatabase, view),
		AllowedRoles: []string{"viewer", "editor"},
	})
	h := api.NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), e.chConn, localCache, 30*time.Second, testutil.NopLogger())
	h.Registry = e.registry

	exec := func(role string) *httptest.ResponseRecorder {
		t.Helper()
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("name", "agg")
		c := context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
		c = auth.WithRole(c, role)
		r := httptest.NewRequestWithContext(c, http.MethodGet, "/v1/pipes/agg/execute", nil)
		w := httptest.NewRecorder()
		h.Execute(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		localCache.Wait() // ristretto Set is async
		return w
	}

	// First call misses (folds the view's own namespace as the dependency), then hits.
	w := exec("viewer")
	require.Equal(t, "MISS", w.Header().Get("X-Cache"))
	require.Equal(t, `[{"s":1770}]`, w.Body.String()) // sum(0..59)
	require.Equal(t, "HIT", exec("viewer").Header().Get("X-Cache"))

	// A different allowed role shares the same cached result (pipes aren't per-role).
	require.Equal(t, "HIT", exec("editor").Header().Get("X-Cache"))

	// A write to the BASE table cascades to the view (registry base->view edge) and
	// invalidates the view-keyed cached result.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(base)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec("viewer").Header().Get("X-Cache"))
}

// TestPipeCacheInvalidation_TrivialCount proves a metadata-only count() still
// invalidates. ClickHouse answers SELECT count() FROM t from metadata (reading no
// data parts), which once defeated execution-time query_log resolution — but the
// definition-time static parse sees `FROM t` in the SQL regardless, so the table
// is a dependency and an ingest into it evicts the cached count. No EXPLAIN needed.
func TestPipeCacheInvalidation_TrivialCount(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	base := createTable(t, "user_id UInt64", "ORDER BY user_id")
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO `%s` SELECT number FROM numbers(60)", base)))

	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })
	// Install the registry's view->source cascade (normally pushed by main on each
	// refresh): a pipe folds a VIEW's own namespace, so a write to the view's base
	// table only evicts it via this cascade. The registry was refreshed above.
	localCache.SetDependents(e.registry.Dependents())

	store := pipes.NewMemoryStore(&pipes.NamedQuery{
		Name: "cnt", SQL: fmt.Sprintf("SELECT count() AS c FROM `%s`.`%s`", testCHDatabase, base),
		AllowedRoles: []string{"viewer"},
	})
	h := api.NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), e.chConn, localCache, 30*time.Second, testutil.NopLogger())
	h.Registry = e.registry

	exec := func() *httptest.ResponseRecorder {
		t.Helper()
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("name", "cnt")
		c := context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
		c = auth.WithRole(c, "viewer")
		r := httptest.NewRequestWithContext(c, http.MethodGet, "/v1/pipes/cnt/execute", nil)
		w := httptest.NewRecorder()
		h.Execute(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		localCache.Wait()
		return w
	}

	w := exec()
	require.Equal(t, "MISS", w.Header().Get("X-Cache"))
	require.Equal(t, `[{"c":60}]`, w.Body.String())
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// The static parse captured the counted table as a dependency, so this write
	// invalidates the cached result even though the count read no data parts.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(base)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec().Header().Get("X-Cache"))
}

// TestPipeCacheInvalidation_UnionDeadBranchPruned proves the per-binding dead-branch
// pruning: on a parameter-gated UNION, ClickHouse folds the bound predicate so the
// arm that can't match ({{source}}='web' makes the 'mobile' arm 'web'='mobile')
// resolves to a constant-false WHERE. Resolution drops that arm's table, so with
// source='web' the result depends ONLY on `web` — a write to the dead-branch `mobile`
// does NOT evict it, while a write to `web` does. This is precise, not stale: the
// pruned arm reads nothing regardless of `mobile`'s data, so it's a true non-dependency.
func TestPipeCacheInvalidation_UnionDeadBranchPruned(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	web := createTable(t, "user_id UInt64", "ORDER BY user_id")
	mobile := createTable(t, "user_id UInt64", "ORDER BY user_id")
	for _, tbl := range []string{web, mobile} {
		require.NoError(t, e.chConn.Exec(ctx,
			fmt.Sprintf("INSERT INTO `%s` SELECT number FROM numbers(60)", tbl)))
	}

	// A parameter-gated UNION; {{source}} defaults to 'web', so the 'mobile' arm's
	// WHERE folds to constant-false and `mobile` is pruned from the dependency set.
	sql := fmt.Sprintf(
		"SELECT sum(user_id) AS s FROM ("+
			"SELECT user_id FROM `%s`.`%s` WHERE {{source:web}} = 'web' "+
			"UNION ALL "+
			"SELECT user_id FROM `%s`.`%s` WHERE {{source:web}} = 'mobile')",
		testCHDatabase, web, testCHDatabase, mobile)

	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })
	localCache.SetDependents(e.registry.Dependents())

	store := pipes.NewMemoryStore(&pipes.NamedQuery{Name: "u", SQL: sql, AllowedRoles: []string{"viewer"}})
	h := api.NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), e.chConn, localCache, 30*time.Second, testutil.NopLogger())
	h.Registry = e.registry

	exec := func() *httptest.ResponseRecorder {
		t.Helper()
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("name", "u")
		c := context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
		c = auth.WithRole(c, "viewer")
		r := httptest.NewRequestWithContext(c, http.MethodGet, "/v1/pipes/u/execute", nil)
		w := httptest.NewRecorder()
		h.Execute(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		localCache.Wait()
		return w
	}

	require.Equal(t, "MISS", exec().Header().Get("X-Cache")) // deps pruned to [web]
	require.Equal(t, `[{"s":1770}]`, exec().Body.String())   // web's sum(0..59), a HIT
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// A write to the pruned dead-branch table `mobile` does NOT evict — it is a true
	// non-dependency for source='web' (precise resolution, not over-resolution).
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(mobile)}})
	require.NoError(t, err)
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// A write to the live-branch table `web` DOES evict.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(web)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec().Header().Get("X-Cache"))
}

// TestPipeCacheInvalidation_MaterializedViewSource proves a pipe reading a
// materialized view invalidates on writes to the MV's SOURCE table. The pipe folds
// the MV's own namespace, which nothing writes directly (ingest writes the source;
// ClickHouse maintains the MV from it), so without the cascade such a pipe would be
// silently stale until its TTL. The registry maps source->MV via
// system.tables.dependencies_table and installs it as a cascade, so a source write
// fans out to evict the MV-keyed cached result.
func TestPipeCacheInvalidation_MaterializedViewSource(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// Source table (registry-known, ingestable). The registry refresh happens here,
	// BEFORE the MV is created, so the MV is deliberately NOT a registry table — the
	// realistic case the source mapping must handle.
	src := createTable(t, "id UInt64, v UInt64", "ORDER BY id")
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO `%s` SELECT number, number FROM numbers(100)", src)))

	mv := src + "_mv"
	require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf(
		"CREATE MATERIALIZED VIEW `%s` ENGINE=AggregatingMergeTree ORDER BY id AS SELECT id, sumState(v) AS s FROM `%s` GROUP BY id",
		mv, src)))
	t.Cleanup(func() { _ = e.chConn.Exec(context.Background(), "DROP VIEW IF EXISTS `"+mv+"`") })

	// The materialized-view -> source map is built during schema discovery, so refresh
	// AFTER creating the view (createTable refreshed before it existed).
	require.NoError(t, e.registry.Refresh(ctx))

	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })
	// Install the registry's view->source cascade (normally pushed by main on each
	// refresh): a pipe folds a VIEW's own namespace, so a write to the view's base
	// table only evicts it via this cascade. The registry was refreshed above.
	localCache.SetDependents(e.registry.Dependents())

	store := pipes.NewMemoryStore(&pipes.NamedQuery{
		Name: "mvpipe", SQL: fmt.Sprintf("SELECT sumMerge(s) AS s FROM `%s`.`%s`", testCHDatabase, mv),
		AllowedRoles: []string{"viewer"},
	})
	h := api.NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), e.chConn, localCache, 30*time.Second, testutil.NopLogger())
	h.Registry = e.registry

	exec := func() *httptest.ResponseRecorder {
		t.Helper()
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("name", "mvpipe")
		c := context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
		c = auth.WithRole(c, "viewer")
		r := httptest.NewRequestWithContext(c, http.MethodGet, "/v1/pipes/mvpipe/execute", nil)
		w := httptest.NewRecorder()
		h.Execute(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		localCache.Wait()
		return w
	}

	require.Equal(t, "MISS", exec().Header().Get("X-Cache")) // folds the MV's namespace; cascade maps source -> MV
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// A write to the SOURCE (what ingest bumps) must evict the MV-reading pipe.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(src)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec().Header().Get("X-Cache"))
}

// TestPipeCacheInvalidation_WeirdIdentifier proves the pipe dependency-invalidation
// path operates on a table whose name a safe-identifier allowlist would reject — here
// an embedded-dot name that also LOOKS like a qualified `db.table` reference, the most
// load-bearing weird case for a feature that parses `db.table` out of EXPLAIN. It is
// the pipe-path counterpart to the query builder's identifier_roundtrip_test.go: the
// round-trip under test is that the dependency name EXPLAIN QUERY TREE reports, once
// SafeEncodeNATS-encoded, equals the namespace an ingest write to that table bumps —
// otherwise a write would silently fail to evict. createRawTable is required because
// createTable sanitizes the dots away; dotted-name resolution itself is proven in
// internal/discovery/fuzz_deps_test.go, and this carries it through the full Execute.
func TestPipeCacheInvalidation_WeirdIdentifier(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// Arbitrary, regex-hostile name: embedded dots so it reads as `a.b.c`. Legal in a
	// quoted ClickHouse identifier; createRawTable quotes it correctly and seeds one row.
	rawName := fmt.Sprintf("it_weird.dep.name_%d", tableCounter.Add(1))
	table := createRawTable(t, rawName, map[string]string{"id": "row1"})

	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })
	localCache.SetDependents(e.registry.Dependents())

	store := pipes.NewMemoryStore(&pipes.NamedQuery{
		Name:         "weird",
		SQL:          fmt.Sprintf("SELECT count() AS c FROM %s.%s", chQuoteIdent(testCHDatabase), chQuoteIdent(table)),
		AllowedRoles: []string{"viewer"},
	})
	h := api.NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), e.chConn, localCache, 30*time.Second, testutil.NopLogger())
	h.Registry = e.registry

	exec := func() *httptest.ResponseRecorder {
		t.Helper()
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("name", "weird")
		c := context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
		c = auth.WithRole(c, "viewer")
		r := httptest.NewRequestWithContext(c, http.MethodGet, "/v1/pipes/weird/execute", nil)
		w := httptest.NewRecorder()
		h.Execute(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		localCache.Wait()
		return w
	}

	// First call resolves the weird-named table via EXPLAIN and folds its namespace
	// into the key; second call hits.
	w := exec()
	require.Equal(t, "MISS", w.Header().Get("X-Cache"))
	require.Equal(t, `[{"c":1}]`, w.Body.String())
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// A write to the weird-named table, encoded exactly as the ingest worker encodes
	// it, must evict the cached result — proving the resolved dependency namespace
	// equals SafeEncodeNATS(rawName) despite the dots.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(table)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec().Header().Get("X-Cache"))
}
