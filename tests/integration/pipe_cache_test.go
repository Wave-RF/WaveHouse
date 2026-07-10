//go:build integration

package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// newPipeHandler wires a one-pipe store and a fresh LocalCache into a PipesHandler
// against the shared ClickHouse. The cache gets the registry's CURRENT base→view
// cascade installed (normally pushed by main on each refresh) — a pipe folds a
// view's own namespace, so a write to the view's base table only evicts it via
// this cascade; tests that create views must Refresh before calling this.
func newPipeHandler(t *testing.T, e *testEnv, name, sql string, roles ...string) (*api.PipesHandler, *cache.LocalCache) {
	t.Helper()
	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })
	localCache.SetDependents(e.registry.Dependents())

	store := pipes.NewMemoryStore(&pipes.NamedQuery{Name: name, SQL: sql, AllowedRoles: roles})
	h := api.NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), e.chConn, localCache, e.registry, 30*time.Second, testutil.NopLogger())
	return h, localCache
}

// execPipe drives GET /v1/pipes/{name}/execute as role, requires 200, and waits
// out ristretto's async Set so a following call observes the cached entry.
func execPipe(t *testing.T, h *api.PipesHandler, lc *cache.LocalCache, name, role string) *httptest.ResponseRecorder {
	t.Helper()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", name)
	c := context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
	c = auth.WithRole(c, role)
	r := httptest.NewRequestWithContext(c, http.MethodGet, "/v1/pipes/"+name+"/execute", nil)
	w := httptest.NewRecorder()
	h.Execute(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	lc.Wait()
	return w
}

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

	h, localCache := newPipeHandler(t, e, "agg",
		fmt.Sprintf("SELECT sum(user_id) AS s FROM `%s`.`%s`", testCHDatabase, view),
		"viewer", "editor")
	exec := func(role string) *httptest.ResponseRecorder { return execPipe(t, h, localCache, "agg", role) }

	// First call misses (folds the view's own namespace as the dependency), then hits.
	w := exec("viewer")
	require.Equal(t, "MISS", w.Header().Get("X-Cache"))
	require.Equal(t, `[{"s":1770}]`, w.Body.String()) // sum(0..59)
	require.Equal(t, "HIT", exec("viewer").Header().Get("X-Cache"))

	// A different allowed role shares the same cached result (pipes aren't per-role).
	require.Equal(t, "HIT", exec("editor").Header().Get("X-Cache"))

	// A write to the BASE table cascades to the view (registry base->view edge) and
	// invalidates the view-keyed cached result.
	_, err := localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(base)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec("viewer").Header().Get("X-Cache"))
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

	// Pin the ClickHouse-version coupling directly before the cache-level assertions:
	// parseExplainTables recognizes a dead UNION arm by ClickHouse's LITERAL rendering
	// of a false-folded WHERE ("constant_value: UInt64_0,"). If an upgrade changes
	// that rendering, pruning silently stops firing — failing SAFE (the arm's tables
	// stay tracked, over-resolve, never a wrong prune) but losing the precision
	// optimization and its TTL-cap signal with nothing but a flat
	// wavehouse_pipe_dep_tables_pruned_total metric. Assert the prune on the bound
	// SQL (what Execute resolves) so a rendering change surfaces in CI with a direct
	// diagnostic, not an obscure eviction failure further down.
	boundSQL := strings.ReplaceAll(sql, "{{source:web}}", "'web'")
	res, err := discovery.ResolveTables(ctx, e.chConn, testCHDatabase, boundSQL)
	require.NoError(t, err)
	require.Contains(t, res.Tables, web, "the live arm's table stays tracked")
	require.NotContains(t, res.Tables, mobile, "the constant-false arm's table must be pruned")
	require.True(t, res.Pruned, "ResolveTables must report the prune (it drives the TTL cap)")

	h, localCache := newPipeHandler(t, e, "u", sql, "viewer")
	exec := func() *httptest.ResponseRecorder { return execPipe(t, h, localCache, "u", "viewer") }

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

	h, localCache := newPipeHandler(t, e, "mvpipe",
		fmt.Sprintf("SELECT sumMerge(s) AS s FROM `%s`.`%s`", testCHDatabase, mv),
		"viewer")
	exec := func() *httptest.ResponseRecorder { return execPipe(t, h, localCache, "mvpipe", "viewer") }

	require.Equal(t, "MISS", exec().Header().Get("X-Cache")) // folds the MV's namespace; cascade maps source -> MV
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// A write to the SOURCE (what ingest bumps) must evict the MV-reading pipe.
	_, err := localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(src)}})
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
// The pipe is a count() on purpose: ClickHouse answers it from metadata (reading no
// data parts), which a runtime signal like system.query_log would drop — the
// pre-optimization EXPLAIN QUERY TREE keeps the table, so the write still evicts.
func TestPipeCacheInvalidation_WeirdIdentifier(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// Arbitrary, regex-hostile name: embedded dots so it reads as `a.b.c`. Legal in a
	// quoted ClickHouse identifier; createRawTable quotes it correctly and seeds one row.
	rawName := fmt.Sprintf("it_weird.dep.name_%d", tableCounter.Add(1))
	table := createRawTable(t, rawName, map[string]string{"id": "row1"})

	h, localCache := newPipeHandler(t, e, "weird",
		fmt.Sprintf("SELECT count() AS c FROM %s.%s", chQuoteIdent(testCHDatabase), chQuoteIdent(table)),
		"viewer")
	exec := func() *httptest.ResponseRecorder { return execPipe(t, h, localCache, "weird", "viewer") }

	// First call resolves the weird-named table via EXPLAIN and folds its namespace
	// into the key; second call hits.
	w := exec()
	require.Equal(t, "MISS", w.Header().Get("X-Cache"))
	require.Equal(t, `[{"c":1}]`, w.Body.String())
	require.Equal(t, "HIT", exec().Header().Get("X-Cache"))

	// A write to the weird-named table, encoded exactly as the ingest worker encodes
	// it, must evict the cached result — proving the resolved dependency namespace
	// equals SafeEncodeNATS(rawName) despite the dots.
	_, err := localCache.Invalidate(ctx, []cache.Namespace{{Table: chsql.SafeEncodeNATS(table)}})
	require.NoError(t, err)
	require.Equal(t, "MISS", exec().Header().Get("X-Cache"))
}

// TestPipeDeps_ExternalReadsAreDetected pins external-read detection against the
// shipped ClickHouse: a table function renders as its own TABLE_FUNCTION node in
// EXPLAIN QUERY TREE, flagged External so the handler TTL-caps the result (a
// write can never evict data that lives outside ClickHouse's tables). Like the
// pruning pin in TestPipeCacheInvalidation_UnionDeadBranchPruned, this surfaces a
// rendering change on a ClickHouse upgrade.
func TestPipeDeps_ExternalReadsAreDetected(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	local := createTable(t, "x UInt64", "ORDER BY x")

	res, err := discovery.ResolveTables(ctx, e.chConn, testCHDatabase,
		fmt.Sprintf("SELECT t.x FROM `%s`.`%s` t JOIN numbers(10) n ON t.x = n.number", testCHDatabase, local))
	require.NoError(t, err)
	require.Contains(t, res.Tables, local, "the local table stays tracked")
	require.True(t, res.External, "a table function must be flagged external")

	res, err = discovery.ResolveTables(ctx, e.chConn, testCHDatabase,
		fmt.Sprintf("SELECT x FROM `%s`.`%s`", testCHDatabase, local))
	require.NoError(t, err)
	require.False(t, res.External, "a plain local read must not be flagged external")
}
