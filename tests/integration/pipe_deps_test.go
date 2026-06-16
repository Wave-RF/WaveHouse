//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// withPipeName stamps the chi {name} route param and the admin role onto a
// request, mirroring how the router would dispatch to the pipes handler.
func withPipeName(r *http.Request, name string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", name)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	return r.WithContext(auth.WithRole(r.Context(), "admin"))
}

func putPipe(t *testing.T, h *api.PipesHandler, name, sql string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"sql": sql})
	r := withPipeName(httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/v1/pipes/"+name, bytes.NewReader(body)), name)
	w := httptest.NewRecorder()
	h.Put(w, r)
	require.Equal(t, http.StatusOK, w.Code, "put body: %s", w.Body.String())
}

// executePipe runs a pipe and returns its X-Cache header ("HIT"/"MISS").
func executePipe(t *testing.T, h *api.PipesHandler, name string) string {
	t.Helper()
	r := withPipeName(httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/pipes/"+name+"/execute", nil), name)
	w := httptest.NewRecorder()
	h.Execute(w, r)
	require.Equal(t, http.StatusOK, w.Code, "execute body: %s", w.Body.String())
	return w.Header().Get("X-Cache")
}

// newPipesHandler builds a PipesHandler wired to real ClickHouse and the shared
// schema registry with dependency resolution enabled (Registry + Database set).
// c may be nil for tests that only exercise Put()-time resolution.
func newPipesHandler(e *testEnv, c cache.Cache) *api.PipesHandler {
	h := api.NewPipesHandler(
		pipes.NewMemoryStore(),
		policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"}),
		e.chConn,
		c,
		30*time.Second,
		testutil.NopLogger(),
	)
	h.Registry = e.registry
	h.Database = testCHDatabase
	return h
}

// TestPipeDeps_ResolveThroughViewAndInvalidate is the gate for the EXPLAIN QUERY
// TREE resolver: a pipe that reads a *view* must still have its underlying base
// table recorded as a dependency (static SQL parsing could not do this), and a
// write to that base table must then invalidate the pipe's cached result.
func TestPipeDeps_ResolveThroughViewAndInvalidate(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	base := createTable(t, "id String, val Float64", "ORDER BY id")
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id, val) VALUES ('a', 1.5)", base)))

	// The pipe reads this view, never the base table directly — so only view→base
	// resolution can recover the real dependency. Deliberately NOT refreshing the
	// registry after creating it, so the view itself stays unknown to the registry
	// and is filtered out, leaving only the base table.
	view := base + "_v"
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("CREATE VIEW %s AS SELECT id, val FROM %s", view, base)))
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.chConn.Exec(dctx, "DROP VIEW IF EXISTS "+view)
	})

	// A cache we own, so we can both observe hits and simulate the ingest worker's
	// invalidation bump deterministically.
	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })

	h := newPipesHandler(e, localCache)

	// PUT runs resolution against real ClickHouse.
	putPipe(t, h, "via_view", fmt.Sprintf("SELECT id, val FROM %s", view))

	saved := h.Store.Get("via_view")
	require.NotNil(t, saved)
	assert.Contains(t, saved.ResolvedTables, base,
		"EXPLAIN QUERY TREE should resolve view %q to its base table %q; got %v", view, base, saved.ResolvedTables)

	// MISS, then HIT once cached.
	assert.Equal(t, "MISS", executePipe(t, h, "via_view"))
	localCache.Wait()
	assert.Equal(t, "HIT", executePipe(t, h, "via_view"))

	// Simulate ingest bumping the base table's version. Because the pipe was keyed
	// on the base table (resolved through the view), this must orphan the entry.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: query.SafeEncodeNATS(base)}})
	require.NoError(t, err)
	localCache.Wait()

	assert.Equal(t, "MISS", executePipe(t, h, "via_view"),
		"a write to the resolved base table should have invalidated the cached pipe result")
}

// TestPipeDeps_ResolveThroughRegistryKnownViewResolvesToBase is the regression guard
// for the bug where the resolver stopped at a registry-known view. The sibling test
// above deliberately leaves the view out of the registry; in production, schema
// auto-refresh eventually discovers it (the registry is built from system.columns,
// which lists views/MVs), and the resolver must STILL expand it to the base table —
// not treat the registry hit as terminal and key the pipe on the view name, which the
// ingest path never bumps (→ stale).
func TestPipeDeps_ResolveThroughRegistryKnownViewResolvesToBase(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	base := createTable(t, "id String, val Float64", "ORDER BY id")

	view := base + "_v"
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("CREATE VIEW %s AS SELECT id, val FROM %s", view, base)))
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.chConn.Exec(dctx, "DROP VIEW IF EXISTS "+view)
	})

	// The distinguishing step: refresh AFTER creating the view, so the registry now
	// knows the view — the production condition the sibling test avoids.
	require.NoError(t, e.registry.Refresh(ctx))
	require.NotNil(t, e.registry.Get(view),
		"precondition: the view must be registry-known for this regression to be meaningful")

	h := newPipesHandler(e, nil) // Put()-time resolution only

	putPipe(t, h, "via_known_view", fmt.Sprintf("SELECT id, val FROM %s", view))

	saved := h.Store.Get("via_known_view")
	require.NotNil(t, saved)
	assert.Equal(t, []string{base}, saved.ResolvedTables,
		"a pipe reading a registry-known view must resolve to exactly its base table %q "+
			"(view expanded, view name dropped); got %v", base, saved.ResolvedTables)
}

// TestPipeDeps_DirectTableResolves covers the plain case: a pipe reading a base
// table directly resolves to exactly that table.
func TestPipeDeps_DirectTableResolves(t *testing.T) {
	e := env(t)

	base := createTable(t, "id String, n UInt64", "ORDER BY id")

	h := newPipesHandler(e, nil) // no cache; this test only checks Put()-time resolution

	putPipe(t, h, "direct", fmt.Sprintf("SELECT id, n FROM %s", base))

	saved := h.Store.Get("direct")
	require.NotNil(t, saved)
	assert.Equal(t, []string{base}, saved.ResolvedTables)
}

// TestPipeDeps_ResolveThroughMaterializedViewAndInvalidate verifies that a pipe
// reading a materialized view resolves to the view's *source* ingest table — the one
// the ingest worker writes and version-bumps — not the MV's storage target, so an
// ingest invalidates the cached pipe result. Resolution expands the MV via
// system.tables.as_select; only src is in the registry, so recovering it proves the
// expansion saw through the MV to the source.
func TestPipeDeps_ResolveThroughMaterializedViewAndInvalidate(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// src is the ingested base table the registry knows and the worker can bump.
	src := createTable(t, "id String, val Float64", "ORDER BY id")

	// target holds the MV's materialized rows; the pipe reads the MV by name, which
	// physically reads target. Neither target nor the MV is refreshed into the
	// registry — so, exactly as in the plain-view test, only the source can survive
	// filtering, and recovering it proves resolution saw through the MV to src.
	target := src + "_mvtarget"
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s (id String, val Float64) ENGINE = MergeTree ORDER BY id", target)))
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.chConn.Exec(dctx, "DROP TABLE IF EXISTS "+target)
	})

	mv := src + "_mv"
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("CREATE MATERIALIZED VIEW %s TO %s AS SELECT id, val FROM %s", mv, target, src)))
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.chConn.Exec(dctx, "DROP VIEW IF EXISTS "+mv)
	})

	// Insert AFTER the MV exists so the row actually materializes into target (an MV
	// only captures inserts that happen after its creation).
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id, val) VALUES ('a', 1.5)", src)))

	localCache, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localCache.Close() })

	h := newPipesHandler(e, localCache)

	// PUT runs resolution against real ClickHouse.
	putPipe(t, h, "via_mv", fmt.Sprintf("SELECT id, val FROM %s", mv))

	saved := h.Store.Get("via_mv")
	require.NotNil(t, saved)
	assert.Contains(t, saved.ResolvedTables, src,
		"a pipe reading materialized view %q should resolve to its source ingest table %q "+
			"(the table the ingest worker bumps); got %v", mv, src, saved.ResolvedTables)

	// MISS, then HIT once cached.
	assert.Equal(t, "MISS", executePipe(t, h, "via_mv"))
	localCache.Wait()
	assert.Equal(t, "HIT", executePipe(t, h, "via_mv"))

	// Simulate the ingest worker bumping the source table's version. Because the pipe
	// was keyed on src (resolved through the MV), this must orphan the cached entry.
	_, err = localCache.Invalidate(ctx, []cache.Namespace{{Table: query.SafeEncodeNATS(src)}})
	require.NoError(t, err)
	localCache.Wait()

	assert.Equal(t, "MISS", executePipe(t, h, "via_mv"),
		"a write to the resolved source table should have invalidated the MV-backed pipe result")
}

// TestPipeDeps_UnionBranchTablesBothResolve verifies that a pipe reading more than one
// base table records ALL of them as dependencies — here two tables via a UNION — so a
// write to either invalidates the cached result. Parameters bind as values, so the
// gating WHEREs don't change which tables are read.
func TestPipeDeps_UnionBranchTablesBothResolve(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// Two ingested base tables the registry knows and the worker can bump.
	t1 := createTable(t, "id String, val Float64", "ORDER BY id")
	t2 := createTable(t, "id String, val Float64", "ORDER BY id")

	// "read t1 if flag else t2", expressed in pure SQL via parameter-gated WHEREs. flag is
	// a declared number param so DummyBind renders a bare 0, not a quoted string.
	sqlTmpl := fmt.Sprintf(
		"SELECT id, val FROM %s WHERE {{flag}} = 1 UNION ALL SELECT id, val FROM %s WHERE {{flag}} = 0",
		t1, t2)

	h := newPipesHandler(e, nil) // Put()-time resolution only; no cache needed

	body, _ := json.Marshal(map[string]any{
		"sql":        sqlTmpl,
		"parameters": []map[string]any{{"name": "flag", "type": "number", "required": true}},
	})
	r := withPipeName(httptest.NewRequestWithContext(ctx, http.MethodPut, "/v1/pipes/union_branch", bytes.NewReader(body)), "union_branch")
	w := httptest.NewRecorder()
	h.Put(w, r)
	require.Equal(t, http.StatusOK, w.Code, "put body: %s", w.Body.String())

	saved := h.Store.Get("union_branch")
	require.NotNil(t, saved)

	assert.Subset(t, saved.ResolvedTables, []string{t1, t2},
		"a pipe reading two tables via UNION should record BOTH (%s, %s) as deps; got %v",
		t1, t2, saved.ResolvedTables)
}
