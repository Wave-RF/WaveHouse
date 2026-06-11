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

	h := api.NewPipesHandler(
		pipes.NewMemoryStore(),
		policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"}),
		e.chConn,
		localCache,
		30*time.Second,
		testutil.NopLogger(),
	)
	h.Registry = e.registry
	h.Database = testCHDatabase

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

// TestPipeDeps_DirectTableResolves covers the plain case: a pipe reading a base
// table directly resolves to exactly that table.
func TestPipeDeps_DirectTableResolves(t *testing.T) {
	e := env(t)

	base := createTable(t, "id String, n UInt64", "ORDER BY id")

	h := api.NewPipesHandler(
		pipes.NewMemoryStore(),
		policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"}),
		e.chConn,
		nil, // no cache needed; this test only checks resolution at Put()
		30*time.Second,
		testutil.NopLogger(),
	)
	h.Registry = e.registry
	h.Database = testCHDatabase

	putPipe(t, h, "direct", fmt.Sprintf("SELECT id, n FROM %s", base))

	saved := h.Store.Get("direct")
	require.NotNil(t, saved)
	assert.Equal(t, []string{base}, saved.ResolvedTables)
}
