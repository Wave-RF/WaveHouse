package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// closedAddr returns a 127.0.0.1 address nothing listens on: bind an
// ephemeral port, then release it.
func closedAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// unloadedRegistry is a registry no refresh has ever succeeded on: a tenant
// whose ClickHouse has not answered yet, or which has no pool.
func unloadedRegistry() *discovery.SchemaRegistry {
	return discovery.NewSchemaRegistry(func() (driver.Conn, string) { return nil, "test" }, tenant.Default, func(tenant.ID) time.Duration { return time.Hour })
}

// assertUnavailable pins the 503 a tenant's ClickHouse side answers with: the
// message, the Retry-After hint, and the JSON error contract.
func assertUnavailable(t *testing.T, w *httptest.ResponseRecorder, message, retryAfter string) {
	t.Helper()
	require.Equal(t, http.StatusServiceUnavailable, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, retryAfter, w.Header().Get("Retry-After"))
	assert.Contains(t, w.Body.String(), message)
	testutil.AssertJSONErrorResponse(t, w)
}

// A table lookup before the tenant's first discovery is a 503 with
// Retry-After, not the 404 of a table the schema lacks: the table may well
// exist. Every route that answered 404 answers it, and the schema list too,
// where an empty list would read as "no tables". A tenant whose registry is
// not built yet — the beat after its adoption — reads the same way.
func TestClickHouseRoutes_SchemaNotLoadedIs503(t *testing.T) {
	t.Parallel()
	sources := map[string]RegistrySource{
		"unloaded registry":  fixedRegistry(unloadedRegistry()),
		"no registry yet":    fixedRegistry(nil),
		"no registry source": nil,
	}
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			schema := NewSchemaHandler(source)
			schema.Tenants = testTenants()
			structured := NewStructuredQueryHandler(nil, nil, source, nil, nil, noTimeout, nil)
			routes := map[string]func() (*httptest.ResponseRecorder, string){
				"ingest": func() (*httptest.ResponseRecorder, string) {
					w := httptest.NewRecorder()
					NewIngestHandler(source, &testutil.MockPublisher{}).Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/"})))
					return w, schemaNotLoadedMessage
				},
				"structured query": func() (*httptest.ResponseRecorder, string) {
					w := httptest.NewRecorder()
					structured.Handle(w, withTenant(structuredQueryRequest(t, "clicks", selectAllQuery())))
					return w, schemaNotLoadedMessage
				},
				"schema get": func() (*httptest.ResponseRecorder, string) {
					w := httptest.NewRecorder()
					schema.Get(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/ops/schema?table=clicks", nil))
					return w, schemaNotLoadedMessage
				},
				"schema list": func() (*httptest.ResponseRecorder, string) {
					w := httptest.NewRecorder()
					schema.Get(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/ops/schema", nil))
					return w, schemaNotLoadedMessage
				},
			}
			for route, call := range routes {
				w, message := call()
				assertUnavailable(t, w, message, retryAfterSchema)
				assert.NotContains(t, w.Body.String(), "unknown table", route)
			}
		})
	}
}

// selectAllQuery is the structured query a permissive role may run.
func selectAllQuery() query.StructuredQuery { return query.StructuredQuery{SelectAll: true} }

// A tenant on no pool — its tuple could not be opened, such as by the
// connection ceiling — fails closed on every route that reaches its
// ClickHouse: a 503 with Retry-After ahead of the cache, so nothing it
// cached before is served either, and on the refresh, which cannot run.
func TestClickHouseRoutes_NoPoolIs503(t *testing.T) {
	t.Parallel()
	reg := testRegistry(t)
	allowAll := staticPolicy(&policy.Policy{
		DefaultRole: "viewer",
		Tables:      map[string]policy.TablePolicy{"clicks": {"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}}},
	})
	noConn := func(*settings.Store) driver.Conn { return nil }

	t.Run("structured query", func(t *testing.T) {
		t.Parallel()
		h := NewStructuredQueryHandler(noConn, nil, fixedRegistry(reg), allowAll, nil, noTimeout, nil)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(structuredQueryRequest(t, "clicks", selectAllQuery())))
		assertUnavailable(t, w, noConnectionMessage, retryAfterPool)
	})
	t.Run("pipe execute", func(t *testing.T) {
		t.Parallel()
		h := NewPipesHandler(staticPipes(&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}}), allowAll, noConn, nil, noTimeout)
		w := httptest.NewRecorder()
		h.Execute(w, withTenant(pipesRequest(t, http.MethodGet, "/v1/pipes/top_pages", "top_pages", nil)))
		assertUnavailable(t, w, noConnectionMessage, retryAfterPool)
	})
	t.Run("raw-SQL proxy", func(t *testing.T) {
		t.Parallel()
		h := newTestQueryHandler(func(*settings.Store) chconn.Target { return chconn.Target{} }, noTimeout)
		body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
		w := postQuery(h, body)
		assertUnavailable(t, w, noConnectionMessage, retryAfterPool)
		assertSecurityHeaders(t, w)
	})
	t.Run("schema refresh", func(t *testing.T) {
		t.Parallel()
		h := NewSchemaHandler(fixedRegistry(unloadedRegistry()))
		h.Tenants = testTenants()
		w := httptest.NewRecorder()
		h.Refresh(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/schema/refresh", nil))
		assertUnavailable(t, w, noConnectionMessage, retryAfterPool)
	})
}

// The three ops routes that reach a tenant's ClickHouse name it in ?tenant=,
// parsed as strictly as the pipe reads: the store the getters receive is
// that tenant's, absent is the default tenant, a query that does not parse
// is a 400, and a tenant that cannot be served gets the tenant routes' 404
// or 503.
func TestClickHouseOpsRoutes_TenantParam(t *testing.T) {
	t.Parallel()
	tenants := nestedTenants(t, map[string]string{"0": fullConfig(100), "acme": fullConfig(100), "globex": `{"unknown_key": true}`})
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{{Name: "clicks", Columns: []discovery.Column{{Name: "page", Type: "String"}}}})
	tests := []struct {
		name, query string
		wantStatus  int
		wantTenant  tenant.ID // on 200, whose store the getters were handed
		wantBody    string
	}{
		{name: "no parameter is the default tenant", query: "", wantStatus: http.StatusOK, wantTenant: tenant.Default},
		{name: "named tenant", query: "tenant=acme", wantStatus: http.StatusOK, wantTenant: "acme"},
		{name: "explicit default tenant", query: "tenant=0", wantStatus: http.StatusOK, wantTenant: tenant.Default},
		{name: "rejected tenant", query: "tenant=globex", wantStatus: http.StatusServiceUnavailable, wantBody: "tenant settings are invalid"},
		{name: "unknown tenant", query: "tenant=initech", wantStatus: http.StatusNotFound, wantBody: "unknown tenant: initech"},
		{name: "empty value is not absent", query: "tenant=", wantStatus: http.StatusBadRequest, wantBody: "invalid ?tenant: tenant id is empty"},
		{name: "repeated, even agreeing", query: "tenant=acme&tenant=acme", wantStatus: http.StatusBadRequest, wantBody: "sent more than once"},
		{name: "malformed id", query: "tenant=a.b", wantStatus: http.StatusBadRequest, wantBody: "invalid ?tenant"},
		{name: "semicolon pair", query: "tenant=acme;x=1", wantStatus: http.StatusBadRequest, wantBody: "invalid query string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var handed []*settings.Store
			registry := func(s *settings.Store) *discovery.SchemaRegistry {
				handed = append(handed, s)
				return reg
			}
			target := func(s *settings.Store) chconn.Target {
				handed = append(handed, s)
				return chconn.Target{URL: "http://" + closedAddr(t)}
			}
			schema := NewSchemaHandler(registry)
			schema.Tenants = tenants
			proxy := NewQueryHandler(target, noTimeout)
			proxy.Tenants = tenants
			sql, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})

			routes := []struct {
				name string
				call func(w http.ResponseWriter, r *http.Request)
				req  *http.Request
				// ok is the status a served tenant answers: the registry
				// getter was handed its store, and the proxy reached its target.
				ok int
			}{
				{name: "schema get", call: schema.Get, req: httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/ops/schema?table=clicks", nil), ok: http.StatusOK},
				{name: "schema list", call: schema.List, req: httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/ops/schema", nil), ok: http.StatusOK},
				{name: "schema refresh", call: schema.Refresh, req: httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/schema/refresh", nil), ok: http.StatusOK},
				// The proxy's target is a closed port: a served tenant is the
				// 502 of an unreachable ClickHouse, past every tenant check.
				{name: "ops query", call: proxy.Handle, req: httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/query", bytes.NewReader(sql)), ok: http.StatusBadGateway},
			}
			for _, route := range routes {
				handed = nil
				if tt.query != "" {
					route.req.URL.RawQuery = strings.TrimSuffix(route.req.URL.RawQuery+"&"+tt.query, "&")
					if route.req.URL.RawQuery[0] == '&' {
						route.req.URL.RawQuery = route.req.URL.RawQuery[1:]
					}
				}
				w := httptest.NewRecorder()
				route.call(w, route.req)
				if tt.wantStatus != http.StatusOK {
					require.Equal(t, tt.wantStatus, w.Code, "%s: %s", route.name, w.Body.String())
					assert.Empty(t, handed, "%s: a refused request must not reach the getters", route.name)
					assert.Contains(t, w.Body.String(), tt.wantBody, route.name)
					testutil.AssertJSONErrorResponse(t, w)
					continue
				}
				require.Equal(t, route.ok, w.Code, "%s: %s", route.name, w.Body.String())
				want, ok := tenants.For(tt.wantTenant)
				require.True(t, ok)
				require.NotEmpty(t, handed, route.name)
				for _, got := range handed {
					assert.Same(t, want, got, route.name)
				}
			}
		})
	}
}

// The ClickHouse-side getters — the connection, the registry, the HTTP
// target and the query deadline — receive the request's own tenant store
// through the real router, on the routes that reach ClickHouse: two tenants
// alternating never hand one the other's.
func TestNewRouter_ClickHouseGettersReceiveTheRequestTenantsStore(t *testing.T) {
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(200)})
	var handed []*settings.Store
	record := func(s *settings.Store) { handed = append(handed, s) }
	reg := testRegistry(t)
	viewer := staticPolicy(&policy.Policy{
		DefaultRole: "viewer",
		Tables:      map[string]policy.TablePolicy{"clicks": {"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}}},
	})
	conn := func(s *settings.Store) driver.Conn { record(s); return &countingConn{} }
	registry := func(s *settings.Store) *discovery.SchemaRegistry { record(s); return reg }
	timeout := func(s *settings.Store) time.Duration { record(s); return time.Second }
	router := NewRouter(Dependencies{
		Tenants:         tenants,
		Ingest:          NewIngestHandler(registry, &testutil.MockPublisher{}),
		StructuredQuery: NewStructuredQueryHandler(conn, nil, registry, viewer, func(*settings.Store) int { return 60 }, timeout, nil),
		Pipes:           NewPipesHandler(staticPipes(&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}}), viewer, conn, nil, timeout),
		Query:           &QueryHandler{},
		SSE:             NewStreamHandler(stream.NewHub(nil, nil, nil), nil),
		Health:          &HealthHandler{},
		Version:         NewVersionHandler("test", "test", "test"),
		Schema:          schemaHandlerOver(reg, tenants),
		AuthMW:          func(next http.Handler) http.Handler { return next },
		PolicySource:    policy.Static(&policy.Policy{}),
	})

	routes := []struct {
		name, path, body string
		getters          int // store-keyed ClickHouse getters the route consults
	}{
		{name: "structured query", path: "/v1/query?table=clicks", body: `{"select_all": true}`, getters: 3},
		{name: "pipe execute", path: "/v1/pipes/top_pages", body: `{}`, getters: 2},
	}
	for _, route := range routes {
		for _, id := range []tenant.ID{"acme", "globex", "acme"} {
			t.Run(route.name+" as "+id.String(), func(t *testing.T) {
				want, ok := tenants.For(id)
				require.True(t, ok)
				handed = nil
				req := httptest.NewRequestWithContext(auth.WithRole(context.Background(), "viewer"), http.MethodPost, route.path, strings.NewReader(route.body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set(tenant.Header, id.String())
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)

				require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
				require.Len(t, handed, route.getters)
				for _, got := range handed {
					assert.Same(t, want, got)
				}
			})
		}
	}
}
