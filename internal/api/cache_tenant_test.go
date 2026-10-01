package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/cache"
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

// gatedCH is a fakeCH that holds every query open until release is closed
// and reports each one on entered as it starts, so a test can hold requests
// in flight together and count the queries they became.
func gatedCH(entered, release chan struct{}) *fakeCH {
	return &fakeCH{answer: func(http.ResponseWriter, *chSeen) {
		entered <- struct{}{}
		<-release
	}}
}

// cachedRoutes are the two read paths that cache and coalesce, each with a
// body the viewer role of cachedRouter may run.
var cachedRoutes = []struct{ name, path, body string }{
	{name: "structured query", path: "/v1/query?table=clicks", body: `{"select_all": true}`},
	{name: "pipe execute", path: "/v1/pipes/top_pages", body: `{}`},
}

// cachedRouter is the real router over tenants, both cached read paths wired
// to ch and c. Every request resolves to the viewer role, which may read
// clicks.page and run top_pages.
func cachedRouter(t *testing.T, tenants *settings.Registry, ch *fakeCH, c cache.Cache) http.Handler {
	t.Helper()
	return cachedRouterOver(t, tenants, ch.target, ch, c)
}

// cachedRouterOver is cachedRouter with the tenant's ClickHouse target chosen
// per request by targetFor.
func cachedRouterOver(t *testing.T, tenants *settings.Registry, targetFor func(*settings.Store) chconn.Target, ch *fakeCH, c cache.Cache) http.Handler {
	t.Helper()
	reg := testRegistry(t)
	viewer := staticPolicy(&policy.Policy{
		DefaultRole: "viewer",
		Tables:      map[string]policy.TablePolicy{"clicks": {"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}}},
	})
	timeout := func(*settings.Store) time.Duration { return 5 * time.Second }
	sq := NewStructuredQueryHandler(targetFor, c, fixedRegistry(reg), viewer, func(*settings.Store) int { return 60 }, timeout, nil)
	sq.ch = ch.reader()
	pipesHandler := NewPipesHandler(staticPipes(&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}}), viewer, targetFor, c, timeout)
	pipesHandler.ch = ch.reader()
	return NewRouter(Dependencies{
		Tenants:         tenants,
		Ingest:          NewIngestHandler(fixedRegistry(reg), &testutil.MockPublisher{}),
		StructuredQuery: sq,
		Pipes:           pipesHandler,
		Query:           &QueryHandler{},
		SSE:             NewStreamHandler(stream.NewHub(nil, nil, nil), nil),
		Health:          &HealthHandler{},
		Version:         NewVersionHandler("test", "test", "test"),
		Schema:          schemaHandlerOver(reg, tenants),
		AuthMW:          func(next http.Handler) http.Handler { return next },
		PolicySource:    policy.Static(&policy.Policy{}),
	})
}

// serveAs posts body to path under id's tenant header — none for "" — and
// returns the recorder.
func serveAs(t *testing.T, router http.Handler, path, body string, id tenant.ID) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if id != "" {
		req.Header.Set(tenant.Header, id.String())
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// A cached result is one tenant's. Two tenants sending the identical request
// through the real router — TenantMW ahead of the handlers, one ristretto pool
// behind them — each miss once and hit once, and ClickHouse is queried once
// per tenant: the second tenant is never served the first one's rows. The
// query key, the singleflight key, and the dependency namespaces all lead
// with the tenant (#583 story 8), which is what keeps this true now that each
// tenant reads its own ClickHouse (story 6) — a tenant-blind key would be a
// silent cross-tenant read.
//
// The subtests share one router, one pool and one query counter, so they run
// in order: neither the parent nor the subtests are parallel.
func TestNewRouter_CacheIsKeyedByTenant(t *testing.T) {
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(200)})
	l1, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l1.Close() })
	ch := &fakeCH{}
	router := cachedRouter(t, tenants, ch, l1)

	for _, route := range cachedRoutes {
		t.Run(route.name, func(t *testing.T) {
			before := ch.reads.Load()
			// Ristretto admits asynchronously: settle after each request so the
			// next one reads what the last one stored.
			xcache := func(id tenant.ID) string {
				w := serveAs(t, router, route.path, route.body, id)
				require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
				l1.Wait()
				return w.Header().Get("X-Cache")
			}
			assert.Equal(t, "MISS", xcache("acme"))
			assert.Equal(t, "HIT", xcache("acme"))
			assert.Equal(t, "MISS", xcache("globex"), "a tenant must never be served another tenant's cached result")
			assert.Equal(t, "HIT", xcache("globex"))
			assert.Equal(t, before+2, ch.reads.Load(), "one query per tenant")
		})
	}
}

// A directory that holds the four files serves tenant 0 alone, and a request
// without the header is tenant 0's: its keys gain the "0" prefix and nothing
// else changes — the second identical request is still a hit, with or
// without the header spelled out. Sequential for the same reason as above.
func TestNewRouter_FlatDirectoryCacheStillHits(t *testing.T) {
	l1, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l1.Close() })
	ch := &fakeCH{}
	router := cachedRouter(t, testTenants(), ch, l1)

	for _, route := range cachedRoutes {
		t.Run(route.name, func(t *testing.T) {
			before := ch.reads.Load()
			xcache := func(id tenant.ID) string {
				w := serveAs(t, router, route.path, route.body, id)
				require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
				l1.Wait()
				return w.Header().Get("X-Cache")
			}
			assert.Equal(t, "MISS", xcache(""))
			assert.Equal(t, "HIT", xcache(""))
			assert.Equal(t, "HIT", xcache(tenant.Default))
			assert.Equal(t, before+1, ch.reads.Load())
		})
	}
}

// Concurrent identical requests coalesce into one flight to ClickHouse — for
// one tenant. The singleflight key is the tenant-led cache key, so two
// tenants' identical requests in flight together are two queries: neither
// waits on, or receives, the other's result. Under synctest the count is
// exact: Wait returns once every request is either inside ClickHouse or parked on
// another's flight, with no sleep to race.
func TestCachedRoutes_SingleflightIsPerTenant(t *testing.T) {
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(200)})
	tests := []struct {
		name        string
		tenants     []tenant.ID
		wantQueries int
	}{
		{name: "the same tenant coalesces", tenants: []tenant.ID{"acme", "acme"}, wantQueries: 1},
		{name: "two tenants do not", tenants: []tenant.ID{"acme", "globex"}, wantQueries: 2},
	}
	for _, route := range cachedRoutes {
		for _, tt := range tests {
			t.Run(route.name+", "+tt.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					entered, release := make(chan struct{}, len(tt.tenants)), make(chan struct{})
					router := cachedRouter(t, tenants, gatedCH(entered, release), nil)
					var wg sync.WaitGroup
					for _, id := range tt.tenants {
						wg.Go(func() {
							w := serveAs(t, router, route.path, route.body, id)
							assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
						})
					}
					synctest.Wait()
					assert.Equal(t, tt.wantQueries, len(entered), "queries in flight once every request is blocked")
					close(release)
					wg.Wait()
				})
			})
		}
	}
}

// #382: a result is filed under the versions read before its query ran, so
// a bump landing mid-query orphans the fill — the next request misses and
// reads the post-write rows — rather than serving pre-write rows until TTL.
// The structured query is bumped the way the ingest worker bumps it; a pipe,
// which names no table yet, by InvalidateTenant.
func TestCachedRoutes_BumpDuringQueryOrphansTheFill(t *testing.T) {
	bumps := map[string]func(ctx context.Context, c cache.Cache) error{
		"structured query": func(ctx context.Context, c cache.Cache) error {
			_, err := c.Invalidate(ctx, []cache.Namespace{{Tenant: tenant.Default, Table: "clicks"}})
			return err
		},
		"pipe execute": func(ctx context.Context, c cache.Cache) error { return c.InvalidateTenant(ctx, tenant.Default) },
	}
	for _, route := range cachedRoutes {
		t.Run(route.name, func(t *testing.T) {
			l1, err := cache.NewLocal(1 << 20)
			require.NoError(t, err)
			t.Cleanup(func() { _ = l1.Close() })
			// The bump lands inside the first query only, as an insert that
			// lands while ClickHouse is still reading would.
			ch := &fakeCH{}
			ch.answer = func(http.ResponseWriter, *chSeen) {
				if ch.reads.Load() == 1 {
					require.NoError(t, bumps[route.name](t.Context(), l1))
				}
			}
			router := cachedRouter(t, testTenants(), ch, l1)
			xcache := func() string {
				w := serveAs(t, router, route.path, route.body, "")
				require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
				l1.Wait()
				return w.Header().Get("X-Cache")
			}
			assert.Equal(t, "MISS", xcache())
			assert.Equal(t, "MISS", xcache(), "the fill of a query a bump overtook is orphaned")
			assert.Equal(t, "HIT", xcache())
			assert.Equal(t, int32(2), ch.reads.Load())
		})
	}
}

// The snapshot is taken before the tenant's pool is chosen. A reload that
// moves the tenant to another address or database — Pools.Reconcile, then
// InvalidateTenant — landing between the two leaves the request on the old
// pool: its fill, read from the old database, is orphaned by the bump rather
// than filed as fresh under the new tenant version. And a tenant on no pool is a 503 even when its
// Lookup hit (#583 story 6).
func TestCachedRoutes_ReloadAsThePoolIsTakenOrphansTheFill(t *testing.T) {
	for _, route := range cachedRoutes {
		t.Run(route.name, func(t *testing.T) {
			l1, err := cache.NewLocal(1 << 20)
			require.NoError(t, err)
			t.Cleanup(func() { _ = l1.Close() })
			ch := &fakeCH{}
			var taken atomic.Int32
			var noPool atomic.Bool
			targetFor := func(s *settings.Store) chconn.Target {
				if noPool.Load() {
					return chconn.Target{}
				}
				if taken.Add(1) == 1 {
					require.NoError(t, l1.InvalidateTenant(t.Context(), tenant.Default))
				}
				return ch.target(s)
			}
			router := cachedRouterOver(t, testTenants(), targetFor, ch, l1)
			xcache := func() string {
				w := serveAs(t, router, route.path, route.body, "")
				require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
				l1.Wait()
				return w.Header().Get("X-Cache")
			}
			assert.Equal(t, "MISS", xcache())
			assert.Equal(t, "MISS", xcache(), "the fill of a query on the pool a reload replaced is orphaned")
			assert.Equal(t, "HIT", xcache())
			assert.Equal(t, int32(2), ch.reads.Load())

			noPool.Store(true)
			w := serveAs(t, router, route.path, route.body, "")
			assertUnavailable(t, w, noConnectionMessage, retryAfterPool)
		})
	}
}

// A structured query files its result under the table as the request names
// it, raw — the namespace the ingest worker bumps after an insert into that
// table (ingest's TestFlushTable_BumpsWhatTheReadFiles) — and the cache
// escapes both, so a name holding a dot or a space is served from the cache
// and orphaned by an insert like any other.
func TestStructuredQuery_RawTableNameMeetsTheInsertsBump(t *testing.T) {
	t.Parallel()
	tables := []string{"default.clicks", "my table"}
	schemas := make([]*discovery.TableSchema, 0, len(tables))
	grants := make(map[string]policy.TablePolicy, len(tables))
	for _, name := range tables {
		schemas = append(schemas, &discovery.TableSchema{Name: name, Columns: []discovery.Column{{Name: "page", Type: "String"}}})
		grants[name] = policy.TablePolicy{"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}}
	}
	l1, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l1.Close() })
	ch := &fakeCH{}
	h := NewStructuredQueryHandler(ch.target, l1, fixedRegistry(testutil.NewTestSchemaRegistry(t, schemas)),
		staticPolicy(&policy.Policy{DefaultRole: "viewer", Tables: grants}), func(*settings.Store) int { return 60 },
		func(*settings.Store) time.Duration { return 5 * time.Second }, nil)
	h.ch = ch.reader()

	for _, table := range tables {
		xcache := func() string {
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(structuredQueryRequest(t, table, query.StructuredQuery{SelectAll: true})))
			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			l1.Wait()
			return w.Header().Get("X-Cache")
		}
		assert.Equal(t, "MISS", xcache(), table)
		assert.Equal(t, "HIT", xcache(), table)
		_, err := l1.Invalidate(t.Context(), []cache.Namespace{{Tenant: tenant.Default, Table: table}})
		require.NoError(t, err)
		assert.Equal(t, "MISS", xcache(), "%s: the insert's bump orphans the cached result", table)
	}
}
