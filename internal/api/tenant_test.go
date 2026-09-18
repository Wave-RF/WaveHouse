package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

func TestTenantMW(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		header     []string // every X-Tenant-ID line sent
		wantStatus int
		wantBody   string
	}{
		{name: "no header resolves to the default tenant", wantStatus: http.StatusOK},
		{name: "empty header resolves to the default tenant", header: []string{""}, wantStatus: http.StatusOK},
		{name: "explicit default tenant", header: []string{"0"}, wantStatus: http.StatusOK},
		{name: "well-formed unknown tenant", header: []string{"acme"}, wantStatus: http.StatusNotFound, wantBody: "unknown tenant: acme"},
		{name: "malformed id", header: []string{"../etc"}, wantStatus: http.StatusBadRequest, wantBody: "invalid X-Tenant-ID"},
		{name: "subject wildcard", header: []string{">"}, wantStatus: http.StatusBadRequest, wantBody: "invalid X-Tenant-ID"},
		{name: "over the length cap", header: []string{strings.Repeat("a", tenant.MaxLen+1)}, wantStatus: http.StatusBadRequest, wantBody: "invalid X-Tenant-ID"},
		{name: "repeated header, even agreeing", header: []string{"0", "0"}, wantStatus: http.StatusBadRequest, wantBody: "sent more than once"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var resolved *settings.Store
			h := TenantMW(testTenants())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resolved, _ = StoreFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/health", nil)
			for _, v := range tt.header {
				req.Header.Add(tenant.Header, v)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body.String())
			if tt.wantStatus == http.StatusOK {
				assert.Same(t, testStore, resolved, "the resolved store rides the request context")
				return
			}
			assert.Nil(t, resolved, "a refused request must not reach the handler")
			assert.Contains(t, w.Body.String(), tt.wantBody)
			assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
		})
	}
}

func TestStoreFromContext_Absent(t *testing.T) {
	t.Parallel()
	store, ok := StoreFromContext(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil).Context())
	assert.False(t, ok)
	assert.Nil(t, store)
}

// A tenant-route handler served without TenantMW must fail closed, never
// fall back to some tenant's settings.
func TestTenantRouteHandlers_NoResolvedTenantIs500(t *testing.T) {
	t.Parallel()
	reg := testRegistry(t)
	handlers := map[string]http.HandlerFunc{
		"ingest":           NewIngestHandler(reg, &testutil.MockPublisher{}, testutil.NopLogger()).Handle,
		"structured query": newStructuredQueryHandler(t).Handle,
		"pipe execute":     NewPipesHandler(staticPipes(), nil, nil, nil, noTimeout, testutil.NopLogger()).Execute,
	}
	for name, handle := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			handle(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/x?table=clicks", strings.NewReader(`{}`)))
			testutil.AssertJSONContains(t, w, http.StatusInternalServerError, map[string]any{"error": "internal server error"})
		})
	}
}

// tenantProbeRouter is a router whose AuthMW records whether the tenant
// store was already resolved when authentication ran.
func tenantProbeRouter(t *testing.T, sawStore *[]bool) http.Handler {
	t.Helper()
	reg := testRegistry(t)
	return NewRouter(Dependencies{
		Tenants: testTenants(),
		Ingest:  NewIngestHandler(reg, &testutil.MockPublisher{}, testutil.NopLogger()),
		Query:   &QueryHandler{},
		SSE:     NewStreamHandler(stream.NewHub(tenant.Default, nil, nil, nil), nil),
		Health:  &HealthHandler{},
		Version: NewVersionHandler("test", "test", "test"),
		Schema:  NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, ok := StoreFromContext(r.Context())
				*sawStore = append(*sawStore, ok)
				next.ServeHTTP(w, r)
			})
		},
		PolicySource: policy.Static(&policy.Policy{}),
		Logger:       testutil.NopLogger(),
	})
}

func TestNewRouter_TenantResolvesBeforeAuth(t *testing.T) {
	t.Parallel()
	var sawStore []bool
	router := tenantProbeRouter(t, &sawStore)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/health", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []bool{true}, sawStore, "AuthMW must run after the tenant is resolved")

	// An unknown tenant is refused before authentication ever runs.
	sawStore = nil
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/health", nil)
	req.Header.Set(tenant.Header, "acme")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Empty(t, sawStore)
}

// The probes, /version, and /v1/ops/* never look at the tenant header: a
// value that would 404 or 400 on a tenant route changes nothing there, and
// the ops tree stays behind AuthMW and the admin gate.
func TestNewRouter_TenantExemptRoutes(t *testing.T) {
	t.Parallel()
	for _, header := range []string{"acme", "../etc"} {
		for _, path := range []string{"/livez", "/readyz", "/healthz", "/version"} {
			t.Run(header+" "+path, func(t *testing.T) {
				t.Parallel()
				var sawStore []bool
				req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
				req.Header.Set(tenant.Header, header)
				w := httptest.NewRecorder()
				tenantProbeRouter(t, &sawStore).ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
			})
		}
		t.Run(header+" /v1/ops/schema", func(t *testing.T) {
			t.Parallel()
			var sawStore []bool
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/ops/schema", nil)
			req.Header.Set(tenant.Header, header)
			w := httptest.NewRecorder()
			tenantProbeRouter(t, &sawStore).ServeHTTP(w, req)
			// Roleless, so the admin gate answers — not the tenant middleware.
			assert.Equal(t, http.StatusForbidden, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, []bool{false}, sawStore, "ops runs AuthMW with no tenant resolved")
		})
	}
}
