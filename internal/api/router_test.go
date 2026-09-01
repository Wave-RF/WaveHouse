package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequireAdmin_AdminAllowed(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(policy.Static(&policy.Policy{}), testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	ctx := auth.WithRole(context.Background(), "admin")
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireAdmin_NonAdminForbidden(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(policy.Static(&policy.Policy{}), testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler should not be called")
	}))
	ctx := auth.WithRole(context.Background(), "viewer")
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
	testutil.AssertJSONErrorResponse(t, w)
}

// TestRequireAdmin_NoRoleForbidden: a roleless request (no token, or an
// invalid/expired one that fell back to an empty role) must never reach an
// admin route — fail closed with 403.
func TestRequireAdmin_NoRoleForbidden(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(nil, testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler should not be called - a roleless request must not reach an admin route")
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
	testutil.AssertJSONErrorResponse(t, w)
}

// TestRequireAdmin_CustomAdminRole: the configured admin_role passes; the
// literal "admin" is an ordinary (denied) role under a custom admin_role.
func TestRequireAdmin_CustomAdminRole(t *testing.T) {
	t.Parallel()
	store := policy.Static(&policy.Policy{AdminRole: "superuser"})
	handler := RequireAdmin(store, testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for role, want := range map[string]int{"superuser": http.StatusOK, "admin": http.StatusForbidden} {
		ctx := auth.WithRole(context.Background(), role)
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, want, w.Code, "role %q", role)
	}
}

// TestRequireAdmin_InvalidTokenFailsLoud: a request whose token failed to
// validate fell back to an empty role; the gate must surface the token reason
// (401) rather than a bare 403.
func TestRequireAdmin_InvalidTokenFailsLoud(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(nil, testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler should not be called")
	}))
	ctx := auth.WithAuthError(context.Background(), errors.New("token expired"))
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "token expired")
}

// TestRequireAdmin_OperatorBypass: the operator bit (set by the auth middleware
// on a valid operator key) passes the gate regardless of the role — even a
// non-admin one.
func TestRequireAdmin_OperatorBypass(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(policy.Static(&policy.Policy{}), testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	ctx := auth.WithOperator(auth.WithRole(context.Background(), "viewer"))
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "operator bit passes the admin gate regardless of role")
}

// TestRequireAdmin_OperatorBypassesNilPolicy: break-glass — with no policy at
// all (IsAdmin admits nobody), the operator bit still admits the request so the
// operator can inspect the policy and trigger a settings reload while locked out.
func TestRequireAdmin_OperatorBypassesNilPolicy(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(nil, testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	ctx := auth.WithOperator(context.Background())
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "operator bit admits even a nil-policy request (break-glass)")
}

// TestCORSMiddleware_OriginsReloadBetweenRequests pins that the allowlist is
// resolved per request, not captured at router construction: a settings
// reload that changes cors.allowed_origins must apply to the very next
// request without rebuilding the middleware.
func TestCORSMiddleware_OriginsReloadBetweenRequests(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	origins := []string{"https://old.example.com"}
	handler := corsMiddleware(func() []string {
		mu.Lock()
		defer mu.Unlock()
		return origins
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	get := func(origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		req.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}

	assert.Equal(t, "https://old.example.com", get("https://old.example.com").Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, get("https://new.example.com").Header().Get("Access-Control-Allow-Origin"), "not yet allowed")

	// "Reload": swap the list the getter returns.
	mu.Lock()
	origins = []string{"https://new.example.com"}
	mu.Unlock()

	assert.Equal(t, "https://new.example.com", get("https://new.example.com").Header().Get("Access-Control-Allow-Origin"), "new allowlist applies on the next request")
	assert.Empty(t, get("https://old.example.com").Header().Get("Access-Control-Allow-Origin"), "old origin no longer allowed")
}

func TestCORSMiddleware_Preflight(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware(func() []string { return []string{"*"} })(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("should not reach handler on OPTIONS")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Methods"), "POST")
	// Last-Event-ID is the SSE resumption header (issue #215): cross-origin
	// fetch-based stream clients that resume via it must clear preflight.
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "Last-Event-ID")
}

func TestCORSMiddleware_NormalRequest(t *testing.T) {
	t.Parallel()
	var called bool
	handler := corsMiddleware(func() []string { return []string{"*"} })(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://app.example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.True(t, called)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSMiddleware_AllowListedOrigin(t *testing.T) {
	t.Parallel()
	var called bool
	handler := corsMiddleware(func() []string { return []string{"https://app.example.com"} })(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://app.example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.True(t, called)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "https://app.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "Origin", w.Header().Get("Vary"))
}

func TestCORSMiddleware_BlockedOrigin(t *testing.T) {
	t.Parallel()
	var called bool
	handler := corsMiddleware(func() []string { return []string{"https://allowed.com"} })(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://evil.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.True(t, called, "handler still invoked for non-allowed origin")
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Methods"),
		"non-allowed origins must not receive any CORS headers")
	// Vary: Origin is still set even though we reject the origin, so that a
	// shared cache can't memoize this headerless reject response and replay
	// it to a later allowed-origin request.
	assert.Equal(t, "Origin", w.Header().Get("Vary"),
		"allowlist mode must always emit Vary: Origin to keep caches per-origin")
}

// TestCORSMiddleware_NoCredentialsHeader pins the deliberate omission of
// Access-Control-Allow-Credentials. WaveHouse is a Bearer-token API; cookies
// are never used (see issue #30). Emitting credentials=true with a wildcard
// origin also breaks browsers per the CORS spec.
func TestCORSMiddleware_NoCredentialsHeader(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		allowed []string
		origin  string
	}{
		{"wildcard", []string{"*"}, "https://anything.example.com"},
		{"empty-allowlist-is-wildcard", nil, "https://anything.example.com"},
		{"allowlist-hit", []string{"https://app.example.com"}, "https://app.example.com"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler := corsMiddleware(func() []string { return tc.allowed })(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			req.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"),
				"Bearer-token API must not emit Access-Control-Allow-Credentials")
		})
	}
}

// TestCORSMiddleware_NoOriginIsPassthrough ensures same-origin / non-browser
// callers don't get CORS response headers stamped onto every response.
func TestCORSMiddleware_NoOriginIsPassthrough(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware(func() []string { return []string{"*"} })(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Methods"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Headers"))
}

// TestCORSMiddleware_BlockedOriginPreflight pins that a preflight from a
// disallowed origin returns 204 with no CORS headers — the browser treats
// that as a preflight failure, so the actual request never fires.
func TestCORSMiddleware_BlockedOriginPreflight(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware(func() []string { return []string{"https://allowed.com"} })(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("should not reach handler on OPTIONS")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://evil.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	// Pin the full "no CORS headers" contract — Allow-Origin alone isn't
	// enough; a regression that leaked Allow-Methods/Allow-Headers/etc. to a
	// disallowed origin would still slip through that single assertion.
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Methods"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Headers"))
	assert.Empty(t, w.Header().Get("Access-Control-Expose-Headers"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
	assert.Empty(t, w.Header().Get("Access-Control-Max-Age"))
	// Vary: Origin is still set in allowlist mode even on a reject, so a
	// shared cache can't memoize this headerless 204 under the URL alone
	// and replay it to a later allowed-origin preflight.
	assert.Equal(t, "Origin", w.Header().Get("Vary"))
}

func TestNewRouter_RoutesRegistered(t *testing.T) {
	t.Parallel()

	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "events", Columns: []discovery.Column{{Name: "id", Type: "String"}}},
	})
	pub := &testutil.MockPublisher{}
	hub := stream.NewHub(nil, nil, nil)

	emb, err := mq.NewEmbedded(t.TempDir(), 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	deps := Dependencies{
		Ingest:       NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:        &QueryHandler{},
		SSE:          NewStreamHandler(hub, nil),
		Health:       &HealthHandler{},
		Version:      NewVersionHandler("test", "test", "test"),
		Schema:       NewSchemaHandler(reg),
		DLQ:          NewDLQHandler(emb.JetStream(), testutil.NopLogger()),
		Pipes:        NewPipesHandler(pipes.Static(), policy.Static(&policy.Policy{}), nil, nil, nil, testutil.NopLogger()),
		AuthMW:       func(next http.Handler) http.Handler { return next },
		PolicySource: policy.Static(&policy.Policy{}),
		Logger:       testutil.NopLogger(),
	}

	router := NewRouter(deps)

	tests := []struct {
		method string
		path   string
		role   string // "" = roleless request (proves the row needs no gate)
		expect int
	}{
		// Canonical K8s-convention probes. Roleless: a 200 proves ungated.
		{http.MethodGet, "/livez", "", http.StatusOK},
		{http.MethodGet, "/readyz", "", http.StatusOK},
		// Deprecated aliases (kept for v0.1.x, removed in v0.2.0).
		{http.MethodGet, "/healthz", "", http.StatusOK},
		{http.MethodGet, "/health", "", http.StatusOK},
		{http.MethodGet, "/ready", "", http.StatusOK},
		{http.MethodGet, "/version", "", http.StatusOK},
		// Public content-free liveness ping for the SDK (under /v1, no auth gate).
		{http.MethodGet, "/v1/health", "", http.StatusOK},
		// Admin-gated /v1/ops routes. The tree-level gate denies before
		// sub-route matching, so a 403 would NOT prove registration (every
		// /v1/ops/<anything> 403s rolelessly) — these rows therefore run as
		// admin and require the handler's 200 (denial is pinned separately by
		// TestNewRouter_SchemaAdminOnly and TestNewRouter_RawSQLAdminGate).
		{http.MethodGet, "/v1/ops/schema", "admin", http.StatusOK},
		{http.MethodGet, "/v1/ops/schema?table=events", "admin", http.StatusOK},
		{http.MethodPost, "/v1/ops/schema/refresh", "admin", http.StatusOK},
		{http.MethodGet, "/v1/ops/dlq/stats", "admin", http.StatusOK},
		// The policy has no HTTP surface at all — the settings directory's
		// files are read and written where they live, and the former
		// endpoints must never come back (404 = no path registered). Pipes
		// keep their reads; the former pipe writes must never come back
		// either (405 = path registered, method not).
		{http.MethodGet, "/v1/ops/policy", "admin", http.StatusNotFound},
		{http.MethodPost, "/v1/ops/policy/validate", "admin", http.StatusNotFound},
		{http.MethodPut, "/v1/ops/policy", "admin", http.StatusNotFound},
		{http.MethodGet, "/v1/ops/pipes", "admin", http.StatusOK},
		{http.MethodPut, "/v1/ops/pipes/x", "admin", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/v1/ops/pipes/x", "admin", http.StatusMethodNotAllowed},
		// Pre-/v1/ops paths, removed with no aliases: they must stay 404.
		// Roleless, so re-registering an alias fails this row whether the
		// alias is gated (403) or — the real hazard — ungated (200).
		{http.MethodGet, "/v1/schema", "", http.StatusNotFound},
		{http.MethodPost, "/v1/schema/refresh", "", http.StatusNotFound},
		{http.MethodGet, "/v1/dlq/stats", "", http.StatusNotFound},
		{http.MethodPost, "/v1/admin/query", "", http.StatusNotFound},
		{http.MethodGet, "/v1/admin/policy", "", http.StatusNotFound},
		{http.MethodGet, "/v1/admin/pipes", "", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tt.role != "" {
				ctx = auth.WithRole(ctx, tt.role)
			}
			req := httptest.NewRequestWithContext(ctx, tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.Equal(t, tt.expect, rec.Code, "unexpected route status")
		})
	}
}

// TestNewRouter_CORSOnStream pins CORS on the SSE endpoint end-to-end through
// NewRouter against /v1/stream specifically (issue #215), so excluding the
// stream path from the middleware would fail here.
func TestNewRouter_CORSOnStream(t *testing.T) {
	t.Parallel()

	hub := stream.NewHub(nil, nil, nil)
	router := NewRouter(Dependencies{
		SSE:         NewStreamHandler(hub, nil),
		Health:      &HealthHandler{},
		AuthMW:      func(next http.Handler) http.Handler { return next },
		CORSOrigins: func() []string { return []string{"https://app.example.com"} },
		Logger:      testutil.NopLogger(),
	})

	// A fetch-based EventSource resuming cross-origin sends both Authorization
	// (the #203 auth migration off ?token=) and Last-Event-ID; the preflight
	// must allow-list both or the browser blocks the request. Last-Event-ID is
	// the header this PR added — without it this subtest fails.
	t.Run("preflight advertises Authorization and Last-Event-ID", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/v1/stream?table=events", nil)
		req.Header.Set("Origin", "https://app.example.com")
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		req.Header.Set("Access-Control-Request-Headers", "Authorization, Last-Event-ID")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, "https://app.example.com", rec.Header().Get("Access-Control-Allow-Origin"))
		allow := rec.Header().Get("Access-Control-Allow-Headers")
		assert.Contains(t, allow, "Last-Event-ID", "SSE resumption header must be allow-listed")
		assert.Contains(t, allow, "Authorization", "fetch-based EventSource auth (#203) must clear preflight")
	})

	// The streaming GET itself must carry Allow-Origin so the browser delivers
	// events. A pre-cancelled context exits the select loop after writing headers.
	t.Run("streaming GET echoes allowed origin", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=events", nil)
		req.Header.Set("Origin", "https://app.example.com")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		assert.Equal(t, "https://app.example.com", rec.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	})

	// A disallowed origin gets no Allow-Origin header, so the browser blocks it.
	t.Run("disallowed origin gets no CORS header", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/v1/stream?table=events", nil)
		req.Header.Set("Origin", "https://evil.example.com")
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})
}

// TestNewRouter_RawSQLAdminGate pins the contract for POST /v1/ops/query:
//
//	admin role   → reaches handler
//	service role → 403 (no longer privileged)
//	viewer role  → 403
//	no role      → 403 (a roleless request never reaches an admin route)
//
// A regression that re-mounted the route under the top-level /v1 auth
// middleware (the pre-move state) would let viewer through — the viewer
// sub-test would flip from 403 to "reaches handler" and fail.
func TestNewRouter_RawSQLAdminGate(t *testing.T) {
	t.Parallel()

	reg := testutil.NewTestSchemaRegistry(t, nil)
	pub := &testutil.MockPublisher{}
	hub := stream.NewHub(nil, nil, nil)

	router := NewRouter(Dependencies{
		Ingest:       NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:        &QueryHandler{},
		SSE:          NewStreamHandler(hub, nil),
		Health:       &HealthHandler{},
		Schema:       NewSchemaHandler(reg),
		AuthMW:       func(next http.Handler) http.Handler { return next },
		PolicySource: policy.Static(&policy.Policy{}),
		Logger:       testutil.NopLogger(),
	})

	post := func(role string) *httptest.ResponseRecorder {
		ctx := context.Background()
		if role != "" {
			ctx = auth.WithRole(ctx, role)
		}
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/ops/query", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	t.Run("admin reaches handler", func(t *testing.T) {
		t.Parallel()
		// nil driver.Conn would panic inside executeQuery, but the handler
		// returns 400 before that on a missing body — which is enough to
		// confirm the gate let the request through.
		rec := post("admin")
		assert.NotEqual(t, http.StatusNotFound, rec.Code, "admin must reach the handler")
		assert.NotEqual(t, http.StatusForbidden, rec.Code, "admin must not be 403'd")
		assert.NotEqual(t, http.StatusUnauthorized, rec.Code, "admin must not be 401'd")
		assert.NotEqual(t, http.StatusMethodNotAllowed, rec.Code, "admin path must remain POST-mounted")
	})

	t.Run("service is 403 (no longer privileged)", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, http.StatusForbidden, post("service").Code)
	})

	t.Run("viewer is 403", func(t *testing.T) {
		t.Parallel()
		rec := post("viewer")
		assert.Equal(t, http.StatusForbidden, rec.Code)
		testutil.AssertJSONErrorResponse(t, rec)
	})

	t.Run("no role is 403", func(t *testing.T) {
		t.Parallel()
		rec := post("")
		assert.Equal(t, http.StatusForbidden, rec.Code)
		testutil.AssertJSONErrorResponse(t, rec)
	})
}

func TestNewRouter_OptionalDepsNil(t *testing.T) {
	t.Parallel()

	reg := testutil.NewTestSchemaRegistry(t, nil)
	pub := &testutil.MockPublisher{}
	hub := stream.NewHub(nil, nil, nil)

	deps := Dependencies{
		Ingest:       NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:        &QueryHandler{},
		SSE:          NewStreamHandler(hub, nil),
		Health:       &HealthHandler{},
		Schema:       NewSchemaHandler(reg),
		AuthMW:       func(next http.Handler) http.Handler { return next },
		PolicySource: policy.Static(&policy.Policy{}),
		Logger:       testutil.NopLogger(),
	}

	// Should not panic.
	router := NewRouter(deps)

	// Admin pipes route should 404 when pipes is nil. Send it as admin so the
	// /v1/ops gate passes and we observe the route being absent (the gate
	// runs before sub-route matching, so a roleless request would 403 first).
	req := httptest.NewRequestWithContext(auth.WithRole(context.Background(), "admin"), http.MethodGet, "/v1/ops/pipes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// DLQ stats should 404 when DLQ is nil. As with pipes above, send it as
	// admin: the /v1/ops gate covers the whole tree and runs before sub-route
	// matching, so a roleless request would 403 before the absent route 404s.
	req = httptest.NewRequestWithContext(auth.WithRole(context.Background(), "admin"), http.MethodGet, "/v1/ops/dlq/stats", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// An absent getter, a nil list, and an empty list all mean allow-all — an
// empty allowlist is NOT "no origins". The validator warns on [] for exactly
// this reason; this pins the behavior the warning describes.
func TestCORSMiddleware_EmptyOrigins_AllowAll(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		origins func() []string
	}{
		{"nil getter", nil},
		{"nil list", func() []string { return nil }},
		{"empty list", func() []string { return []string{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler := corsMiddleware(tt.origins)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			req.Header.Set("Origin", "https://anything.example.com")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
		})
	}
}

func TestNewRouter_NotFoundEmitsJSON(t *testing.T) {
	t.Parallel()

	reg := testutil.NewTestSchemaRegistry(t, nil)
	pub := &testutil.MockPublisher{}
	hub := stream.NewHub(nil, nil, nil)
	deps := Dependencies{
		Ingest: NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:  &QueryHandler{},
		SSE:    NewStreamHandler(hub, nil),
		Health: &HealthHandler{},
		Schema: NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler { return next },
		Logger: testutil.NopLogger(),
	}
	router := NewRouter(deps)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/no-such-path", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	testutil.AssertJSONErrorResponse(t, rec)
}

func TestNewRouter_MethodNotAllowedEmitsJSON(t *testing.T) {
	t.Parallel()

	reg := testutil.NewTestSchemaRegistry(t, nil)
	pub := &testutil.MockPublisher{}
	hub := stream.NewHub(nil, nil, nil)
	deps := Dependencies{
		Ingest: NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:  &QueryHandler{},
		SSE:    NewStreamHandler(hub, nil),
		Health: &HealthHandler{},
		Schema: NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler { return next },
		Logger: testutil.NopLogger(),
	}
	router := NewRouter(deps)

	// /livez is registered for GET only; POST should hit MethodNotAllowed.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/livez", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	testutil.AssertJSONErrorResponse(t, rec)
}

func TestJSONRecoverer_PanicEmitsJSON(t *testing.T) {
	t.Parallel()

	handler := jsonRecoverer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	testutil.AssertJSONErrorResponse(t, rec)
	assert.Contains(t, rec.Body.String(), "internal server error")
}

func TestJSONRecoverer_NoPanicPassthrough(t *testing.T) {
	t.Parallel()

	handler := jsonRecoverer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusTeapot, rec.Code)
}

func TestJSONRecoverer_AbortHandlerRepanics(t *testing.T) {
	t.Parallel()

	handler := jsonRecoverer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	assert.PanicsWithValue(t, http.ErrAbortHandler, func() {
		handler.ServeHTTP(rec, req)
	}, "ErrAbortHandler must propagate so the server's serve loop can terminate the connection")
}

func TestJSONRecoverer_PanicAfterPartialWriteDoesNotCorrupt(t *testing.T) {
	t.Parallel()

	// If the handler has already flushed bytes to the wire before
	// panicking, the headers are committed and a JSON 500 appended after
	// them would corrupt the response. The recoverer must detect this
	// and skip the write.
	handler := jsonRecoverer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial body"))
		panic("boom mid-stream")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "status already committed before panic must not be overwritten")
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"), "headers already flushed must not be rewritten")
	assert.Equal(t, "partial body", rec.Body.String(), "JSON 500 body must not be appended after a partial write")
}

// TestNewRouter_SchemaAdminOnly confirms schema discovery is admin-only: a
// tokenless/non-admin request is denied (403), while an admin reaches the
// handler. Schema carries no policy/allowlist gate of its own, so the admin
// gate is its entire authorization story.
func TestNewRouter_SchemaAdminOnly(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "events", Columns: []discovery.Column{{Name: "id", Type: "String"}}},
	})
	pub := &testutil.MockPublisher{}
	hub := stream.NewHub(nil, nil, nil)

	router := NewRouter(Dependencies{
		Ingest:       NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:        &QueryHandler{},
		SSE:          NewStreamHandler(hub, nil),
		Health:       &HealthHandler{},
		Schema:       NewSchemaHandler(reg),
		AuthMW:       func(next http.Handler) http.Handler { return next },
		PolicySource: policy.Static(&policy.Policy{}),
		Logger:       testutil.NopLogger(),
	})

	get := func(path, role string) *httptest.ResponseRecorder {
		ctx := context.Background()
		if role != "" {
			ctx = auth.WithRole(ctx, role)
		}
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	for _, path := range []string{"/v1/ops/schema", "/v1/ops/schema?table=events"} {
		t.Run(path+" tokenless 403", func(t *testing.T) {
			t.Parallel()
			rec := get(path, "")
			assert.Equal(t, http.StatusForbidden, rec.Code,
				"tokenless request to %s must be denied (schema is admin-only)", path)
			testutil.AssertJSONErrorResponse(t, rec)
		})
		t.Run(path+" admin reaches handler", func(t *testing.T) {
			t.Parallel()
			rec := get(path, "admin")
			// Require the handler's 200, not merely "not denied": under the
			// tree-level /v1/ops gate a 404 would also be "not 403", so only
			// a success status proves the admin reached the schema handler.
			assert.Equal(t, http.StatusOK, rec.Code, "admin must reach schema")
		})
	}
}
