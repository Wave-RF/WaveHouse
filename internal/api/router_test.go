package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRequireAdmin_AdminAllowed(t *testing.T) {
	t.Parallel()
	handler := RequireAdmin(policy.NewMemoryStore(&policy.Policy{}), testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	handler := RequireAdmin(policy.NewMemoryStore(&policy.Policy{}), testutil.NopLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	store := policy.NewMemoryStore(&policy.Policy{AdminRole: "superuser"})
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

func TestCORSMiddleware_Preflight(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware([]string{"*"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("should not reach handler on OPTIONS")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Methods"), "POST")
}

func TestCORSMiddleware_NormalRequest(t *testing.T) {
	t.Parallel()
	var called bool
	handler := corsMiddleware([]string{"*"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	handler := corsMiddleware([]string{"https://app.example.com"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	handler := corsMiddleware([]string{"https://allowed.com"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
			handler := corsMiddleware(tc.allowed)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	handler := corsMiddleware([]string{"*"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	handler := corsMiddleware([]string{"https://allowed.com"})(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
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

	reg := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{Name: "events", Columns: []discovery.Column{{Name: "id", Type: "String"}}},
	})
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	deps := Dependencies{
		Ingest:  NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:   &QueryHandler{},
		SSE:     NewStreamHandler(hub, nil),
		Health:  &HealthHandler{},
		Version: NewVersionHandler("test", "test", "test"),
		Schema:  NewSchemaHandler(reg),
		AuthMW:  func(next http.Handler) http.Handler { return next },
		Logger:  testutil.NopLogger(),
	}

	router := NewRouter(deps)

	tests := []struct {
		method string
		path   string
		expect int // expected status (not 404/405)
	}{
		// Canonical K8s-convention probes.
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodGet, "/readyz", http.StatusOK},
		// Deprecated aliases (kept for v0.1.x, removed in v0.2.0).
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodGet, "/ready", http.StatusOK},
		{http.MethodGet, "/version", http.StatusOK},
		// Schema is admin-only (see TestNewRouter_SchemaAdminOnly); a roleless
		// request is denied 403 — the route still exists, which is what this
		// registration test asserts (not 404/405).
		{http.MethodGet, "/v1/schema", http.StatusForbidden},
		{http.MethodGet, "/v1/schema?table=events", http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(context.Background(), tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.Equal(t, tt.expect, rec.Code, "unexpected route status")
			assert.NotEqual(t, http.StatusNotFound, rec.Code, "route should exist")
			assert.NotEqual(t, http.StatusMethodNotAllowed, rec.Code, "method should be allowed")
		})
	}
}

// TestNewRouter_RawSQLAdminGate pins the contract for POST /v1/admin/query:
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

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	router := NewRouter(Dependencies{
		Ingest:      NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:       &QueryHandler{},
		SSE:         NewStreamHandler(hub, nil),
		Health:      &HealthHandler{},
		Schema:      NewSchemaHandler(reg),
		AuthMW:      func(next http.Handler) http.Handler { return next },
		PolicyStore: policy.NewMemoryStore(&policy.Policy{}),
		Logger:      testutil.NopLogger(),
	})

	post := func(role string) *httptest.ResponseRecorder {
		ctx := context.Background()
		if role != "" {
			ctx = auth.WithRole(ctx, role)
		}
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/admin/query", nil)
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

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	deps := Dependencies{
		Ingest:      NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:       &QueryHandler{},
		SSE:         NewStreamHandler(hub, nil),
		Health:      &HealthHandler{},
		Schema:      NewSchemaHandler(reg),
		AuthMW:      func(next http.Handler) http.Handler { return next },
		PolicyStore: policy.NewMemoryStore(&policy.Policy{}),
		Logger:      testutil.NopLogger(),
	}

	// Should not panic.
	router := NewRouter(deps)

	// Admin pipes route should 404 when pipes is nil. Send it as admin so the
	// /v1/admin gate passes and we observe the route being absent (the gate
	// runs before sub-route matching, so a roleless request would 403 first).
	req := httptest.NewRequestWithContext(auth.WithRole(context.Background(), "admin"), http.MethodGet, "/v1/admin/pipes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// DLQ stats should 404 when DLQ is nil (the route — and its admin gate —
	// is never registered, so no role is needed to observe the 404).
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/dlq/stats", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCORSMiddleware_EmptyOrigins_AllowAll(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://anything.example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestNewRouter_NotFoundEmitsJSON(t *testing.T) {
	t.Parallel()

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()
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

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()
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

	// /healthz is registered for GET only; POST should hit MethodNotAllowed.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/healthz", nil)
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
	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	router := NewRouter(Dependencies{
		Ingest:      NewIngestHandler(reg, pub, testutil.NopLogger()),
		Query:       &QueryHandler{},
		SSE:         NewStreamHandler(hub, nil),
		Health:      &HealthHandler{},
		Schema:      NewSchemaHandler(reg),
		AuthMW:      func(next http.Handler) http.Handler { return next },
		PolicyStore: policy.NewMemoryStore(&policy.Policy{}),
		Logger:      testutil.NopLogger(),
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

	for _, path := range []string{"/v1/schema", "/v1/schema?table=events"} {
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
			assert.NotEqual(t, http.StatusForbidden, rec.Code, "admin must reach schema")
			assert.NotEqual(t, http.StatusUnauthorized, rec.Code)
		})
	}
}
