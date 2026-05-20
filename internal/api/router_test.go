package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRequireRole_AllowedRole(t *testing.T) {
	t.Parallel()
	mw := RequireRole(true, "admin", "service")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ctx := context.WithValue(context.Background(), ContextKeyRole, "admin")
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireRole_DeniedRole(t *testing.T) {
	t.Parallel()
	mw := RequireRole(true, "admin", "service")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler should not be called")
	}))

	ctx := context.WithValue(context.Background(), ContextKeyRole, "viewer")
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assertJSONErrorResponse(t, w)
}

func TestRequireRole_NoRole_Passthrough(t *testing.T) {
	t.Parallel()
	mw := RequireRole(false, "admin")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// No role in context — auth disabled scenario → passthrough.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
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
		Ingest: NewIngestHandler(reg, pub),
		Query:  &QueryHandler{},
		SSE:    NewSSEHandler(hub, nil),
		WS:     NewWSHandler(hub, nil, nil),
		Health: &HealthHandler{},
		Schema: NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler { return next },
	}

	router := NewRouter(deps)

	tests := []struct {
		method string
		path   string
		expect int // expected status (not 404/405)
	}{
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodGet, "/ready", http.StatusOK},
		{http.MethodGet, "/v1/schema", http.StatusOK},
		{http.MethodGet, "/v1/schema/events", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(context.Background(), tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.NotEqual(t, http.StatusNotFound, rec.Code, "route should exist")
			assert.NotEqual(t, http.StatusMethodNotAllowed, rec.Code, "method should be allowed")
		})
	}
}

// TestNewRouter_RawSQLAdminGate pins the contract for POST /v1/admin/query:
//
//	admin role          → reaches handler
//	service role        → reaches handler (same gate as the rest of /v1/admin/*)
//	viewer role         → 403
//	no role, auth off   → reaches handler (dev/test posture)
//	no role, auth on    → 401
//
// A regression that re-mounted the route under the top-level /v1 auth
// middleware (the pre-move state) would let viewer through — the viewer
// sub-test would flip from 403 to "reaches handler" and fail.
func TestNewRouter_RawSQLAdminGate(t *testing.T) {
	t.Parallel()

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	build := func(authEnabled bool) http.Handler {
		return NewRouter(Dependencies{
			Ingest:      NewIngestHandler(reg, pub),
			Query:       &QueryHandler{},
			SSE:         NewSSEHandler(hub, nil),
			WS:          NewWSHandler(hub, nil, nil),
			Health:      &HealthHandler{},
			Schema:      NewSchemaHandler(reg),
			AuthMW:      func(next http.Handler) http.Handler { return next },
			AuthEnabled: authEnabled,
		})
	}

	post := func(router http.Handler, role string) *httptest.ResponseRecorder {
		ctx := context.Background()
		if role != "" {
			ctx = context.WithValue(ctx, ContextKeyRole, role)
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
		rec := post(build(true), "admin")
		assert.NotEqual(t, http.StatusNotFound, rec.Code, "admin must reach the handler")
		assert.NotEqual(t, http.StatusForbidden, rec.Code, "admin must not be 403'd")
		assert.NotEqual(t, http.StatusUnauthorized, rec.Code, "admin must not be 401'd")
		assert.NotEqual(t, http.StatusMethodNotAllowed, rec.Code, "admin path must remain POST-mounted")
	})

	t.Run("service reaches handler", func(t *testing.T) {
		t.Parallel()
		// /v1/admin/query shares the /v1/admin/* gate — admin and
		// service both pass. Service tokens already have admin-scoped
		// powers across the rest of the admin tree (policy CRUD, pipes
		// CRUD, log-level), so excluding them just for raw SQL would be
		// inconsistency without a real authorization win.
		rec := post(build(true), "service")
		assert.NotEqual(t, http.StatusNotFound, rec.Code)
		assert.NotEqual(t, http.StatusForbidden, rec.Code)
		assert.NotEqual(t, http.StatusUnauthorized, rec.Code)
		assert.NotEqual(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("viewer is 403", func(t *testing.T) {
		t.Parallel()
		rec := post(build(true), "viewer")
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assertJSONErrorResponse(t, rec)
	})

	t.Run("auth disabled passthrough for no role", func(t *testing.T) {
		t.Parallel()
		// Auth disabled = role middleware passes through (dev/test posture).
		// The endpoint is still reachable so the handler can decide. Pin
		// negative assertions for the four statuses that would indicate
		// a routing/auth regression: 404 (route missing), 403 (role gate
		// firing despite auth being off), 401 (auth middleware rejecting
		// despite auth being off), 405 (POST no longer mounted on this path).
		rec := post(build(false), "")
		assert.NotEqual(t, http.StatusNotFound, rec.Code)
		assert.NotEqual(t, http.StatusForbidden, rec.Code)
		assert.NotEqual(t, http.StatusUnauthorized, rec.Code)
		assert.NotEqual(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("auth enabled rejects no role with 401", func(t *testing.T) {
		t.Parallel()
		rec := post(build(true), "")
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assertJSONErrorResponse(t, rec)
	})
}

func TestNewRouter_OptionalDepsNil(t *testing.T) {
	t.Parallel()

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	deps := Dependencies{
		Ingest: NewIngestHandler(reg, pub),
		Query:  &QueryHandler{},
		SSE:    NewSSEHandler(hub, nil),
		WS:     NewWSHandler(hub, nil, nil),
		Health: &HealthHandler{},
		Schema: NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler { return next },
		// DLQ, Policy, Pipes, StructuredQuery all nil.
	}

	// Should not panic.
	router := NewRouter(deps)

	// Admin pipes route should 404 when pipes is nil.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/admin/pipes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// DLQ stats should 404 when DLQ is nil.
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

func TestRequireRole_NoRole_FailClosed(t *testing.T) {
	t.Parallel()

	// Empty context is REJECTED
	mw := RequireRole(true, "admin")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler should not be called - security should have blocked this!")
	}))

	// Create a request with NO role in the context
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "unauthorized")
	assertJSONErrorResponse(t, w)
}

func TestNewRouter_NotFoundEmitsJSON(t *testing.T) {
	t.Parallel()

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()
	deps := Dependencies{
		Ingest: NewIngestHandler(reg, pub),
		Query:  &QueryHandler{},
		SSE:    NewSSEHandler(hub, nil),
		WS:     NewWSHandler(hub, nil, nil),
		Health: &HealthHandler{},
		Schema: NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler { return next },
	}
	router := NewRouter(deps)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/no-such-path", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assertJSONErrorResponse(t, rec)
}

func TestNewRouter_MethodNotAllowedEmitsJSON(t *testing.T) {
	t.Parallel()

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()
	deps := Dependencies{
		Ingest: NewIngestHandler(reg, pub),
		Query:  &QueryHandler{},
		SSE:    NewSSEHandler(hub, nil),
		WS:     NewWSHandler(hub, nil, nil),
		Health: &HealthHandler{},
		Schema: NewSchemaHandler(reg),
		AuthMW: func(next http.Handler) http.Handler { return next },
	}
	router := NewRouter(deps)

	// /health is registered for GET only; POST should hit MethodNotAllowed.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assertJSONErrorResponse(t, rec)
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
	assertJSONErrorResponse(t, rec)
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
