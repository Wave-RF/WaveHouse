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
		SSE:    NewSSEHandler(hub, nil, "WAVEHOUSE"),
		WS:     NewWSHandler(hub, nil, nil, "WAVEHOUSE"),
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

func TestNewRouter_OptionalDepsNil(t *testing.T) {
	t.Parallel()

	reg := discovery.NewSchemaRegistryFromMap(nil)
	pub := &testutil.MockPublisher{}
	hub := NewHub()

	deps := Dependencies{
		Ingest: NewIngestHandler(reg, pub),
		Query:  &QueryHandler{},
		SSE:    NewSSEHandler(hub, nil, "WAVEHOUSE"),
		WS:     NewWSHandler(hub, nil, nil, "WAVEHOUSE"),
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
		SSE:    NewSSEHandler(hub, nil, "WAVEHOUSE"),
		WS:     NewWSHandler(hub, nil, nil, "WAVEHOUSE"),
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
		SSE:    NewSSEHandler(hub, nil, "WAVEHOUSE"),
		WS:     NewWSHandler(hub, nil, nil, "WAVEHOUSE"),
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
