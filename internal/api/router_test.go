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

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), ContextKeyRole, "admin")
	req = req.WithContext(ctx)

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

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), ContextKeyRole, "viewer")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestRequireRole_NoRole_Passthrough(t *testing.T) {
	t.Parallel()
	mw := RequireRole(false, "admin")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// No role in context — auth disabled scenario → passthrough.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestCORSMiddleware_Preflight(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware([]string{"*"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("should not reach handler on OPTIONS")
	}))

	req := httptest.NewRequest(http.MethodOptions, "/", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/", nil)
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
			req := httptest.NewRequest(tt.method, tt.path, nil)
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
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/pipes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// DLQ stats should 404 when DLQ is nil.
	req = httptest.NewRequest(http.MethodGet, "/v1/dlq/stats", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCORSMiddleware_EmptyOrigins_AllowAll(t *testing.T) {
	t.Parallel()
	handler := corsMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
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
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "unauthorized")
}
