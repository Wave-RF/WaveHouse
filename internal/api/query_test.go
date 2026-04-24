package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

// safeHandle calls a handler and recovers from panics.
// Used when tests verify validation logic but pass nil for driver.Conn,
// which would panic once the handler reaches executeQuery.
func safeHandle(handler http.HandlerFunc, w *httptest.ResponseRecorder, r *http.Request) {
	defer func() { _ = recover() }()
	handler(w, r)
}

func TestQueryHandler_MissingSQL(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler(nil, nil, 0)
	body, _ := json.Marshal(queryRequest{SQL: ""})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing sql")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler(nil, nil, 0)
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader([]byte(`{bad}`)))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_PolicyForbidsRawSQL(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	store := policy.NewMemoryStore(p)
	h := NewQueryHandler(nil, nil, 0)
	h.PolicyStore = store

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM clicks"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	// Set role to "viewer" (no RawSQL permission).
	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	h.Handle(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "raw SQL queries require admin role")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_PolicyAllowsAdmin(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	store := policy.NewMemoryStore(p)
	h := NewQueryHandler(nil, nil, 0)
	h.PolicyStore = store

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM clicks"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	// Admin bypasses the raw SQL check.
	ctx := context.WithValue(r.Context(), ContextKeyRole, "admin")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	// Will fail at executeQuery (nil conn) but should get past the policy check.
	safeHandle(h.Handle, w, r)
	// Should NOT be 403.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
}

func TestQueryHandler_PolicyAllowsRawSQLRole(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"analyst": {AllowColumns: []string{"page"}, RawSQL: true},
				},
			},
		},
	}
	store := policy.NewMemoryStore(p)
	h := NewQueryHandler(nil, nil, 0)
	h.PolicyStore = store

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM clicks"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	ctx := context.WithValue(r.Context(), ContextKeyRole, "analyst")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	safeHandle(h.Handle, w, r)
	// Should NOT be 403 — analyst has RawSQL: true.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
}

func TestQueryHandler_NoPolicyAllowsAll(t *testing.T) {
	t.Parallel()
	// No PolicyStore — raw SQL should be allowed for any role.
	h := NewQueryHandler(nil, nil, 0)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	r = r.WithContext(ctx)

	safeHandle(h.Handle, w, r)
	// Should NOT be 403.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
}

func TestQueryCacheKey_Deterministic(t *testing.T) {
	t.Parallel()
	k1 := queryCacheKey("SELECT 1", nil)
	k2 := queryCacheKey("SELECT 1", nil)
	assert.Equal(t, k1, k2)

	k3 := queryCacheKey("SELECT 1", []any{"a"})
	assert.NotEqual(t, k1, k3)

	k4 := queryCacheKey("SELECT 2", nil)
	assert.NotEqual(t, k1, k4)
}
