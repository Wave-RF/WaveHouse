package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pipesRequest(t *testing.T, method, path, name string, body any) *http.Request {
	t.Helper()
	var r *http.Request
	if body != nil {
		data, _ := json.Marshal(body)
		r = httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(data))
	} else {
		r = httptest.NewRequestWithContext(context.Background(), method, path, nil)
	}
	rctx := chi.NewRouteContext()
	if name != "" {
		rctx.URLParams.Add("name", name)
	}
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestPipesHandler_List(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT page, count(*) FROM clicks GROUP BY page"},
		&pipes.NamedQuery{Name: "recent", SQL: "SELECT * FROM clicks ORDER BY ts DESC LIMIT 10"},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/pipes", nil)
	h.List(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []*pipes.NamedQuery
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, 2)
}

func TestPipesHandler_Get_Found(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT page FROM clicks"},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/top_pages", "top_pages", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var got pipes.NamedQuery
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "top_pages", got.Name)
}

func TestPipesHandler_Get_NotFound(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore()
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/nope", "nope", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "pipe not found")
	assertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_NotFound(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore()
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/nope/execute", "nope", nil)
	h.Execute(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "pipe not found")
	assertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_RoleAuthorization(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		allowedRoles  []string
		role          string // role placed in context when setRole is true
		setRole       bool   // false → no ContextKeyRole in context at all
		wantForbidden bool
	}{
		{"open pipe, no role", nil, "", false, false},
		{"open pipe, empty role", nil, "", true, false},
		{"open pipe, any role", nil, "random_role", true, false},
		{"restricted pipe, matching role", []string{"admin"}, "admin", true, false},
		{"restricted pipe, non-matching role", []string{"admin"}, "viewer", true, true},
		{"restricted pipe, no role in context", []string{"admin"}, "", false, true},
		{"restricted pipe, empty role in context", []string{"admin"}, "", true, true},
		{"wildcard, any role", []string{"*"}, "viewer", true, false},
		{"wildcard, no role", []string{"*"}, "", false, false}, // wildcard is intentionally public
		{"multi-role allowlist, matching", []string{"admin", "editor"}, "editor", true, false},
		{"multi-role allowlist, non-matching", []string{"admin", "editor"}, "viewer", true, true},
		{"non-admin allowlist, no role in context", []string{"service"}, "", false, true},
		{"empty allowlist entry, empty role", []string{""}, "", true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := pipes.NewMemoryStore(
				&pipes.NamedQuery{
					Name:         "report",
					SQL:          "SELECT * FROM clicks",
					AllowedRoles: tc.allowedRoles,
				},
			)
			h := NewPipesHandler(store, nil, nil, 0)

			w := httptest.NewRecorder()
			r := pipesRequest(t, http.MethodPost, "/v1/pipes/report/execute", "report", nil)
			if tc.setRole {
				ctx := context.WithValue(r.Context(), ContextKeyRole, tc.role)
				ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
				r = r.WithContext(ctx)
			}

			// safeHandle recovers the nil-Conn panic on the allowed path so a
			// served request surfaces as a clean non-403 (default 200) rather
			// than crashing the parallel test binary. A forbidden request is
			// rejected before executeQuery, so it returns a real 403.
			safeHandle(h.Execute, w, r)

			if tc.wantForbidden {
				assert.Equal(t, http.StatusForbidden, w.Code,
					"pipe restricted to %v must reject role=%q (set=%v)", tc.allowedRoles, tc.role, tc.setRole)
				assertJSONErrorResponse(t, w)
			} else {
				assert.NotEqual(t, http.StatusForbidden, w.Code,
					"pipe with AllowedRoles=%v must allow role=%q (set=%v)", tc.allowedRoles, tc.role, tc.setRole)
				assert.NotEqual(t, http.StatusNotFound, w.Code)
			}
		})
	}
}

func TestPipesHandler_Execute_RestrictedPipe_EmptyRoleDenied(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{
			Name:         "admin_report",
			SQL:          "SELECT * FROM clicks",
			AllowedRoles: []string{"admin"},
		},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	// No ContextKeyRole set which simulates auth-disabled or a JWT without the role claim.
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/admin_report/execute", "admin_report", nil)

	safeHandle(h.Execute, w, r)

	assert.Equal(t, http.StatusForbidden, w.Code,
		"pipe restricted to %v must reject a request with no role in context", []string{"admin"})
	assertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_MissingParam(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{
			Name: "by_page",
			SQL:  "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{
				{Name: "page", Type: "string", Required: true},
			},
		},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	// No query params or body — missing "page".
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/by_page/execute", "by_page", nil)

	h.Execute(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing required parameter")
	assertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_ParamsFromQuery(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{
			Name: "by_page",
			SQL:  "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{
				{Name: "page", Type: "string", Required: true},
			},
		},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/pipes/by_page/execute?page=/home", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "by_page")
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	safeHandle(h.Execute, w, r)

	// Should pass param binding — will fail later at executeQuery (nil conn).
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

func TestPipesHandler_Put_InvalidJSON(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore()
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/v1/pipes/test", bytes.NewReader([]byte(`{bad}`)))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "test")
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	h.Put(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
}

func TestPipesHandler_Put_Success(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore()
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPut, "/v1/pipes/new_pipe", "new_pipe", map[string]any{
		"sql":         "SELECT count(*) FROM clicks",
		"description": "counts",
	})
	h.Put(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"ok":true`)
	// Verify the pipe is in the store.
	assert.NotNil(t, store.Get("new_pipe"))
	assert.Equal(t, "SELECT count(*) FROM clicks", store.Get("new_pipe").SQL)
}

func TestPipesHandler_Put_MissingSQL(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore()
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPut, "/v1/pipes/bad", "bad", map[string]any{
		"description": "no sql",
	})
	h.Put(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "SQL is required")
}

func TestPipesHandler_Delete_Success(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "to_delete", SQL: "SELECT 1"},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodDelete, "/v1/pipes/to_delete", "to_delete", nil)
	h.Delete(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"ok":true`)
	assert.Nil(t, store.Get("to_delete"))
}

func TestPipesHandler_Execute_PostBodyParams(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{
			Name: "by_page",
			SQL:  "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{
				{Name: "page", Type: "string", Required: true},
			},
		},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	body := map[string]any{"page": "/about"}
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/by_page/execute", "by_page", body)

	safeHandle(h.Execute, w, r)

	// Should pass param binding — will fail at executeQuery (nil conn).
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}
