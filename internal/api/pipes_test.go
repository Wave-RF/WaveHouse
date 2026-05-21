package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
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
	testutil.RunRoleMatrix(t, testutil.StandardRoleMatrix(), func(t *testing.T, tc testutil.RoleCase) *httptest.ResponseRecorder {
		store := pipes.NewMemoryStore(
			&pipes.NamedQuery{
				Name:         "report",
				SQL:          "SELECT * FROM clicks",
				AllowedRoles: tc.AllowedRoles,
			},
		)
		h := NewPipesHandler(store, nil, nil, 0)

		w := httptest.NewRecorder()
		r := pipesRequest(t, http.MethodPost, "/v1/pipes/report/execute", "report", nil)
		if tc.SetRole {
			ctx := context.WithValue(r.Context(), ContextKeyRole, tc.Role)
			ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
			r = r.WithContext(ctx)
		}

		// safeHandle recovers the nil-backend panic on the allowed path so a
		// served request surfaces as a clean non-403 rather than crashing the
		// parallel test binary; a forbidden request returns a real 403 first.
		safeHandle(h.Execute, w, r)
		return w
	})
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
