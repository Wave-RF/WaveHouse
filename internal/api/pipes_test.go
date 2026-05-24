package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
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
			ctx := auth.WithRole(r.Context(), tc.Role)
			ctx = auth.WithClaims(ctx, jwt.MapClaims{})
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

// TestPipesHandler_Execute_DefaultRoleGrantsAccess: a tokenless request (no role
// in context) resolves to the policy default_role, which is in AllowedRoles.
func TestPipesHandler_Execute_DefaultRoleGrantsAccess(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "report", SQL: "SELECT * FROM clicks", AllowedRoles: []string{"viewer"}},
	)
	h := NewPipesHandler(store, nil, nil, 0)
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{DefaultRole: "viewer"})

	w := httptest.NewRecorder()
	// No role in context (tokenless / JWT without role claim).
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/report/execute", "report", nil)

	safeHandle(h.Execute, w, r)

	assert.NotEqual(t, http.StatusForbidden, w.Code,
		"empty role should resolve to default_role 'viewer', which is in AllowedRoles")
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// TestPipesHandler_Execute_DefaultRoleNotInAllowedRolesDenied: the default_role
// is still gated by AllowedRoles — a default that isn't listed is denied.
func TestPipesHandler_Execute_DefaultRoleNotInAllowedRolesDenied(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "admin_report", SQL: "SELECT * FROM clicks", AllowedRoles: []string{"admin"}},
	)
	h := NewPipesHandler(store, nil, nil, 0)
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{DefaultRole: "viewer"})

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/admin_report/execute", "admin_report", nil)

	safeHandle(h.Execute, w, r)

	assert.Equal(t, http.StatusForbidden, w.Code,
		"default_role 'viewer' is not in AllowedRoles [admin] → denied")
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
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

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
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

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
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	safeHandle(h.Execute, w, r)

	// Should pass param binding — will fail at executeQuery (nil conn).
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// TestPipesHandler_Execute_NoAllowedRoles_NonAdminDenied: a pipe with no
// allowed_roles authorizes nobody but the privileged built-ins. An ordinary
// authenticated caller (claims present, role "viewer") is rejected because the
// empty allowlist matches no role.
func TestPipesHandler_Execute_NoAllowedRoles_NonAdminDenied(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "open", SQL: "SELECT * FROM clicks"}, // no AllowedRoles
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/open/execute", "open", nil)
	ctx := auth.WithClaims(r.Context(), jwt.MapClaims{})
	ctx = auth.WithRole(ctx, "viewer")
	r = r.WithContext(ctx)

	safeHandle(h.Execute, w, r)

	assert.Equal(t, http.StatusForbidden, w.Code,
		"a pipe with no allowed_roles must reject a non-admin role")
	assertJSONErrorResponse(t, w)
}

// TestPipesHandler_Execute_NoAllowedRoles_AdminAllowed: the privileged built-in
// roles bypass the allowlist, so admin can run a pipe with no allowed_roles.
func TestPipesHandler_Execute_NoAllowedRoles_AdminAllowed(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "open", SQL: "SELECT * FROM clicks"},
	)
	h := NewPipesHandler(store, nil, nil, 0)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/open/execute", "open", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	safeHandle(h.Execute, w, r)

	assert.NotEqual(t, http.StatusForbidden, w.Code,
		"admin bypasses the allowlist on a pipe with no allowed_roles")
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}
