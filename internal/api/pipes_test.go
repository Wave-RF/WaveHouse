package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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
	if name == "" {
		return r
	}
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", name)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// noTimeout is the pipe-execution deadline source for handler tests that
// never reach ClickHouse.
func noTimeout(*settings.Store) time.Duration { return 0 }

func TestPipesHandler_List(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT page, count(*) FROM clicks GROUP BY page"},
		&pipes.NamedQuery{Name: "recent", SQL: "SELECT * FROM clicks ORDER BY ts DESC LIMIT 10"},
	)
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)
	h.Tenants = testTenants()

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/pipes", nil)
	h.List(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []*pipes.NamedQuery
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, 2)
}

func TestPipesHandler_Get_Found(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT page FROM clicks"},
	)
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)
	h.Tenants = testTenants()

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/ops/pipes/top_pages", "top_pages", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var got pipes.NamedQuery
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "top_pages", got.Name)
}

func TestPipesHandler_Get_NotFound(t *testing.T) {
	t.Parallel()
	store := staticPipes()
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)
	h.Tenants = testTenants()

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/ops/pipes/nope", "nope", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "pipe not found")
	testutil.AssertJSONErrorResponse(t, w)
}

// The admin reads serve the default tenant, which a nested settings directory
// need not hold and may hold rejected: the tenant routes' 404 and 503, never
// a nil store.
func TestPipesHandler_AdminReads_DefaultTenantNotServed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		configs    map[string]string
		wantStatus int
		wantBody   string
	}{
		{name: "no 0 folder", configs: map[string]string{"acme": fullConfig(100)}, wantStatus: http.StatusNotFound, wantBody: "unknown tenant: 0"},
		{name: "rejected 0 folder", configs: map[string]string{"acme": fullConfig(100), "0": `{"unknown_key": true}`}, wantStatus: http.StatusServiceUnavailable, wantBody: "tenant settings are invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := NewPipesHandler(staticPipes(&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT 1"}), nil, nil, nil, noTimeout)
			h.Tenants = nestedTenants(t, tt.configs)

			reads := map[string]func(http.ResponseWriter, *http.Request){"list": h.List, "get": h.Get}
			for name, read := range reads {
				w := httptest.NewRecorder()
				read(w, pipesRequest(t, http.MethodGet, "/v1/ops/pipes/top_pages", "top_pages", nil))
				assert.Equal(t, tt.wantStatus, w.Code, name)
				assert.Contains(t, w.Body.String(), tt.wantBody, name)
				testutil.AssertJSONErrorResponse(t, w)
			}
		})
	}
}

// The admin reads name their tenant in ?tenant=, parsed strictly: the store
// the pipes source receives is that tenant's, a query that does not parse is
// a 400 rather than a read of the default tenant, and a tenant that cannot be
// served gets the tenant routes' 404 or 503.
func TestPipesHandler_AdminReads_TenantParam(t *testing.T) {
	t.Parallel()
	tenants := nestedTenants(t, map[string]string{"0": fullConfig(100), "acme": fullConfig(100), "globex": `{"unknown_key": true}`})
	tests := []struct {
		name, query string
		wantStatus  int
		wantTenant  tenant.ID // on 200, whose store the source was handed
		wantBody    string
	}{
		{name: "no parameter is the default tenant", query: "", wantStatus: http.StatusOK, wantTenant: tenant.Default},
		{name: "named tenant", query: "tenant=acme", wantStatus: http.StatusOK, wantTenant: "acme"},
		{name: "explicit default tenant", query: "tenant=0", wantStatus: http.StatusOK, wantTenant: tenant.Default},
		{name: "an unrelated parameter changes nothing", query: "tenant=acme&pretty=1", wantStatus: http.StatusOK, wantTenant: "acme"},
		{name: "rejected tenant", query: "tenant=globex", wantStatus: http.StatusServiceUnavailable, wantBody: "tenant settings are invalid"},
		{name: "unknown tenant", query: "tenant=initech", wantStatus: http.StatusNotFound, wantBody: "unknown tenant: initech"},
		{name: "empty value is not absent", query: "tenant=", wantStatus: http.StatusBadRequest, wantBody: "invalid ?tenant: tenant id is empty"},
		{name: "repeated, even agreeing", query: "tenant=acme&tenant=acme", wantStatus: http.StatusBadRequest, wantBody: "sent more than once"},
		{name: "malformed id", query: "tenant=a.b", wantStatus: http.StatusBadRequest, wantBody: "invalid ?tenant"},
		{name: "semicolon pair", query: "tenant=acme;x=1", wantStatus: http.StatusBadRequest, wantBody: "invalid query string"},
		{name: "bad escape", query: "tenant=%zz", wantStatus: http.StatusBadRequest, wantBody: "invalid query string"},
		{name: "a malformed pair elsewhere refuses the read too", query: "tenant=acme&x=%zz", wantStatus: http.StatusBadRequest, wantBody: "invalid query string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, read := range []string{"list", "get"} {
				var handed *settings.Store
				h := NewPipesHandler(func(s *settings.Store) pipes.Source {
					handed = s
					return pipes.Static(&pipes.NamedQuery{Name: "top_pages", SQL: "SELECT 1"})
				}, nil, nil, nil, noTimeout)
				h.Tenants = tenants

				w := httptest.NewRecorder()
				if read == "list" {
					r := pipesRequest(t, http.MethodGet, "/v1/ops/pipes", "", nil)
					r.URL.RawQuery = tt.query
					h.List(w, r)
				} else {
					r := pipesRequest(t, http.MethodGet, "/v1/ops/pipes/top_pages", "top_pages", nil)
					r.URL.RawQuery = tt.query
					h.Get(w, r)
				}

				require.Equal(t, tt.wantStatus, w.Code, "%s: %s", read, w.Body.String())
				if tt.wantStatus != http.StatusOK {
					assert.Nil(t, handed, "%s: a refused read must not reach the pipes source", read)
					assert.Contains(t, w.Body.String(), tt.wantBody, read)
					testutil.AssertJSONErrorResponse(t, w)
					continue
				}
				want, ok := tenants.For(tt.wantTenant)
				require.True(t, ok)
				assert.Same(t, want, handed, read)
			}
		})
	}
}

func TestPipesHandler_List_Empty(t *testing.T) {
	t.Parallel()
	store := staticPipes()
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)
	h.Tenants = testTenants()

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/ops/pipes", "", nil)
	h.List(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var list []interface{}
	err := json.Unmarshal(w.Body.Bytes(), &list)
	assert.NoError(t, err)
	assert.Empty(t, list, "Response should be an empty list")
}

func TestPipesHandler_Execute_NotFound(t *testing.T) {
	t.Parallel()
	store := staticPipes()
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/nope/execute", "nope", nil)
	h.Execute(w, withTenant(r))

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "pipe not found")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_RoleAuthorization(t *testing.T) {
	t.Parallel()
	testutil.RunRoleMatrix(t, testutil.StandardRoleMatrix(), func(t *testing.T, tc testutil.RoleCase) *httptest.ResponseRecorder {
		store := staticPipes(
			&pipes.NamedQuery{
				Name:         "report",
				SQL:          "SELECT * FROM clicks",
				AllowedRoles: tc.AllowedRoles,
			},
		)
		// A real (non-nil) policy so the default admin role ("admin") is defined
		// and bypasses the allowlist, per the matrix. With a nil policy nobody is
		// admin (total lockout) — covered separately in internal/policy tests.
		h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

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
		safeHandle(h.Execute, w, withTenant(r))
		return w
	})
}

func TestPipesHandler_Execute_RestrictedPipe_EmptyRoleDenied(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{
			Name:         "admin_report",
			SQL:          "SELECT * FROM clicks",
			AllowedRoles: []string{"admin"},
		},
	)
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)

	w := httptest.NewRecorder()
	// No ContextKeyRole set, which simulates no token or a JWT without the role claim.
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/admin_report/execute", "admin_report", nil)

	safeHandle(h.Execute, w, withTenant(r))

	assert.Equal(t, http.StatusForbidden, w.Code,
		"pipe restricted to %v must reject a request with no role in context", []string{"admin"})
	assert.Contains(t, w.Body.String(), "forbidden")
	testutil.AssertJSONErrorResponse(t, w)
}

// TestPipesHandler_Execute_DefaultRoleGrantsAccess: a tokenless request (no role
// in context) resolves to the policy default_role, which is in AllowedRoles.
func TestPipesHandler_Execute_DefaultRoleGrantsAccess(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "report", SQL: "SELECT * FROM clicks", AllowedRoles: []string{"viewer"}},
	)
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)
	h.PolicySource = staticPolicy(&policy.Policy{DefaultRole: "viewer"})

	w := httptest.NewRecorder()
	// No role in context (tokenless / JWT without role claim).
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/report/execute", "report", nil)

	safeHandle(h.Execute, w, withTenant(r))

	assert.NotEqual(t, http.StatusForbidden, w.Code,
		"empty role should resolve to default_role 'viewer', which is in AllowedRoles")
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// TestPipesHandler_Execute_DefaultRoleNotInAllowedRolesDenied: the default_role
// is still gated by AllowedRoles — a default that isn't listed is denied.
func TestPipesHandler_Execute_DefaultRoleNotInAllowedRolesDenied(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "admin_report", SQL: "SELECT * FROM clicks", AllowedRoles: []string{"admin"}},
	)
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)
	h.PolicySource = staticPolicy(&policy.Policy{DefaultRole: "viewer"})

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/admin_report/execute", "admin_report", nil)

	safeHandle(h.Execute, w, withTenant(r))

	assert.Equal(t, http.StatusForbidden, w.Code,
		"default_role 'viewer' is not in AllowedRoles [admin] → denied")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_MissingParam(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{
			Name: "by_page",
			SQL:  "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{
				{Name: "page", Type: "string", Required: true},
			},
		},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

	w := httptest.NewRecorder()
	// No query params or body — missing "page".
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/by_page/execute", "by_page", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	h.Execute(w, withTenant(r))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing required parameter")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_ParamsFromQuery(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{
			Name: "by_page",
			SQL:  "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{
				{Name: "page", Type: "string", Required: true},
			},
		},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/pipes/by_page/execute?page=/home", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "by_page")
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	safeHandle(h.Execute, w, withTenant(r))

	// Should pass param binding — failing later at the unwired target.
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// TestPipesHandler_Execute_RequestBodyCap pins the control-plane body cap on a
// pipe's POST parameter body (#315). An oversized body returns 413 — the auth
// gate (admin) and pipe lookup pass, so the cap is what fires. maxRequestBytes
// is tiny so we don't allocate 1 MiB per run.
func TestPipesHandler_Execute_RequestBodyCap(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{
			Name:       "by_page",
			SQL:        "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{{Name: "page", Type: "string", Required: true}},
		},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)
	h.maxRequestBytes = 64

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/by_page/execute", "by_page", map[string]any{
		"page": strings.Repeat("x", 200),
	})
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	h.Execute(w, withTenant(r))

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code, "oversized body must 413")
	assert.Contains(t, w.Body.String(), "request body exceeded")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestPipesHandler_Execute_PostBodyParams(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{
			Name: "by_page",
			SQL:  "SELECT * FROM clicks WHERE page = {{page}}",
			Parameters: []pipes.ParamDef{
				{Name: "page", Type: "string", Required: true},
			},
		},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

	w := httptest.NewRecorder()
	body := map[string]any{"page": "/about"}
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/by_page/execute", "by_page", body)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	safeHandle(h.Execute, w, withTenant(r))

	// Should pass param binding — failing later at the unwired target.
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// TestPipesHandler_Execute_NoAllowedRoles_NonAdminDenied: a pipe with no
// allowed_roles authorizes nobody but the privileged built-ins. An ordinary
// authenticated caller (claims present, role "viewer") is rejected because the
// empty allowlist matches no role.
func TestPipesHandler_Execute_NoAllowedRoles_NonAdminDenied(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "open", SQL: "SELECT * FROM clicks"}, // no AllowedRoles
	)
	h := NewPipesHandler(store, nil, nil, nil, noTimeout)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/open/execute", "open", nil)
	ctx := auth.WithClaims(r.Context(), jwt.MapClaims{})
	ctx = auth.WithRole(ctx, "viewer")
	r = r.WithContext(ctx)

	safeHandle(h.Execute, w, withTenant(r))

	assert.Equal(t, http.StatusForbidden, w.Code,
		"a pipe with no allowed_roles must reject a non-admin role")
	testutil.AssertJSONErrorResponse(t, w)
}

// TestPipesHandler_Execute_ArrayParamBinds: an array body param renders into an
// IN list and passes binding (failing only later at the unwired ClickHouse
// target).
func TestPipesHandler_Execute_ArrayParamBinds(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{
			Name:       "by_ids",
			SQL:        "SELECT * FROM clicks WHERE id IN {{ids}}",
			Parameters: []pipes.ParamDef{{Name: "ids", Type: "array", Required: true}},
		},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

	w := httptest.NewRecorder()
	body := map[string]any{"ids": []any{"a", "b"}}
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/by_ids/execute", "by_ids", body)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	safeHandle(h.Execute, w, withTenant(r))

	// Binding succeeded — the only failure left is the unwired target, never a
	// 400.
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// TestPipesHandler_Execute_ObjectParamRejected: a JSON object has no scalar SQL
// form and is refused with a 400 before any query runs (#317).
func TestPipesHandler_Execute_ObjectParamRejected(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "by_col", SQL: "SELECT * FROM clicks WHERE col = {{p}}"},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

	w := httptest.NewRecorder()
	body := map[string]any{"p": map[string]any{"k": "v"}}
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/by_col/execute", "by_col", body)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	h.Execute(w, withTenant(r))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "unsupported parameter type object")
	testutil.AssertJSONErrorResponse(t, w)
}

// TestPipesHandler_Execute_NoAllowedRoles_AdminAllowed: the privileged built-in
// roles bypass the allowlist, so admin can run a pipe with no allowed_roles.
func TestPipesHandler_Execute_NoAllowedRoles_AdminAllowed(t *testing.T) {
	t.Parallel()
	store := staticPipes(
		&pipes.NamedQuery{Name: "open", SQL: "SELECT * FROM clicks"},
	)
	h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), nil, nil, noTimeout)

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/open/execute", "open", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))

	safeHandle(h.Execute, w, withTenant(r))

	assert.NotEqual(t, http.StatusForbidden, w.Code,
		"admin bypasses the allowlist on a pipe with no allowed_roles")
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// pipeCallAs runs the pipe name as the writer role and returns the recorder.
func pipeCallAs(t *testing.T, h *PipesHandler, name string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPost, "/v1/pipes/"+name, name, map[string]any{"msg": "hello"})
	h.Execute(w, withTenant(r.WithContext(auth.WithRole(r.Context(), "writer"))))
	return w
}

// writerPipesHandler serves queries to the writer role, through ch.
func writerPipesHandler(t *testing.T, ch *fakeCH, c cache.Cache, queries ...*pipes.NamedQuery) *PipesHandler {
	t.Helper()
	for _, q := range queries {
		q.AllowedRoles = []string{"writer"}
	}
	timeout := func(*settings.Store) time.Duration { return 5 * time.Second }
	h := NewPipesHandler(staticPipes(queries...), staticPolicy(&policy.Policy{}), ch.target, c, timeout)
	h.ch = ch.reader()
	return h
}

// #386: a pipe that writes executes on every call. Served from the cache, a
// repeat would answer 200 with the first call's `[]` and never reach
// ClickHouse — the write silently dropped.
func TestPipesHandler_Execute_MutationRunsEveryCall(t *testing.T) {
	t.Parallel()
	for name, sql := range map[string]string{
		"insert":           "INSERT INTO audit_log VALUES ({{msg}}, now())",
		"insert after cte": "WITH m AS (SELECT {{msg}} AS msg) INSERT INTO audit_log SELECT msg, now() FROM m",
		"alter delete":     "ALTER TABLE audit_log DELETE WHERE msg = {{msg}}",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l1, err := cache.NewLocal(1 << 20)
			require.NoError(t, err)
			t.Cleanup(func() { _ = l1.Close() })
			ch := &fakeCH{}
			h := writerPipesHandler(t, ch, l1, &pipes.NamedQuery{Name: "log", SQL: sql})

			for range 3 {
				w := pipeCallAs(t, h, "log")
				require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
				assert.Equal(t, "BYPASS", w.Header().Get("X-Cache"))
				assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
				assert.JSONEq(t, `[]`, w.Body.String())
				l1.Wait()
			}
			assert.Equal(t, int32(3), ch.writes.Load(), "every call must reach ClickHouse, not read-only")
			assert.Zero(t, ch.reads.Load())
		})
	}
}

// Identical mutation calls in flight together are each executed: coalescing
// them would run one write for all of them. Under synctest, Wait returns once
// every request is inside ClickHouse or parked on another's flight.
func TestPipesHandler_Execute_ConcurrentMutationsNotCoalesced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const calls = 3
		entered, gate := make(chan struct{}, calls), make(chan struct{})
		ch := gatedCH(entered, gate)
		h := writerPipesHandler(t, ch, nil, &pipes.NamedQuery{Name: "log", SQL: "INSERT INTO audit_log VALUES ({{msg}}, now())"})
		var wg sync.WaitGroup
		for range calls {
			wg.Go(func() {
				w := pipeCallAs(t, h, "log")
				assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			})
		}
		synctest.Wait()
		assert.Len(t, entered, calls, "writes in flight once every request is blocked")
		close(gate)
		wg.Wait()
		assert.Equal(t, int32(calls), ch.writes.Load())
	})
}

// A read pipe keeps its cache, including one whose table name starts with a
// write verb: the classifier reads the statement, not the words in it.
func TestPipesHandler_Execute_ReadPipeStaysCached(t *testing.T) {
	t.Parallel()
	l1, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l1.Close() })
	ch := &fakeCH{}
	h := writerPipesHandler(t, ch, l1, &pipes.NamedQuery{Name: "recent", SQL: "SELECT * FROM insert_log WHERE msg = {{msg}}"})

	for _, want := range []string{"MISS", "HIT", "HIT"} {
		w := pipeCallAs(t, h, "recent")
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
		assert.Equal(t, want, w.Header().Get("X-Cache"))
		l1.Wait()
	}
	assert.Equal(t, int32(1), ch.reads.Load())
	assert.Zero(t, ch.writes.Load())
}

// What a pipe sends ClickHouse: the bound SQL as the body, its query timeout
// as max_execution_time, and readonly=2 on a read — the statement IsMutation
// reads as a write is the one sent without it.
func TestPipesHandler_Execute_RequestSettings(t *testing.T) {
	t.Parallel()
	ch := &fakeCH{}
	h := writerPipesHandler(t, ch, nil,
		&pipes.NamedQuery{Name: "recent", SQL: "SELECT * FROM log WHERE msg = {{msg}}"},
		&pipes.NamedQuery{Name: "log", SQL: "INSERT INTO log VALUES ({{msg}})"},
	)

	require.Equal(t, http.StatusOK, pipeCallAs(t, h, "recent").Code)
	assert.Equal(t, "SELECT * FROM log WHERE msg = 'hello'", ch.sql())
	assert.Equal(t, "2", ch.setting("readonly"))
	assert.Equal(t, "5", ch.setting("max_execution_time"))
	assert.Empty(t, ch.setting("max_result_rows"), "a pipe carries no role caps")

	require.Equal(t, http.StatusOK, pipeCallAs(t, h, "log").Code)
	assert.Equal(t, "INSERT INTO log VALUES ('hello')", ch.sql())
	assert.False(t, ch.last().query.Has("readonly"))
	assert.Equal(t, "5", ch.setting("max_execution_time"))
}

// TestPipesHandler_Execute_ServesClickHouseBytes: a pipe's response is
// ClickHouse's own rendering, rows framed as an array.
func TestPipesHandler_Execute_ServesClickHouseBytes(t *testing.T) {
	t.Parallel()
	ch := &fakeCH{answer: answerRows("{\"page\":\"/a\",\"n\":1}\n{\"page\":\"/b\",\"n\":2}\n")}
	h := writerPipesHandler(t, ch, nil, &pipes.NamedQuery{Name: "top", SQL: "SELECT page, n FROM t"})
	w := pipeCallAs(t, h, "top")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, `[{"page":"/a","n":1},{"page":"/b","n":2}]`, w.Body.String())
}
