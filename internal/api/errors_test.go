package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSONError_SetsContentTypeAndStatus(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSONError(w, http.StatusBadRequest, "boom")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "boom", body["error"])
}

func TestWriteJSONError_EscapesSpecialCharacters(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSONError(w, http.StatusInternalServerError, `oops "quoted" \n`)

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, `oops "quoted" \n`, body["error"])
}

// warnBufLogger returns a WARN-level JSON logger that writes to the returned
// buffer. It's injected into the gate/handler under test (the way
// internal/policy/store_test.go injects one into NewStore), so reading a
// denial's structured WARN needs no process-global slog.SetDefault swap — which
// is why, unlike the old default-logger capture, these tests can run in parallel.
func warnBufLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})), &buf
}

// TestRequireAdmin_DenialLogsStructuredWarn pins the structured WARN emitted on
// an admin-gate denial: a concrete non-admin role, the route, the "not on the
// allowlist" reason, and gate=admin (so the denial is attributable to the admin
// check, which the route pattern alone can't convey).
func TestRequireAdmin_DenialLogsStructuredWarn(t *testing.T) {
	t.Parallel()
	logger, buf := warnBufLogger()
	handler := RequireAdmin(policy.NewMemoryStore(&policy.Policy{}), logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on a denied request")
	}))

	ctx := auth.WithRole(context.Background(), "viewer")
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/admin/query", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	out := buf.String()
	assert.Contains(t, out, `"level":"WARN"`, "a denial that logs nothing is the bug this guards against")
	assert.Contains(t, out, `"msg":"authorization denied"`)
	assert.Contains(t, out, `"reason":"role not in allowed roles"`)
	assert.Contains(t, out, `"role_observed":"viewer"`)
	assert.Contains(t, out, `"role_resolved":"viewer"`)
	assert.Contains(t, out, `"roles_allowed":null`, "the admin gate logs no explicit allowlist")
	assert.Contains(t, out, `"route":"/v1/admin/query"`)
	assert.Contains(t, out, `"method":"GET"`)
	assert.Contains(t, out, `"status":403`)
	assert.Contains(t, out, `"gate":"admin"`)
}

// TestRequireAdmin_EmptyRoleDenialLogsResolvedRole: a tokenless request maps to
// the policy default_role before the admin check, so role_observed is empty
// while role_resolved is the default — the signal that says "the public default
// role can't reach admin", not "the client sent the wrong role".
func TestRequireAdmin_EmptyRoleDenialLogsResolvedRole(t *testing.T) {
	t.Parallel()
	logger, buf := warnBufLogger()
	store := policy.NewMemoryStore(&policy.Policy{DefaultRole: "viewer"})
	handler := RequireAdmin(store, logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on a denied request")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/admin/query", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	out := buf.String()
	assert.Contains(t, out, `"msg":"authorization denied"`)
	assert.Contains(t, out, `"role_observed":""`, "no token / role claim was presented")
	assert.Contains(t, out, `"role_resolved":"viewer"`, "empty role resolved to default_role")
	assert.Contains(t, out, `"reason":"role not in allowed roles"`)
}

// TestRequireAdmin_InvalidTokenDenialLogsFailLoudReason: a present-but-invalid
// token fails loud — the WARN reason carries the token error and the status is
// 401, distinguishing it from an ordinary roleless 403. The admin gate logs no
// explicit allowlist, so roles_allowed is null.
func TestRequireAdmin_InvalidTokenDenialLogsFailLoudReason(t *testing.T) {
	t.Parallel()
	logger, buf := warnBufLogger()
	handler := RequireAdmin(nil, logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on a denied request")
	}))

	ctx := auth.WithAuthError(context.Background(), errors.New("token expired"))
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/admin/query", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	out := buf.String()
	assert.Contains(t, out, `"msg":"authorization denied"`)
	assert.Contains(t, out, `"reason":"token expired"`)
	assert.Contains(t, out, `"status":401`)
	assert.Contains(t, out, `"roles_allowed":null`)
}

// TestPipesHandler_Execute_DenialLogsAllowedRoles: a pipe denial logs the pipe's
// allowed_roles as roles_allowed and gate=pipe with the pipe name, so an
// operator who forgot to grant a role sees the exact set that would have let the
// caller through — and which pipe (the route pattern is /v1/pipes/{name}, so the
// concrete name isn't in the route field).
func TestPipesHandler_Execute_DenialLogsAllowedRoles(t *testing.T) {
	t.Parallel()
	logger, buf := warnBufLogger()
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "report", SQL: "SELECT * FROM clicks", AllowedRoles: []string{"analyst", "viewer"}},
	)
	h := NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), nil, nil, 0, logger)

	r := pipesRequest(t, http.MethodPost, "/v1/pipes/report/execute", "report", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "guest"))
	w := httptest.NewRecorder()
	h.Execute(w, r)
	require.Equal(t, http.StatusForbidden, w.Code)

	out := buf.String()
	assert.Contains(t, out, `"msg":"authorization denied"`)
	assert.Contains(t, out, `"role_observed":"guest"`)
	assert.Contains(t, out, `"roles_allowed":["analyst","viewer"]`)
	assert.Contains(t, out, `"reason":"role not in allowed roles"`)
	assert.Contains(t, out, `"method":"POST"`)
	assert.Contains(t, out, `"gate":"pipe"`)
	assert.Contains(t, out, `"pipe":"report"`)
}

// TestIngest_DenialLogsPolicyGate: the policy-evaluator denial carries
// gate=policy plus the table and action it evaluated, so a per-table policy
// denial is distinguishable from an admin-gate or pipe-allowlist denial — the
// /v1/ingest route pattern alone doesn't say which check failed, or on what.
func TestIngest_DenialLogsPolicyGate(t *testing.T) {
	t.Parallel()
	logger, buf := warnBufLogger()
	h := NewIngestHandler(testRegistry(), &testutil.MockPublisher{}, logger)
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {Select: map[string]policy.RolePermissions{"viewer": {}}}, // no insert for viewer
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	req = req.WithContext(auth.WithRole(req.Context(), "viewer"))
	w := httptest.NewRecorder()
	h.Handle(w, req)
	require.Equal(t, http.StatusForbidden, w.Code)

	out := buf.String()
	assert.Contains(t, out, `"msg":"authorization denied"`)
	assert.Contains(t, out, `"gate":"policy"`)
	assert.Contains(t, out, `"table":"clicks"`)
	assert.Contains(t, out, `"action":"insert"`)
	assert.Contains(t, out, `"role_resolved":"viewer"`)
}

// TestAuthzDenied_LogsChiRoutePattern: routed through the real mux, the WARN's
// route is the matched route template, not the raw path — low-cardinality and
// free of concrete path params. /v1/schema runs the admin gate (no /admin
// prefix), so gate=admin is what tells the operator which check denied it.
func TestAuthzDenied_LogsChiRoutePattern(t *testing.T) {
	t.Parallel()
	logger, buf := warnBufLogger()
	reg := discovery.NewSchemaRegistryFromMap(nil)
	router := NewRouter(Dependencies{
		Ingest:      NewIngestHandler(reg, &testutil.MockPublisher{}, logger),
		Query:       &QueryHandler{},
		SSE:         NewStreamHandler(stream.NewHub(nil, nil, nil), nil),
		Health:      &HealthHandler{},
		Schema:      NewSchemaHandler(reg),
		AuthMW:      func(next http.Handler) http.Handler { return next },
		PolicyStore: policy.NewMemoryStore(&policy.Policy{}),
		Logger:      logger,
	})

	ctx := auth.WithRole(context.Background(), "viewer")
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/schema", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	out := buf.String()
	assert.Contains(t, out, `"route":"/v1/schema"`, "route should be the chi pattern")
	assert.Contains(t, out, `"gate":"admin"`, "/v1/schema runs the admin gate, not a policy one")
}
