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

// captureWarnLogs swaps the default slog logger for a WARN-level JSON handler
// writing to a buffer, runs fn, restores the logger, and returns the parsed log
// records. Tests using it must NOT call t.Parallel(): the swap is process-global,
// and Go pauses parallel tests until the non-parallel ones finish, so a
// non-parallel test owns the default logger for the duration of fn.
func captureWarnLogs(t *testing.T, fn func()) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	fn()

	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal(line, &m))
		records = append(records, m)
	}
	return records
}

// findDenialLog returns the single "authorization denied" WARN record, failing
// if it's absent (a denial that logs nothing is the bug this guards against).
func findDenialLog(t *testing.T, records []map[string]any) map[string]any {
	t.Helper()
	for _, m := range records {
		if m["msg"] == "authorization denied" {
			assert.Equal(t, "WARN", m["level"], "denial must be logged at WARN")
			return m
		}
	}
	t.Fatal("expected an 'authorization denied' WARN log, got none")
	return nil
}

// TestRequireAdmin_DenialLogsStructuredWarn pins the structured WARN emitted on
// an admin-gate denial: a concrete non-admin role, the route, and the "not on
// the allowlist" reason.
func TestRequireAdmin_DenialLogsStructuredWarn(t *testing.T) {
	handler := RequireAdmin(policy.NewMemoryStore(&policy.Policy{}))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on a denied request")
	}))

	records := captureWarnLogs(t, func() {
		ctx := auth.WithRole(context.Background(), "viewer")
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/admin/query", nil)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	})

	d := findDenialLog(t, records)
	assert.Equal(t, "role not in allowed roles", d["reason"])
	assert.Equal(t, "viewer", d["role_observed"])
	assert.Equal(t, "viewer", d["role_resolved"])
	assert.Nil(t, d["roles_allowed"], "the admin gate logs no explicit allowlist")
	assert.Equal(t, "/v1/admin/query", d["route"])
	assert.Equal(t, http.MethodGet, d["method"])
	assert.Equal(t, float64(http.StatusForbidden), d["status"])
}

// TestRequireAdmin_EmptyRoleDenialLogsResolvedRole: a tokenless request maps to
// the policy default_role before the admin check, so role_observed is empty
// while role_resolved is the default — the signal that says "the public default
// role can't reach admin", not "the client sent the wrong role".
func TestRequireAdmin_EmptyRoleDenialLogsResolvedRole(t *testing.T) {
	store := policy.NewMemoryStore(&policy.Policy{DefaultRole: "viewer"})
	handler := RequireAdmin(store)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on a denied request")
	}))

	records := captureWarnLogs(t, func() {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/admin/query", nil)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	})

	d := findDenialLog(t, records)
	assert.Equal(t, "", d["role_observed"], "no token / role claim was presented")
	assert.Equal(t, "viewer", d["role_resolved"], "empty role resolved to default_role")
	assert.Equal(t, "role not in allowed roles", d["reason"])
}

// TestRequireAdmin_InvalidTokenDenialLogsFailLoudReason: a present-but-invalid
// token fails loud — the WARN reason carries the token error and the status is
// 401, distinguishing it from an ordinary roleless 403. The admin gate logs no
// explicit allowlist, so roles_allowed is absent.
func TestRequireAdmin_InvalidTokenDenialLogsFailLoudReason(t *testing.T) {
	handler := RequireAdmin(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on a denied request")
	}))

	records := captureWarnLogs(t, func() {
		ctx := auth.WithAuthError(context.Background(), errors.New("token expired"))
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/admin/query", nil)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	})

	d := findDenialLog(t, records)
	assert.Equal(t, "token expired", d["reason"])
	assert.Equal(t, float64(http.StatusUnauthorized), d["status"])
	assert.Nil(t, d["roles_allowed"])
}

// TestPipesHandler_Execute_DenialLogsAllowedRoles: a pipe denial logs the pipe's
// allowed_roles as roles_allowed, so an operator who forgot to grant a role sees
// the exact set that would have let the caller through.
func TestPipesHandler_Execute_DenialLogsAllowedRoles(t *testing.T) {
	store := pipes.NewMemoryStore(
		&pipes.NamedQuery{Name: "report", SQL: "SELECT * FROM clicks", AllowedRoles: []string{"analyst", "viewer"}},
	)
	h := NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), nil, nil, 0)

	records := captureWarnLogs(t, func() {
		r := pipesRequest(t, http.MethodPost, "/v1/pipes/report/execute", "report", nil)
		r = r.WithContext(auth.WithRole(r.Context(), "guest"))
		w := httptest.NewRecorder()
		h.Execute(w, r)
		require.Equal(t, http.StatusForbidden, w.Code)
	})

	d := findDenialLog(t, records)
	assert.Equal(t, "guest", d["role_observed"])
	assert.Equal(t, []any{"analyst", "viewer"}, d["roles_allowed"])
	assert.Equal(t, "role not in allowed roles", d["reason"])
	assert.Equal(t, http.MethodPost, d["method"])
}

// TestAuthzDenied_LogsChiRoutePattern: routed through the real mux, the WARN's
// route is the matched route template, not the raw path — low-cardinality and
// free of concrete path params.
func TestAuthzDenied_LogsChiRoutePattern(t *testing.T) {
	reg := discovery.NewSchemaRegistryFromMap(nil)
	router := NewRouter(Dependencies{
		Ingest:      NewIngestHandler(reg, &testutil.MockPublisher{}),
		Query:       &QueryHandler{},
		SSE:         NewSSEHandler(NewHub(), nil),
		WS:          NewWSHandler(NewHub(), nil, nil),
		Health:      &HealthHandler{},
		Schema:      NewSchemaHandler(reg),
		AuthMW:      func(next http.Handler) http.Handler { return next },
		PolicyStore: policy.NewMemoryStore(&policy.Policy{}),
	})

	records := captureWarnLogs(t, func() {
		ctx := auth.WithRole(context.Background(), "viewer")
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/schema", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code)
	})

	d := findDenialLog(t, records)
	assert.Equal(t, "/v1/schema", d["route"], "route should be the chi pattern")
}
