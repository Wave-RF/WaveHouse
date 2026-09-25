package app

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// Each role wires its own components and nothing else; the settings registry,
// the MQ, the coordinator, the reload triggers and a listener are every
// process's. New does not validate, so the embedded MQ stands in for the
// shared one a split needs (config.Validate refuses it outside tests).
func TestNew_RolesChooseTheComponents(t *testing.T) {
	for _, tc := range []struct {
		name  string
		roles []config.Role
		want  []string
	}{
		{"every role", config.AllRoles(), []string{
			"clickhouse", "schema discovery", "dedupe", "mq", "cache", "coord",
			"sweeper", "hub bridge", "keepalive", "ingest worker",
			"auth", "sighup", "settings watcher", "http server",
		}},
		{"api", []config.Role{config.RoleAPI}, []string{
			"clickhouse", "schema discovery", "dedupe", "mq", "cache", "coord",
			"hub bridge", "keepalive",
			"auth", "sighup", "settings watcher", "http server",
		}},
		{"ingest", []config.Role{config.RoleIngest}, []string{
			"clickhouse", "mq", "cache", "coord",
			"ingest worker",
			"sighup", "settings watcher", "http server",
		}},
		{"sweeper", []config.Role{config.RoleSweeper}, []string{
			"mq", "coord",
			"sweeper",
			"sighup", "settings watcher", "http server",
		}},
		{"ingest and sweeper", []config.Role{config.RoleSweeper, config.RoleIngest}, []string{
			"clickhouse", "mq", "cache", "coord",
			"sweeper", "ingest worker",
			"sighup", "settings watcher", "http server",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, writeSettings(t, nil))
			cfg.Roles = tc.roles
			a := newApp(t, cfg, Options{})
			assert.Equal(t, tc.want, componentNames(a))
		})
	}
}

func TestNew_RefusesAConfigWithoutRoles(t *testing.T) {
	guardGlobals(t)
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Roles = nil
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorContains(t, err, "roles is empty")
}

func hs256(t *testing.T, secret, role string) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"role": role, "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(secret))
	require.NoError(t, err)
	return tok
}

// A process without the api role serves the ops-only router: the probes,
// /version and the settings reload, which takes the operator key alone — no
// token verifier runs there, so even an admin token the API would admit is
// refused. Every tenant route, and the rest of /v1/ops, is not there.
func TestNew_OpsOnlyRouter(t *testing.T) {
	dir := writeSettings(t, nil)
	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileRoles), []byte(`{"roles": ["admin"]}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FilePolicies), []byte(`{"admin_role": "admin", "tables": {}}`), 0o600))
	cfg := testConfig(t, dir)
	cfg.Auth.OperatorKey = "unit-test-operator-key"
	admin := hs256(t, cfg.Auth.JWTSecret, "admin")
	do := func(a *App, method, target, header, value string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
		if header != "" {
			req.Header.Set(header, value)
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return rec
	}

	full := newApp(t, cfg, Options{})
	require.Equal(t, http.StatusOK, do(full, http.MethodPost, "/v1/ops/settings/reload", "Authorization", "Bearer "+admin).Code,
		"the API admits the admin token")

	sweeperCfg := *cfg
	sweeperCfg.Roles = []config.Role{config.RoleSweeper}
	a := newApp(t, &sweeperCfg, Options{})

	for _, path := range []string{"/livez", "/readyz", "/healthz", "/version"} {
		assert.Equal(t, http.StatusOK, do(a, http.MethodGet, path, "", "").Code, path)
	}

	reload := "/v1/ops/settings/reload"
	rec := do(a, http.MethodPost, reload, "X-Operator-Key", cfg.Auth.OperatorKey)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"adopted":true`)
	assert.Equal(t, http.StatusUnauthorized, do(a, http.MethodPost, reload, "Authorization", "Bearer "+admin).Code,
		"no verifier runs without the api role, so the token is invalid here")
	assert.Equal(t, http.StatusForbidden, do(a, http.MethodPost, reload, "", "").Code)
	assert.Equal(t, http.StatusForbidden, do(a, http.MethodPost, reload, "X-Operator-Key", "wrong").Code)

	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/v1/ingest"},
		{http.MethodGet, "/v1/stream"},
		{http.MethodPost, "/v1/query"},
		{http.MethodGet, "/v1/health"},
		{http.MethodGet, "/v1/pipes/nope"},
		{http.MethodGet, "/v1/ops/schema"},
		{http.MethodPost, "/v1/ops/query"},
		{http.MethodGet, "/v1/ops/dlq/stats"},
	} {
		assert.Equal(t, http.StatusNotFound, do(a, route.method, route.path, "X-Operator-Key", cfg.Auth.OperatorKey).Code, route.path)
	}
}

// Readiness follows what the process has: an ingest process is ready when a
// ClickHouse pool answers (here none can), a sweeper-only one once booted.
// Liveness never waits on schema discovery, which only the API runs.
func TestNew_OpsOnlyReadiness(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Roles = []config.Role{config.RoleIngest}
	a := newApp(t, cfg, Options{})
	assert.Equal(t, http.StatusOK, get(t, a.Handler(), "/livez").Code)
	rec := get(t, a.Handler(), "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Nil(t, a.Registry(), "no schema registry without the api role")
}

func TestNew_OpsOnlyPrometheusInline(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Roles = []config.Role{config.RoleIngest}
	cfg.Prometheus = config.Prometheus{Enabled: true, Path: "/metrics"}
	a := newApp(t, cfg, Options{})
	rec := get(t, a.Handler(), "/metrics")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "wavehouse_")
}

// A sweeper-only process serves its listener and runs the sweeper under the
// lease, as the all-roles one does.
func TestRun_SweeperOnlyProcess(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Roles = []config.Role{config.RoleSweeper}
	a := newApp(t, cfg, Options{Listener: ln})
	rival := a.coord.(*coord.Local).Peer()

	baseURL, stop := runApp(t, a, ln)
	status, _ := httpGet(t, baseURL+"/readyz")
	assert.Equal(t, http.StatusOK, status)
	require.Eventually(t, func() bool {
		term, err := rival.TryAcquire(t.Context(), sweeperLease)
		if err == nil {
			require.NoError(t, term.Resign(t.Context()))
		}
		return errors.Is(err, coord.ErrHeld)
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, stop())
}
