package app

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/dedupe/dedupetest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// None of these tests run in parallel: New installs a process-wide default
// logger and, with Prometheus on, the global OTel providers. Every app boots
// against a ClickHouse address that is guaranteed closed, so the boot-time
// schema discovery fails fast and deterministically (the degraded path) no
// matter what is listening on the developer's :9000.

// closedAddr returns a 127.0.0.1 address nothing listens on: bind an
// ephemeral port, then release it.
func closedAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// closedPort is closedAddr's port number, for the config fields that take one.
func closedPort(t *testing.T) int {
	t.Helper()
	tcp, err := net.ResolveTCPAddr("tcp", closedAddr(t))
	require.NoError(t, err)
	return tcp.Port
}

// writeSettings materializes the embedded seed with the ClickHouse address
// pointed at a closed port, then applies patch to config.json's top-level
// blocks (each value re-marshaled whole).
func writeSettings(t *testing.T, patch map[string]any) string {
	t.Helper()
	files, err := settings.Seed()
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(files[settings.FileConfig], &doc))
	var ch map[string]any
	require.NoError(t, json.Unmarshal(doc["clickhouse"], &ch))
	ch["addr"] = closedAddr(t)
	doc["clickhouse"], err = json.Marshal(ch)
	require.NoError(t, err)
	for key, val := range patch {
		doc[key], err = json.Marshal(val)
		require.NoError(t, err)
	}
	files[settings.FileConfig], err = json.Marshal(doc)
	require.NoError(t, err)

	dir := t.TempDir()
	for name, data := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	}
	return dir
}

func testConfig(t *testing.T, settingsDir string) *config.Config {
	t.Helper()
	return &config.Config{
		DataDir:  t.TempDir(),
		Server:   config.Server{Port: closedPort(t), ShutdownTimeout: 2},
		MQ:       config.MQ{Backend: config.MQEmbedded},
		Cache:    config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:   config.Dedupe{Backend: config.DedupePebble},
		Coord:    config.Coord{Backend: config.CoordLocal},
		Auth:     config.Auth{JWTSecret: "unit-test-secret"},
		Settings: config.Settings{Dir: settingsDir},
	}
}

// guardGlobals restores the process-wide state New may replace: the default
// logger, and the OTel providers when Prometheus/OTLP is on.
func guardGlobals(t *testing.T) {
	t.Helper()
	savedLogger := slog.Default()
	savedProp := otel.GetTextMapPropagator()
	savedTP := otel.GetTracerProvider()
	savedMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		slog.SetDefault(savedLogger)
		otel.SetTextMapPropagator(savedProp)
		otel.SetTracerProvider(savedTP)
		otel.SetMeterProvider(savedMP)
	})
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newApp(t *testing.T, cfg *config.Config, opts Options) *App {
	t.Helper()
	guardGlobals(t)
	opts.Config = cfg
	a, err := New(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, a.Close(context.Background())) })
	return a
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return rec
}

func TestNew_DegradedBootServesDiagnostics(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, nil))
	a := newApp(t, cfg, Options{Build: BuildInfo{Version: "1.2.3", GitCommit: "abc", BuildTime: "now"}})

	rec := get(t, a.Handler(), "/livez")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "boot without ClickHouse is degraded, not fatal")
	assert.Contains(t, rec.Body.String(), "schema discovery")

	rec = get(t, a.Handler(), "/version")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"version":"1.2.3"`)

	assert.NotNil(t, a.Registry())
	assert.NotNil(t, a.MQ())
	assert.NoError(t, a.Close(context.Background()))
	assert.NoError(t, a.Close(context.Background()), "Close is idempotent")
}

// A flat directory's registry holds the default tenant only: no header and
// "0" reach the route, any other well-formed id is a 404, a malformed one a
// 400, and the ops tree never looks at the header. A 503 is the handler's own
// answer — boot is degraded without ClickHouse — so it proves the tenant
// resolved.
func TestNew_TenantHeaderResolvesAgainstTheRegistry(t *testing.T) {
	a := newApp(t, testConfig(t, writeSettings(t, nil)), Options{})

	tests := []struct {
		name, path, header string
		want               int
	}{
		{name: "no header", path: "/v1/health", want: http.StatusServiceUnavailable},
		{name: "default tenant", path: "/v1/health", header: "0", want: http.StatusServiceUnavailable},
		{name: "unknown tenant", path: "/v1/health", header: "acme", want: http.StatusNotFound},
		{name: "malformed tenant", path: "/v1/health", header: "a.b", want: http.StatusBadRequest},
		{name: "ops ignores the header", path: "/v1/ops/schema", header: "acme", want: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil)
			if tt.header != "" {
				req.Header.Set(tenant.Header, tt.header)
			}
			rec := httptest.NewRecorder()
			a.Handler().ServeHTTP(rec, req)
			assert.Equal(t, tt.want, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// A tenant the registry cannot resolve must not read as "DLQ off": off is what
// lets the ingest worker ack and drop a message it cannot read, so the miss
// parks instead. The other async getters degrade to their zero value.
func TestAsyncGetters_RegistryMiss(t *testing.T) {
	t.Parallel()
	tenants := settings.NewRegistry(&settings.Store{})
	unknown := tenant.ID("acme")

	assert.True(t, dlqFor(tenants)(unknown, "events"), "an unknown tenant's failed rows park on the DLQ")
	assert.Zero(t, perTenant(tenants, (*settings.Store).GapWindow)(unknown))
}

func TestNew_DedupeFollowsSettings(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "disabled leaves the store closed", enabled: false},
		{name: "enabled opens the store", enabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeSettings(t, map[string]any{"dedupe": map[string]any{
				"enabled": tt.enabled, "id_field": "event_id", "require_id": false, "tables": map[string]any{},
			}})
			cfg := testConfig(t, dir)
			a := newApp(t, cfg, Options{})
			assert.Equal(t, tt.enabled, a.dedup.For(tenant.Default).Open())
			_, err := os.Stat(filepath.Join(cfg.DataDir, "pebble"))
			assert.Equal(t, tt.enabled, err == nil, "the Pebble instance exists iff dedupe is on: a server with dedupe off opens nothing")
		})
	}
}

// rewriteSettings replaces config.json in dir with the seed plus patch,
// the same way an operator edit lands before a reload.
func rewriteSettings(t *testing.T, dir string, patch map[string]any) {
	t.Helper()
	src := writeSettings(t, patch)
	data, err := os.ReadFile(filepath.Join(src, settings.FileConfig)) //nolint:gosec // G304: path rooted in t.TempDir()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileConfig), data, 0o600)) //nolint:gosec // G703: dir is a t.TempDir() from writeSettings
}

func TestReload_DrivesTheRegisteredHooks(t *testing.T) {
	// Hooks are registered in New and fired by the reload triggers Run
	// starts; a direct Reload stands in for any of the three triggers and
	// pins that the relocated hooks still follow the adopted document.
	dir := writeSettings(t, map[string]any{"mq": map[string]any{"max_bytes_gb": 1}})
	cfg := testConfig(t, dir)
	a := newApp(t, cfg, Options{})
	dedup := a.dedup.For(tenant.Default)
	require.False(t, dedup.Open())
	require.Equal(t, int64(1<<30), a.mq.MaxBytes(tenant.Default))

	rewriteSettings(t, dir, map[string]any{
		"dedupe": map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}},
		"mq":     map[string]any{"max_bytes_gb": 2},
	})
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.True(t, dedup.Open(), "dedupe hook opened the store")
	// How the budget is split across the tenant's queues is internal/mq's to test.
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes(tenant.Default), "mq hook applied the new byte budget")

	rewriteSettings(t, dir, map[string]any{"mq": map[string]any{"max_bytes_gb": 2}})
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.False(t, dedup.Open(), "dedupe hook closed the store")
}

// writeNestedSettings materializes a nested settings directory: tenant folder
// → the patch writeSettings applies to that tenant's config.json.
func writeNestedSettings(t *testing.T, tenants map[string]map[string]any) string {
	t.Helper()
	root := t.TempDir()
	for folder, patch := range tenants {
		require.NoError(t, os.Rename(writeSettings(t, patch), filepath.Join(root, folder)))
	}
	return root
}

// invalidQuery is a config.json patch Validate rejects.
var invalidQuery = map[string]any{"query": map[string]any{"default_max_rows": -1, "timestamp_bucket_seconds": 60}}

func componentNames(a *App) []string {
	names := make([]string, len(a.components))
	for i, c := range a.components {
		names[i] = c.name
	}
	return names
}

// A nested directory boots without a 0 folder and with a rejected tenant:
// each tenant route answers for the tenant its header names, and only the
// rejected one is refused. GET /v1/pipes/{name} stands in for the tenant
// routes because its own 404 needs no ClickHouse, so it proves the handler
// ran with a resolved store.
func TestNew_NestedDirectory(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil, "broken": invalidQuery})
	a := newApp(t, testConfig(t, root), Options{})

	assert.NotContains(t, componentNames(a), "settings watcher", "a nested directory is reloaded by whoever wrote the folder, never watched")
	flat := newApp(t, testConfig(t, writeSettings(t, nil)), Options{})
	assert.Contains(t, componentNames(flat), "settings watcher")

	pipe := func(header string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/pipes/nope", nil)
		if header != "" {
			req.Header.Set(tenant.Header, header)
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return rec
	}
	tests := []struct {
		name, header string
		wantStatus   int
		wantBody     string
	}{
		{name: "served tenant", header: "acme", wantStatus: http.StatusNotFound, wantBody: "pipe not found"},
		{name: "the tenant beside it", header: "globex", wantStatus: http.StatusNotFound, wantBody: "pipe not found"},
		{name: "rejected tenant", header: "broken", wantStatus: http.StatusServiceUnavailable, wantBody: "tenant settings are invalid"},
		{name: "unknown tenant", header: "initech", wantStatus: http.StatusNotFound, wantBody: "unknown tenant: initech"},
		{name: "no header is tenant 0, which this directory does not hold", wantStatus: http.StatusNotFound, wantBody: "unknown tenant: 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := pipe(tt.header)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}

	// Fixing the folder and reloading is the recovery, with no restart.
	rewriteSettings(t, filepath.Join(root, "broken"), nil)
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Contains(t, pipe("broken").Body.String(), "pipe not found")
}

// The control plane's loop over a nested directory, through the real wiring:
// write a tenant's folder, then reload that tenant with the operator key. An
// admin token cannot — over a nested directory the ops routes reach every
// tenant, so the operator key alone opens them.
func TestNew_NestedOperatorReloadsOneTenant(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil, "broken": invalidQuery})
	cfg := testConfig(t, root)
	cfg.Auth.OperatorKey = "unit-test-operator-key"
	a := newApp(t, cfg, Options{})

	// header is one name and value, or none.
	do := func(method, target string, header ...string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
		if len(header) == 2 {
			req.Header.Set(header[0], header[1])
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return rec
	}
	operator := []string{"X-Operator-Key", cfg.Auth.OperatorKey}
	as := func(id string) []string { return []string{tenant.Header, id} }

	require.Equal(t, http.StatusServiceUnavailable, do(http.MethodGet, "/v1/pipes/nope", as("broken")...).Code)

	rewriteSettings(t, filepath.Join(root, "broken"), nil)
	rec := do(http.MethodPost, "/v1/ops/settings/reload?tenant=broken", operator...)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"adopted":true`)
	rec = do(http.MethodGet, "/v1/pipes/nope", as("broken")...)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "pipe not found", "the tenant is served again, with no restart")

	// The admin reads name their tenant the same way; without it they read
	// tenant 0, which this directory does not hold.
	assert.Equal(t, http.StatusOK, do(http.MethodGet, "/v1/ops/pipes?tenant=acme", operator...).Code)
	assert.Equal(t, http.StatusNotFound, do(http.MethodGet, "/v1/ops/pipes", operator...).Code)
	assert.Equal(t, http.StatusNotFound, do(http.MethodPost, "/v1/ops/settings/reload?tenant=initech", operator...).Code)
	assert.Equal(t, http.StatusForbidden, do(http.MethodPost, "/v1/ops/settings/reload?tenant=acme").Code)
}

// Over a nested directory the operator key is the only credential the ops
// tree takes, and nothing watches the directory — so booting one without the
// key leaves SIGHUP as the only reload, and boot says so rather than repeat
// the flat directory's recovery advice.
func TestNew_NestedWithoutAnOperatorKeyWarnsTheOpsTreeIsClosed(t *testing.T) {
	const warning = "no caller can reach those routes"
	boot := func(t *testing.T, settingsDir, operatorKey string) string {
		t.Helper()
		guardGlobals(t)
		logs := logtest.Capture(t, slog.LevelWarn)
		cfg := testConfig(t, settingsDir)
		cfg.Auth.OperatorKey = operatorKey
		a, err := New(t.Context(), Options{Config: cfg})
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, a.Close(context.Background())) })
		return logs.String()
	}

	t.Run("nested, no key", func(t *testing.T) {
		assert.Contains(t, boot(t, writeNestedSettings(t, map[string]map[string]any{"acme": nil}), ""), warning)
	})
	t.Run("nested, key set", func(t *testing.T) {
		assert.NotContains(t, boot(t, writeNestedSettings(t, map[string]map[string]any{"acme": nil}), "unit-test-operator-key"), warning)
	})
	t.Run("flat, no key", func(t *testing.T) {
		logs := boot(t, writeSettings(t, nil), "")
		assert.NotContains(t, logs, warning)
		assert.Contains(t, logs, "no auth.operator_key set")
	})
}

// A tenant's queue budget and dedupe store follow its own folder alone:
// another tenant's reload moves neither. A rejected or removed folder keeps
// its tenant's queue at the budget it last had — removing never touches
// data — while its dedupe store closes, its seen ids kept. CORS is read per
// request, so a lost 0 folder is felt at once on the routes that read tenant
// 0's list.
func TestReload_NestedHooksFollowEachTenant(t *testing.T) {
	dedupeOn := map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}}
	grown := map[string]any{"dedupe": dedupeOn, "mq": map[string]any{"max_bytes_gb": 2}}
	root := writeNestedSettings(t, map[string]map[string]any{
		"0":    {"mq": map[string]any{"max_bytes_gb": 1}},
		"acme": {"mq": map[string]any{"max_bytes_gb": 1}},
	})
	a := newApp(t, testConfig(t, root), Options{})
	dedup0, dedupAcme := a.dedup.For(tenant.Default), a.dedup.For("acme")
	require.False(t, dedup0.Open())
	require.False(t, dedupAcme.Open())
	require.Equal(t, int64(1<<30), a.mq.MaxBytes(tenant.Default))
	require.Equal(t, int64(1<<30), a.mq.MaxBytes("acme"), "each served tenant's queue opens at boot at its own budget")
	// CORS is per tenant, not a hook's: a tenant route reads its own tenant's
	// list and the exempt routes tenant 0's (the seed's ["*"] in every folder
	// here), both through the registry, so a lost 0 folder is felt at once.
	allowOrigin := func(path string, id ...string) string {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		req.Header.Set("Origin", "https://app.example.com")
		for _, id := range id {
			req.Header.Set(tenant.Header, id)
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return rec.Header().Get("Access-Control-Allow-Origin")
	}
	require.Equal(t, "*", allowOrigin("/version"))
	require.Equal(t, "*", allowOrigin("/v1/health", "acme"))

	rewriteSettings(t, filepath.Join(root, "acme"), grown)
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.True(t, dedupAcme.Open(), "acme's dedupe switch opens acme's own store")
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes("acme"), "acme's budget resizes acme's own queue")
	assert.False(t, dedup0.Open(), "and moves nothing of tenant 0's")
	assert.Equal(t, int64(1<<30), a.mq.MaxBytes(tenant.Default))

	rewriteSettings(t, filepath.Join(root, "0"), grown)
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.True(t, dedup0.Open())
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes(tenant.Default))

	rewriteSettings(t, filepath.Join(root, "0"), invalidQuery)
	_, adopted = a.tenants.Reload("test")
	require.False(t, adopted)
	assert.False(t, dedup0.Open(), "a rejected 0 folder closes tenant 0's own store, which answers no request now")
	assert.True(t, dedupAcme.Open(), "and costs acme nothing")
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes(tenant.Default), "tenant 0's queue is kept at the budget it last had")
	assert.Empty(t, allowOrigin("/version"), "the exempt routes read tenant 0 through the registry, which is no longer serving it")
	assert.Equal(t, "*", allowOrigin("/v1/health", "acme"), "acme's own routes keep acme's list")

	// A removed 0 folder is the same: the registry forgets the tenant, and
	// its queue stays at the budget it last had.
	require.NoError(t, os.RemoveAll(filepath.Join(root, "0")))
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	_, known := a.tenants.Resolve(tenant.Default)
	require.False(t, known)
	assert.False(t, dedup0.Open())
	assert.True(t, dedupAcme.Open())
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes(tenant.Default))
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes("acme"))
	assert.Empty(t, allowOrigin("/version"))
	assert.Equal(t, "*", allowOrigin("/v1/health", "acme"))
}

// One dedupe store per tenant over a nested directory (#583 story 7), each
// following its own tenant's switch, and every one a share of the one Pebble
// instance at data_dir/pebble (story 3): opened by its folder's adoption,
// closed — its seen ids kept — once the folder is rejected or removed, and
// reopened over the same seen ids when the folder is back. The instance is
// open while some tenant's store is, and Close releases it.
func TestNew_NestedDedupeStoreFollowsEachTenant(t *testing.T) {
	dedupeOn := map[string]any{"dedupe": map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}}}
	root := writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn, "globex": nil, "broken": invalidQuery})
	cfg := testConfig(t, root)
	a := newApp(t, cfg, Options{})
	ctx := t.Context()

	acme, globex := a.dedup.For("acme"), a.dedup.For("globex")
	assert.True(t, acme.Open(), "acme's switch is on")
	assert.False(t, globex.Open(), "globex's is off")
	assert.DirExists(t, filepath.Join(cfg.DataDir, "pebble"), "one instance for every tenant")
	for _, id := range []string{"acme", "globex", "broken"} {
		assert.NoDirExists(t, filepath.Join(cfg.DataDir, id), "and no directory of a tenant's own")
	}
	dup, err := dedupetest.Mark(ctx, acme, eventKey)
	require.NoError(t, err)
	assert.False(t, dup)

	rewriteSettings(t, filepath.Join(root, "globex"), dedupeOn)
	a.tenants.Reload("test")
	assert.True(t, globex.Open(), "globex's reload opens globex's store")
	dup, err = dedupetest.Mark(ctx, globex, eventKey)
	require.NoError(t, err)
	assert.False(t, dup, "an id acme has seen is new to globex")

	// A reload that rejects globex's folder alone closes globex's store, and
	// nothing else: a rejected tenant answers no request, so it holds no store.
	rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
	_, adopted, known := a.tenants.ReloadTenant("globex", "test")
	require.True(t, known)
	require.False(t, adopted)
	assert.False(t, globex.Open())
	assert.True(t, acme.Open())

	// A removed folder closes its store; with none left open, the instance
	// closes too, its files staying where they are.
	require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
	a.tenants.Reload("test")
	_, known = a.tenants.Resolve("acme")
	require.False(t, known)
	assert.False(t, acme.Open(), "a tenant the registry no longer holds has its store closed")
	assert.Nil(t, a.dedupeStats(), "no store open: the instance is closed")
	entries, err := os.ReadDir(filepath.Join(cfg.DataDir, "pebble"))
	require.NoError(t, err)
	assert.NotEmpty(t, entries)

	// Restoring the folder restores the tenant, seen ids included.
	require.NoError(t, os.Rename(writeSettings(t, dedupeOn), filepath.Join(root, "acme")))
	a.tenants.Reload("test")
	restored := a.dedup.For("acme")
	assert.True(t, restored.Open())
	dup, err = dedupetest.Mark(ctx, restored, eventKey)
	require.NoError(t, err)
	assert.True(t, dup, "an id seen before the folder was removed is still a duplicate")

	require.NoError(t, a.Close(context.Background()))
	assert.False(t, restored.Open(), "Close releases every open store")
}

// Validate refuses a backend no layer has a case for, so the switch's default
// is reached only by a Config built by hand; it must refuse boot, not wire
// nothing.
func TestNew_RefusesALayerWithoutABackend(t *testing.T) {
	for _, tc := range []struct {
		key   string
		unset func(*config.Config)
	}{
		{"dedupe.backend", func(c *config.Config) { c.Dedupe.Backend = "" }},
		{"mq.backend", func(c *config.Config) { c.MQ.Backend = "" }},
		{"cache.backend", func(c *config.Config) { c.Cache.Backend = "" }},
	} {
		t.Run(tc.key, func(t *testing.T) {
			guardGlobals(t)
			cfg := testConfig(t, writeSettings(t, nil))
			tc.unset(cfg)
			_, err := New(t.Context(), Options{Config: cfg})
			require.ErrorContains(t, err, tc.key+` "" has no wiring`)
		})
	}
}

// A Pebble instance that cannot open follows the registry's own rule for the
// shape: a flat directory refuses boot, like every other store, and a nested
// one fails closed for every tenant with dedupe on, since they share the
// instance — their ingest answers 500 until a reload or a restart opens it —
// while the process, and every tenant with dedupe off, carries on.
func TestNew_DedupeOpenFailure(t *testing.T) {
	dedupeOn := map[string]any{"dedupe": map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}}}
	// A regular file where the instance's directory should be is what Pebble
	// refuses to open.
	block := func(t *testing.T, dataDir string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "pebble"), nil, 0o600))
	}
	t.Run("flat refuses boot", func(t *testing.T) {
		guardGlobals(t)
		cfg := testConfig(t, writeSettings(t, dedupeOn))
		block(t, cfg.DataDir)
		_, err := New(t.Context(), Options{Config: cfg})
		require.ErrorContains(t, err, "dedupe open")
	})
	t.Run("nested fails closed", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn, "globex": dedupeOn, "initech": nil})
		cfg := testConfig(t, root)
		block(t, cfg.DataDir)
		a := newApp(t, cfg, Options{})
		for _, id := range []tenant.ID{"acme", "globex"} {
			store := a.dedup.For(id)
			assert.False(t, store.Open())
			_, err := dedupetest.Mark(t.Context(), store, eventKey)
			require.ErrorIs(t, err, dedupe.ErrUnavailable, "%s: switched on but not open, so its ingest fails closed", id)
		}
		_, err := dedupetest.Mark(t.Context(), a.dedup.For("initech"), eventKey)
		require.ErrorIs(t, err, dedupe.ErrDisabled, "a tenant with dedupe off is as it would be anyway")
	})
}

// A tenant's queue the MQ cannot open follows the registry's rule for the
// shape, as the dedupe store does: a flat directory refuses boot, and a nested
// one boots with that tenant's queue closed and every other tenant's open.
// The obstacle is a regular file where the embedded server keeps a stream's
// store — the embedded implementation's layout, which this test takes on to
// force the failure, as TestNew_DedupeOpenFailure does Pebble's. The failed
// open clears it, so the next publish opens the queue: each one tries again.
func TestNew_QueueOpenFailure(t *testing.T) {
	block := func(t *testing.T, dataDir, stream string) {
		t.Helper()
		p := filepath.Join(dataDir, "nats", "jetstream", "$G", "streams", stream)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o750))
		require.NoError(t, os.WriteFile(p, nil, 0o600))
	}
	t.Run("flat refuses boot", func(t *testing.T) {
		guardGlobals(t)
		cfg := testConfig(t, writeSettings(t, nil))
		block(t, cfg.DataDir, "DLQ_0")
		_, err := New(t.Context(), Options{Config: cfg})
		require.ErrorContains(t, err, "mq open")
	})
	t.Run("nested costs the tenant alone", func(t *testing.T) {
		cfg := testConfig(t, writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil}))
		block(t, cfg.DataDir, "DLQ_acme")
		a := newApp(t, cfg, Options{})
		assert.Zero(t, a.mq.MaxBytes("acme"), "acme's queue did not open")
		assert.Equal(t, int64(50<<30), a.mq.MaxBytes("globex"), "and costs globex nothing")

		require.NoError(t, a.MQ().Publish(t.Context(), mq.Topic{Tenant: "acme", Table: "t"}, []byte("x")))
		assert.Equal(t, int64(50<<30), a.mq.MaxBytes("acme"), "a publish opened it at acme's budget")
	})
}

// Boot opens each served tenant's queue under New's context, as New's doc
// says: a stop signaled during boot is not held up by one open per tenant.
func TestNew_QueueSetupHonorsTheBootContext(t *testing.T) {
	guardGlobals(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := New(ctx, Options{Config: testConfig(t, writeSettings(t, nil))})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "mq open")
}

// The tenants on the writer's ClickHouse address and database read the same
// tables, so an insert invalidates a table's cached results under every one
// of them — whatever their user, so across pools — and under no tenant on
// another address or database: the cache the worker is handed fans the
// namespaces out by the pools' sharing rule. The writer's own tenant is
// bumped even when it is on no pool.
func TestSharedTables_InvalidatesTheTenantsSharingTheTables(t *testing.T) {
	t.Parallel()
	sharing := map[tenant.ID][]tenant.ID{tenant.Default: {tenant.Default, "acme", "globex"}, "initech": {"initech"}}
	mock := &testutil.MockCache{}
	c := sharedTables{Cache: mock, sharing: func(id tenant.ID) []tenant.ID { return sharing[id] }}

	n, err := c.Invalidate(t.Context(), []cache.Namespace{
		{Tenant: tenant.Default, Table: "events"},
		{Tenant: tenant.Default, Table: "events", Scope: "org_1"},
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(6), n)
	assert.ElementsMatch(t, []cache.Namespace{
		{Tenant: tenant.Default, Table: "events"},
		{Tenant: tenant.Default, Table: "events", Scope: "org_1"},
		{Tenant: "acme", Table: "events"},
		{Tenant: "acme", Table: "events", Scope: "org_1"},
		{Tenant: "globex", Table: "events"},
		{Tenant: "globex", Table: "events", Scope: "org_1"},
	}, mock.GetNamespaces(), "the batch's tenant and the ones sharing its tables; initech reads another database")

	mock = &testutil.MockCache{}
	c = sharedTables{Cache: mock, sharing: func(tenant.ID) []tenant.ID { return nil }}
	_, err = c.Invalidate(t.Context(), []cache.Namespace{{Tenant: "orphan", Table: "events"}})
	require.NoError(t, err)
	assert.Equal(t, []cache.Namespace{{Tenant: "orphan", Table: "events"}}, mock.GetNamespaces(), "a writer on no pool still bumps its own")
}

// A tenant back on a pool after an absence — its folder rejected, then
// repaired; removed, then restored — was out of the fan-out while away, so
// the wiring orphans its table-keyed cache as it comes back; a tenant that stayed
// is never touched, and a reload that changes nothing bumps nobody.
func TestReload_ReadmittedTenantCacheIsOrphaned(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil})
	a := newApp(t, testConfig(t, root), Options{})
	// The hooks read a.cache at reload time: a recording cache from here on.
	mock := &testutil.MockCache{}
	a.cache = mock

	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Empty(t, mock.GetTenants(), "nothing readmitted, nothing orphaned")

	rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
	a.tenants.Reload("test")
	assert.Empty(t, mock.GetTenants(), "a rejection releases; it orphans nothing yet")
	rewriteSettings(t, filepath.Join(root, "globex"), nil)
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, []tenant.ID{"globex"}, mock.GetTenants(), "repaired: back on a pool, its cache orphaned")

	require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
	a.tenants.Reload("test")
	require.NoError(t, os.Rename(writeSettings(t, nil), filepath.Join(root, "acme")))
	a.tenants.Reload("test")
	assert.Equal(t, []tenant.ID{"globex", "acme"}, mock.GetTenants(), "restored: the same")
}

// keepalive is a config.json patch setting the stream block's keepalive pair.
func keepalive(interval, buckets int) map[string]any {
	return map[string]any{"stream": map[string]any{"keepalive_interval": interval, "keepalive_buckets": buckets, "gap_window_minutes": 15}}
}

// One wheel keeps every tenant's streams alive, so it runs at the shortest
// keepalive_interval among the tenants being served — an upper bound the
// longer ones are inside of (#597 tracks honoring each tenant's own). A flat
// directory's single tenant gets exactly its own pair.
func TestShortestKeepalive(t *testing.T) {
	open := func(t *testing.T, dir string) *settings.Registry {
		t.Helper()
		guardGlobals(t)
		tenants, findings := settings.Open(dir)
		require.NotNil(t, tenants, "findings: %v", findings)
		return tenants
	}

	t.Run("flat directory", func(t *testing.T) {
		period, buckets := shortestKeepalive(open(t, writeSettings(t, keepalive(45, 5))))
		assert.Equal(t, 45*time.Second, period)
		assert.Equal(t, 5, buckets)
	})

	t.Run("nested directory", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": keepalive(30, 3), "globex": keepalive(10, 2), "initech": keepalive(10, 7)})
		tenants := open(t, root)
		period, buckets := shortestKeepalive(tenants)
		assert.Equal(t, 10*time.Second, period)
		assert.Equal(t, 2, buckets, "tenants tied on the interval resolve to the first in id order")

		// A rejected tenant is not being served, so its setting is not weighed.
		rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
		rewriteSettings(t, filepath.Join(root, "initech"), invalidQuery)
		tenants.Reload("test")
		period, buckets = shortestKeepalive(tenants)
		assert.Equal(t, 30*time.Second, period)
		assert.Equal(t, 3, buckets)
	})

	t.Run("no tenant served falls back to the wheel's defaults", func(t *testing.T) {
		period, buckets := shortestKeepalive(open(t, writeNestedSettings(t, map[string]map[string]any{"acme": invalidQuery})))
		assert.Zero(t, period)
		assert.Zero(t, buckets)
	})
}

func gapWindow(minutes int) map[string]any {
	return map[string]any{"stream": map[string]any{"keepalive_interval": 30, "keepalive_buckets": 3, "gap_window_minutes": minutes}}
}

// Each tenant keeps its own stream.gap_window_minutes, since each has a queue
// of its own — a rejected tenant the window its folder last had, so its
// clients resume once the folder is fixed, and everything while that window
// is unknown. A removed tenant is not named and keeps no history
// (mq.Purger.PurgeAcked). A flat directory's single tenant gets exactly its
// own window.
func TestGapWindows(t *testing.T) {
	open := func(t *testing.T, dir string) *settings.Registry {
		t.Helper()
		guardGlobals(t)
		tenants, findings := settings.Open(dir)
		require.NotNil(t, tenants, "findings: %v", findings)
		return tenants
	}

	t.Run("flat directory", func(t *testing.T) {
		assert.Equal(t, map[tenant.ID]time.Duration{tenant.Default: 45 * time.Minute}, gapWindows(open(t, writeSettings(t, gapWindow(45)))))
	})

	t.Run("nested directory", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": gapWindow(15), "globex": gapWindow(60), "initech": gapWindow(30)})
		tenants := open(t, root)
		assert.Equal(t, map[tenant.ID]time.Duration{"acme": 15 * time.Minute, "globex": 60 * time.Minute, "initech": 30 * time.Minute}, gapWindows(tenants))

		rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
		tenants.Reload("test")
		assert.Equal(t, map[tenant.ID]time.Duration{"acme": 15 * time.Minute, "globex": 60 * time.Minute, "initech": 30 * time.Minute}, gapWindows(tenants),
			"a rejected tenant keeps the window its folder last had")

		require.NoError(t, os.RemoveAll(filepath.Join(root, "globex")))
		tenants.Reload("test")
		assert.Equal(t, map[tenant.ID]time.Duration{"acme": 15 * time.Minute, "initech": 30 * time.Minute}, gapWindows(tenants),
			"a removed tenant keeps none")
	})

	t.Run("a folder rejected since boot keeps everything", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": invalidQuery})
		tenants := open(t, root)
		assert.Equal(t, map[tenant.ID]time.Duration{"acme": keepEverything}, gapWindows(tenants))

		rewriteSettings(t, filepath.Join(root, "acme"), gapWindow(15))
		tenants.Reload("test")
		assert.Equal(t, map[tenant.ID]time.Duration{"acme": 15 * time.Minute}, gapWindows(tenants),
			"its own window once its folder validates")
	})
}

// A finding about a nested directory itself — a loose file beside the tenant
// folders — refuses boot, like an invalid flat directory.
func TestNew_NestedLooseFileRefusesBoot(t *testing.T) {
	guardGlobals(t)
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil})
	require.NoError(t, os.WriteFile(filepath.Join(root, "notes.txt"), []byte("scratch"), 0o600))
	a, err := New(t.Context(), Options{Config: testConfig(t, root)})
	require.Error(t, err)
	assert.Nil(t, a)
	assert.Contains(t, err.Error(), "settings directory")
}

func TestNew_RefusesInvalidSettingsDirectory(t *testing.T) {
	guardGlobals(t)
	cfg := testConfig(t, t.TempDir()) // empty: every required file is missing
	a, err := New(t.Context(), Options{Config: cfg})
	require.Error(t, err)
	assert.Nil(t, a)
	assert.Contains(t, err.Error(), "settings directory")
}

// jwksServer serves one Ed25519 verification key under kid and returns the
// signer that pairs with it — one tenant's identity provider — and a count
// of the fetches it answered.
func jwksServer(t *testing.T, kid string) (*httptest.Server, ed25519.PrivateKey, *atomic.Int32) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	body, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid,
		"x": base64.RawURLEncoding.EncodeToString(pub),
	}}})
	require.NoError(t, err)
	var fetches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, priv, &fetches
}

// signRole issues a token for role, signed by priv under kid.
func signRole(t *testing.T, priv ed25519.PrivateKey, kid, role string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"role": role, "exp": jwt.NewNumericDate(time.Now().Add(time.Hour))})
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(priv)
	require.NoError(t, err)
	return signed
}

// authPatch is a config.json patch pointing the tenant's verifier at jwksURL.
func authPatch(jwksURL string) map[string]any {
	return map[string]any{"auth": map[string]any{"jwks_url": jwksURL, "role_claim": "role"}}
}

// analystPipe gives the settings directory at dir one pipe, `p`, that the
// analyst role may run: the one tenant route whose answer tells a token that
// verified (the query runs, and fails against the closed ClickHouse) from
// one that did not (401, the fail-loud denial) without a ClickHouse.
func analystPipe(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileRoles), []byte(`{"roles": ["analyst"]}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FilePipes), []byte(`{"pipes": [{"name": "p", "sql": "SELECT 1", "allowed_roles": ["analyst"]}]}`), 0o600))
}

// A boot that fails after stores are open releases them, so the same data_dir
// boots again: here the MQ refuses its directory (a regular file in its
// place) once the dedupe store is already open, and a second New on the same
// data_dir must find the Pebble lock released.
func TestNew_LateBootFailureReleasesEverything(t *testing.T) {
	guardGlobals(t)
	dir := writeSettings(t, map[string]any{"dedupe": map[string]any{
		"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{},
	}})
	cfg := testConfig(t, dir)
	natsDir := filepath.Join(cfg.DataDir, "nats")
	require.NoError(t, os.WriteFile(natsDir, []byte("not a directory"), 0o600))
	a, err := New(t.Context(), Options{Config: cfg})
	require.Error(t, err)
	assert.Nil(t, a)
	assert.Contains(t, err.Error(), "mq open")

	require.NoError(t, os.Remove(natsDir))
	a, err = New(t.Context(), Options{Config: cfg})
	require.NoError(t, err, "the stores opened before the failure were released")
	assert.True(t, a.dedup.For(tenant.Default).Open())
	assert.NoError(t, a.Close(context.Background()))
}

// An unreachable JWKS endpoint no longer refuses boot: the tenant's verifier
// is in place, fail-closed, so a token of the wrong family is refused (401)
// and one that could not be checked is turned away to retry (503) rather
// than evaluated under the default_role, and the process serves everything
// else.
func TestNew_UnreachableJWKSBootsFailClosed(t *testing.T) {
	dir := writeSettings(t, authPatch("http://"+closedAddr(t)+"/jwks.json"))
	analystPipe(t, dir)
	started := time.Now()
	a := newApp(t, testConfig(t, dir), Options{})
	assert.Less(t, time.Since(started), 5*time.Second, "boot must not wait on the endpoint")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/pipes/p", nil)
	// Signed with the boot secret: the HMAC family, which a JWKS tenant never
	// accepts, fetched or not.
	req.Header.Set("Authorization", "Bearer "+testutil.MakeJWT(t, map[string]any{"role": "analyst"}))
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "invalid token")

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/pipes/p", nil)
	req.Header.Set("Authorization", "Bearer "+signRole(t, priv, "k1", "analyst"))
	rec = httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
	assert.Equal(t, "30", rec.Header().Get("Retry-After"))
}

// Each tenant verifies tokens with its own folder's auth block, through the
// real wiring: a token acme's identity provider issued runs acme's pipe and
// is refused under globex's header, and pointing acme's folder at another
// provider and reloading it swaps acme's verifier alone.
func TestNew_VerifierPerTenant(t *testing.T) {
	acme, acmeKey, _ := jwksServer(t, "acme-1")
	globex, globexKey, globexFetches := jwksServer(t, "globex-1")
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":   authPatch(acme.URL),
		"globex": authPatch(globex.URL),
	})
	analystPipe(t, filepath.Join(root, "acme"))
	analystPipe(t, filepath.Join(root, "globex"))
	cfg := testConfig(t, root)
	cfg.Auth.OperatorKey = "unit-test-operator-key"
	a := newApp(t, cfg, Options{})

	pipe := func(id, token string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/pipes/p", nil)
		req.Header.Set(tenant.Header, id)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	// verified reports whether the token passed the pipe's role gate: the
	// query then runs and fails against the closed ClickHouse, never the
	// 401 of a refused token or the 503 of a verifier still fetching.
	verified := func(id, token string) bool {
		code := pipe(id, token)
		return code != http.StatusUnauthorized && code != http.StatusServiceUnavailable
	}
	eventuallyVerified := func(id, token string) {
		t.Helper()
		require.Eventually(t, func() bool { return verified(id, token) }, 5*time.Second, 10*time.Millisecond,
			"%s's token never verified under %s: the key set is fetched off the boot path", id, id)
	}
	acmeToken := signRole(t, acmeKey, "acme-1", "analyst")
	globexToken := signRole(t, globexKey, "globex-1", "analyst")
	eventuallyVerified("acme", acmeToken)
	eventuallyVerified("globex", globexToken)
	assert.False(t, verified("globex", acmeToken), "acme's token is refused under globex's header")
	assert.False(t, verified("acme", globexToken))
	assert.False(t, verified("acme", testutil.MakeJWT(t, map[string]any{"role": "analyst"})), "the boot secret's HMAC family never verifies under a JWKS tenant")

	// acme moves to globex's provider; globex's folder is untouched.
	rewriteSettings(t, filepath.Join(root, "acme"), authPatch(globex.URL))
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/settings/reload?tenant=acme", nil)
	req.Header.Set("X-Operator-Key", cfg.Auth.OperatorKey)
	a.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	eventuallyVerified("acme", globexToken)
	assert.False(t, verified("acme", acmeToken), "the swap is unconditional: acme's old provider is gone")
	assert.True(t, verified("globex", globexToken))
	assert.False(t, verified("globex", acmeToken))

	// A reload that rejects globex's folder drops its verifier with it — the
	// hooks run on a reload that adopts nothing — and the fixed folder gets
	// a fresh one, fetched again.
	rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/settings/reload?tenant=globex", nil)
	req.Header.Set("X-Operator-Key", cfg.Auth.OperatorKey)
	a.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, "body: %s", rec.Body.String())
	assert.Equal(t, http.StatusServiceUnavailable, pipe("globex", globexToken), "a rejected tenant is not served")
	before := globexFetches.Load()
	rewriteSettings(t, filepath.Join(root, "globex"), authPatch(globex.URL))
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/settings/reload?tenant=globex", nil)
	req.Header.Set("X-Operator-Key", cfg.Auth.OperatorKey)
	a.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	eventuallyVerified("globex", globexToken)
	assert.Greater(t, globexFetches.Load(), before, "the fixed folder got a fresh verifier, fetched again")
}

func TestNew_PrometheusInlineMountsOnRouter(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Prometheus = config.Prometheus{Enabled: true, Path: "/metrics"}
	a := newApp(t, cfg, Options{})

	rec := get(t, a.Handler(), "/metrics")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "wavehouse_", "the private registry serves the process's own instruments")
}

// runApp starts Run on a harness listener and returns the base URL plus a
// stop that cancels Run and reports how it returned.
func runApp(t *testing.T, a *App, ln net.Listener) (baseURL string, stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	stop = func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("Run did not return after cancel")
		}
	}
	return "http://" + ln.Addr().String(), stop
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func TestRun_ServesUntilCancelled(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := testConfig(t, writeSettings(t, nil))
	a := newApp(t, cfg, Options{Listener: ln})

	baseURL, stop := runApp(t, a, ln)
	status, body := httpGet(t, baseURL+"/livez")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "schema discovery")

	assert.NoError(t, stop(), "a cancelled Run is a clean stop")

	// Between Run returning and Close, SIGHUP must still be captured: its
	// default disposition is terminate, so if the registration had been
	// released at the start of the stop this signal would kill the test
	// binary. Sent to the process itself; the sighup loop has already
	// returned, so the signal is simply discarded.
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGHUP))
	time.Sleep(50 * time.Millisecond)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/livez", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	assert.Error(t, err, "the listener is closed after Run returns")
}

func TestRun_PrometheusSidecar(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Prometheus = config.Prometheus{Enabled: true, Path: "/metrics", Port: closedPort(t)}
	a := newApp(t, cfg, Options{Listener: ln})

	baseURL, stop := runApp(t, a, ln)
	rec := get(t, a.Handler(), "/metrics")
	assert.Equal(t, http.StatusNotFound, rec.Code, "dedicated port: nothing mounted on the API router")

	sidecar := fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.Prometheus.Port)
	var status int
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, sidecar, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		status = resp.StatusCode
		return true
	}, 5*time.Second, 20*time.Millisecond, "sidecar never came up")
	assert.Equal(t, http.StatusOK, status)

	status, _ = httpGet(t, baseURL+"/version")
	assert.Equal(t, http.StatusOK, status)
	assert.NoError(t, stop())
}

func TestRun_ListenFailureStopsEverything(t *testing.T) {
	// Hold the wildcard address the server binds (":port"), not loopback: a
	// process already on the port holds the same address, and macOS allows a
	// wildcard bind while only 127.0.0.1:port is held (Linux refuses both),
	// which turned this test into a hang there.
	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	defer func() { _ = taken.Close() }()
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.Server.Port = taken.Addr().(*net.TCPAddr).Port
	a := newApp(t, cfg, Options{})

	// Bounded so a bind that unexpectedly succeeds fails the assertion below
	// (a cancelled Run returns nil) instead of blocking until the package
	// timeout kills the binary with no output.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = a.Run(ctx)
	require.Error(t, err, "boot must fail immediately when the port is taken")
	assert.True(t, strings.HasPrefix(err.Error(), "http server: "), "the failing component names itself: %v", err)
	// Eventually: AfterFunc runs stopCancel on its own goroutine, which Run
	// does not join, so the cancel may land a beat after Run returns.
	assert.Eventually(t, func() bool { return a.stopCtx.Err() != nil }, time.Second, time.Millisecond,
		"a component failure begins the stop, so a reload mid-hook gives up too")
}

// dyingConsumerBroker is the real broker, except that the ingest worker's
// consumer reports that delivery ended shortly after it starts — what a
// deleted durable or a closed MQ connection looks like from the worker.
type dyingConsumerBroker struct {
	mq.Broker
	reason error
}

func (b dyingConsumerBroker) CreateConsumer(context.Context, mq.ConsumerConfig) (mq.Consumer, error) {
	return dyingConsumer{reason: b.reason}, nil
}

type dyingConsumer struct{ reason error }

func (c dyingConsumer) Consume(func(*mq.Message), int) (func(), <-chan error, error) {
	failed := make(chan error, 1)
	failed <- c.reason
	return func() {}, failed, nil
}

func TestRun_DeadIngestWorkerStopsEverything(t *testing.T) {
	a := newApp(t, testConfig(t, writeSettings(t, nil)), Options{})
	// The worker takes its consumer from a.mq when Run starts it.
	a.mq = dyingConsumerBroker{Broker: a.mq, reason: fmt.Errorf("%w: consumer deleted", mq.ErrDeliveryEnded)}

	// Bounded so a worker failure that goes unnoticed fails the assertion
	// below (a cancelled Run returns nil) instead of serving forever — which
	// is exactly the silent stall this guards against.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := a.Run(ctx)
	require.ErrorIs(t, err, mq.ErrDeliveryEnded, "an API that accepts events nothing writes must not keep running")
	assert.True(t, strings.HasPrefix(err.Error(), "ingest worker: "), "the failing component names itself: %v", err)
}

func TestClose_AbandonsAStuckCloseAtTheDeadline(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	a := &App{}
	a.stopCtx, a.stopCancel = context.WithCancel(context.Background())
	// Wired first, so it is released last — after the stuck one.
	earlierReleased := false
	a.add(component{name: "earlier", close: func(context.Context) error {
		earlierReleased = true
		return nil
	}})
	a.add(component{name: "stuck", close: func(context.Context) error {
		<-release // ignores its context, like a local store's Close
		return nil
	}})
	a.add(component{name: "fine", close: func(context.Context) error { return nil }})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := a.Close(ctx)
	assert.Less(t, time.Since(started), time.Second, "the release budget bounds a close that ignores it")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stuck: abandoned at the release deadline")
	assert.Contains(t, err.Error(), "earlier: not released, budget spent")
	assert.False(t, earlierReleased, "a close after the abandoned one would overlap it and break the reverse order")
	assert.NotContains(t, err.Error(), "fine")
}

// openStream opens GET /v1/stream?table=events for tenant id ("" sends no
// header) and returns its body once the ": connected" preamble arrives.
func openStream(t *testing.T, baseURL, id string) io.Reader {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/v1/stream?table=events", nil)
	require.NoError(t, err)
	if id != "" {
		req.Header.Set(tenant.Header, id)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
	buf := make([]byte, 64)
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	require.Contains(t, string(buf[:n]), ": connected", "the stream is open")
	return resp.Body
}

// endsCleanly fails unless the server ends the stream within a second, and
// with the clean end of the response rather than a broken connection.
func endsCleanly(t *testing.T, stream io.Reader) {
	t.Helper()
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(stream)
		read <- err
	}()
	select {
	case err := <-read:
		assert.NoError(t, err, "the server ended the stream cleanly")
	case <-time.After(time.Second):
		t.Fatal("the stream is still open")
	}
}

// A stream is a connection to close, not work to drain: the stop ends every
// open one at once rather than waiting out server.shutdown_timeout. A reload
// that stops serving a tenant — its folder rejected or removed — ends that
// tenant's streams the same way, and no other tenant's; the client's
// reconnect then meets the tenant's 503 or 404. A flat directory never stops
// serving tenant 0, so a reload it rejects leaves the stream open.
func TestRun_StopEndsOpenStreams(t *testing.T) {
	start := func(t *testing.T, settingsDir string) (a *App, baseURL string, stop func() error) {
		t.Helper()
		var lc net.ListenConfig
		ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		a = newApp(t, testConfig(t, settingsDir), Options{Listener: ln})
		baseURL, stop = runApp(t, a, ln)
		return a, baseURL, stop
	}
	// registered waits for tenant id's stream to join the hub: openStream
	// returns at the ": connected" preamble, which the handler writes first.
	registered := func(t *testing.T, a *App, id tenant.ID) {
		t.Helper()
		topic := mq.Topic{Tenant: id, Table: "events"}
		require.Eventually(t, func() bool { return a.hub.Len(topic) == 1 }, 5*time.Second, 5*time.Millisecond)
	}

	t.Run("the stop", func(t *testing.T) {
		_, baseURL, stop := start(t, writeSettings(t, nil))
		resp := openStream(t, baseURL, "")
		started := time.Now()
		assert.NoError(t, stop())
		assert.Less(t, time.Since(started), time.Second, "the open stream held the stop for the drain budget")
		endsCleanly(t, resp)
	})

	t.Run("a reload that stops serving the tenant", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil})
		a, baseURL, stop := start(t, root)
		defer func() { assert.NoError(t, stop()) }()
		reconnect := func(id string) int {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/v1/stream?table=events", nil)
			require.NoError(t, err)
			req.Header.Set(tenant.Header, id)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_ = resp.Body.Close()
			return resp.StatusCode
		}
		acme, globex := openStream(t, baseURL, "acme"), openStream(t, baseURL, "globex")
		registered(t, a, "acme")
		registered(t, a, "globex")

		rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
		_, adopted, known := a.tenants.ReloadTenant("globex", "test")
		require.True(t, known)
		require.False(t, adopted)
		endsCleanly(t, globex)
		assert.Equal(t, 1, a.hub.Len(mq.Topic{Tenant: "acme", Table: "events"}), "acme's stream stays open")
		assert.Equal(t, http.StatusServiceUnavailable, reconnect("globex"), "rejected: retried until its folder is fixed")

		require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
		a.tenants.Reload("test")
		endsCleanly(t, acme)
		assert.Equal(t, http.StatusNotFound, reconnect("acme"), "removed: the client stops")
	})

	t.Run("a reload the flat directory rejects", func(t *testing.T) {
		dir := writeSettings(t, nil)
		a, baseURL, stop := start(t, dir)
		resp := openStream(t, baseURL, "")
		registered(t, a, tenant.Default)
		rewriteSettings(t, dir, invalidQuery)
		_, adopted := a.tenants.Reload("test")
		require.False(t, adopted)
		// An evicted stream leaves the hub only once its handler returns.
		assert.Never(t, func() bool { return a.hub.Len(mq.Topic{Tenant: tenant.Default, Table: "events"}) == 0 },
			200*time.Millisecond, 10*time.Millisecond, "tenant 0 keeps its previous settings, and its stream")
		assert.NoError(t, stop())
		endsCleanly(t, resp)
	})
}

// poolSettings is a config.json patch: the seed's clickhouse block pointed
// at addr with the native pool sized to open.
func poolSettings(addr string, open int) map[string]any {
	return map[string]any{"clickhouse": map[string]any{
		"addr": addr, "http_port": 8123, "http_scheme": "http", "database": "default", "username": "default", "query_timeout": 30,
		"tls":     map[string]any{"enabled": false, "ca_file": "", "cert_file": "", "key_file": "", "insecure_skip_verify": false, "server_name": ""},
		"headers": map[string]any{}, "max_open_conns": open, "max_idle_conns": 5,
	}}
}

// TestNew_RefusesAPoolAboveTheCeiling: clickhouse.max_total_conns is boot
// config and the settings pool must fit under it, so an impossible pair
// refuses to boot naming both numbers (#530).
func TestNew_RefusesAPoolAboveTheCeiling(t *testing.T) {
	guardGlobals(t)
	cfg := testConfig(t, writeSettings(t, poolSettings(closedAddr(t), 10)))
	cfg.ClickHouse.MaxTotalConns = 4
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorContains(t, err, "clickhouse.max_open_conns 10")
	require.ErrorContains(t, err, "clickhouse.max_total_conns 4")
}

// TestReload_PoolAboveTheCeilingKeepsTheConnection: a reload that raises the
// pool above the ceiling is refused whole — the connection keeps its wiring,
// not just its size — and the next reload that fits applies.
func TestReload_PoolAboveTheCeilingKeepsTheConnection(t *testing.T) {
	boot, moved := closedAddr(t), "127.0.0.1:9"
	dir := writeSettings(t, poolSettings(boot, 10))
	cfg := testConfig(t, dir)
	cfg.ClickHouse.MaxTotalConns = 10
	a := newApp(t, cfg, Options{})
	require.Equal(t, boot, a.pools.For(tenant.Default).Identity().Addr)

	logs := logtest.Capture(t, slog.LevelError)
	refused := poolSettings(moved, 20)
	refused["clickhouse"].(map[string]any)["database"] = "moved_db"
	rewriteSettings(t, dir, refused)
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, boot, a.pools.For(tenant.Default).Identity().Addr, "the refused reload leaves the connection as it was")
	_, database := a.discoverySource(tenant.Default)()
	assert.Equal(t, a.pools.For(tenant.Default).Identity().Database, database, "discovery reads the kept pool's database")
	assert.NotEqual(t, "moved_db", database, "not the adopted document's")
	assert.Contains(t, logs.String(), "clickhouse pools reconciled in part")
	assert.Contains(t, logs.String(), "clickhouse.max_open_conns 20")
	assert.Contains(t, logs.String(), "tenant 0 keeps its previous pool")

	rewriteSettings(t, dir, poolSettings(moved, 10))
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, moved, a.pools.For(tenant.Default).Identity().Addr, "the next reload that fits applies")
}

// chSettings is a config.json patch: the seed's clickhouse block pointed at
// addr as user, with the native pool sized to open.
func chSettings(addr, user string, open int) map[string]any {
	p := poolSettings(addr, open)
	p["clickhouse"].(map[string]any)["username"] = user
	return p
}

// A nested directory gets one pool per tuple among its tenants (#583 story
// 6): tenants naming the same address, database, user and tls share one
// Manager, sized to their largest ask; a tenant naming another gets its own.
// A reload that changes one sharer's username moves that tenant to a pool of
// its own and leaves the other on the very same Manager, resized to its own
// ask — the worked example of the story.
func TestNew_NestedPoolsFollowEachTenantsTuple(t *testing.T) {
	shared, other := closedAddr(t), closedAddr(t)
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":    chSettings(shared, "default", 10),
		"globex":  chSettings(shared, "default", 20),
		"initech": chSettings(other, "default", 10),
	})
	a := newApp(t, testConfig(t, root), Options{})

	acme, globex, initech := a.pools.For("acme"), a.pools.For("globex"), a.pools.For("initech")
	require.NotNil(t, acme)
	assert.Same(t, acme, globex, "one tuple, one pool")
	assert.NotSame(t, acme, initech)
	assert.Equal(t, 20, acme.Sizes().MaxOpenConns, "the largest ask among the sharers")
	assert.Equal(t, []tenant.ID{"acme", "globex"}, a.pools.SharingTables("acme"))
	assert.NotNil(t, a.discoveries.For("acme"))
	assert.NotNil(t, a.discoveries.For("initech"))
	assert.NotSame(t, a.discoveries.For("acme"), a.discoveries.For("globex"), "one registry per tenant, shared pool or not")

	rewriteSettings(t, filepath.Join(root, "globex"), chSettings(shared, "reporting", 20))
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Same(t, acme, a.pools.For("acme"), "acme keeps its Manager")
	assert.NotSame(t, acme, a.pools.For("globex"), "globex moved to a pool of its own")
	assert.Equal(t, "reporting", a.pools.For("globex").Identity().Username)
	assert.Equal(t, 10, acme.Sizes().MaxOpenConns, "acme's pool shrank to acme's ask")
	assert.Same(t, initech, a.pools.For("initech"))
	assert.Equal(t, []tenant.ID{"acme", "globex"}, a.pools.SharingTables("acme"), "same address and database: still the same tables")
}

// A nested directory's pools must fit the ceiling together: boot is refused
// naming the sum and the ceiling, like a flat directory's one pool.
func TestNew_NestedRefusesPoolsAboveTheCeiling(t *testing.T) {
	guardGlobals(t)
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":   chSettings(closedAddr(t), "default", 10),
		"globex": chSettings(closedAddr(t), "default", 10),
	})
	cfg := testConfig(t, root)
	cfg.ClickHouse.MaxTotalConns = 15
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorContains(t, err, "clickhouse.max_open_conns 10")
	require.ErrorContains(t, err, "at 20, above clickhouse.max_total_conns 15")
}

// A reload whose new tenant's pool would put the pools over the ceiling
// leaves that tenant on no pool — it fails closed, its schema never
// discovered — and the open pools untouched; the next reload that frees the
// budget opens it.
func TestReload_CeilingRefusesAThirdTupleThenOpensIt(t *testing.T) {
	a1, a2, a3 := closedAddr(t), closedAddr(t), closedAddr(t)
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":   chSettings(a1, "default", 10),
		"globex": chSettings(a2, "default", 10),
	})
	cfg := testConfig(t, root)
	cfg.ClickHouse.MaxTotalConns = 25
	a := newApp(t, cfg, Options{})
	acme, globex := a.pools.For("acme"), a.pools.For("globex")

	logs := logtest.Capture(t, slog.LevelError)
	require.NoError(t, os.Rename(writeSettings(t, chSettings(a3, "default", 10)), filepath.Join(root, "initech")))
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Nil(t, a.pools.For("initech"), "not opened")
	assert.Contains(t, logs.String(), "clickhouse pools reconciled in part")
	assert.Contains(t, logs.String(), "not opened for tenant initech")
	assert.Contains(t, logs.String(), "at 30, above clickhouse.max_total_conns 25")
	assert.Same(t, acme, a.pools.For("acme"))
	assert.Same(t, globex, a.pools.For("globex"))

	// The tenant is served — its settings are fine — but fails closed on
	// its ClickHouse side: no pool, so no discovery, so no table is known.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ingest?table=clicks", strings.NewReader(`{"page": "/"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(tenant.Header, "initech")
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
	assert.Equal(t, "5", rec.Header().Get("Retry-After"))

	rewriteSettings(t, filepath.Join(root, "acme"), chSettings(a1, "default", 5))
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	require.NotNil(t, a.pools.For("initech"), "opened once a sharer made room")
	assert.Same(t, acme, a.pools.For("acme"))
	assert.Equal(t, 5, acme.Sizes().MaxOpenConns)
}

// A tenant the registry stops serving — its folder rejected, then removed —
// releases what it held: its pool, its schema registry and the loop
// refreshing it, its verifier, and its dedupe store, its seen ids kept. Once
// removed its routes answer 404, the ingest worker is handed no ClickHouse
// and a DLQ switch that reads on for it — its queued rows are parked — and a
// /livez diagnostic naming it goes back to the no-tenant line; the tenant
// beside it keeps its own. Restoring the folder restores the tenant over a
// fresh pool, registry and verifier, and an id it sent before the removal is
// still a duplicate. Its open streams end too: TestRun_StopEndsOpenStreams.
func TestReload_TenantGoneReleasesItsPoolAndRegistry(t *testing.T) {
	jwks, _, fetches := jwksServer(t, "acme-1")
	acmeSettings := authPatch(jwks.URL)
	acmeSettings["dedupe"] = map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}}
	root := writeNestedSettings(t, map[string]map[string]any{"acme": acmeSettings, "globex": nil})
	a := newApp(t, testConfig(t, root), Options{})
	acme, acmeRegistry, acmeDedup := a.pools.For("acme"), a.discoveries.For("acme"), a.dedup.For("acme")
	require.NotNil(t, acme)
	require.NotNil(t, acmeRegistry)
	require.NotNil(t, a.pools.For("globex"))
	loops := *a.discoveries.cur.Load()
	stopped := func(id tenant.ID) bool {
		select {
		case <-loops[id].done:
			return true
		case <-time.After(5 * time.Second):
			return false
		}
	}
	pipe := func(id string) string {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/pipes/nope", nil)
		req.Header.Set(tenant.Header, id)
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return fmt.Sprintf("%d %s", rec.Code, rec.Body.String())
	}
	dup, err := dedupetest.Mark(t.Context(), acmeDedup, eventKey)
	require.NoError(t, err)
	require.False(t, dup)
	require.Eventually(t, func() bool { return fetches.Load() > 0 }, 5*time.Second, 10*time.Millisecond, "acme's key set is fetched off the boot path")

	rewriteSettings(t, filepath.Join(root, "globex"), invalidQuery)
	_, adopted, known := a.tenants.ReloadTenant("globex", "test")
	require.True(t, known)
	require.False(t, adopted)
	assert.Nil(t, a.pools.For("globex"), "a rejected tenant is on no pool")
	assert.Nil(t, a.discoveries.For("globex"), "and has no registry")
	assert.True(t, stopped("globex"), "nor a loop refreshing one")
	assert.Same(t, acme, a.pools.For("acme"))
	assert.Same(t, acmeRegistry, a.discoveries.For("acme"))

	// Adopted in part from here on: globex's folder stays rejected. No
	// tenant has completed a first discovery, and the diagnostic names acme.
	a.discoveries.onAttempt("acme", errors.New("connection refused"))
	require.Contains(t, get(t, a.Handler(), "/livez").Body.String(), "tenant acme")
	require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
	a.tenants.Reload("test")
	assert.Nil(t, a.pools.For("acme"))
	assert.Nil(t, a.discoveries.For("acme"))
	assert.True(t, stopped("acme"))
	// A stopped loop's last attempt can land after the reload: its line must
	// not come back.
	a.discoveries.onAttempt("acme", errors.New("connection refused"))
	assert.False(t, acmeDedup.Open(), "its dedupe store is closed")
	assert.Contains(t, pipe("acme"), "404 {\"error\":\"unknown tenant: acme\"}")
	assert.Empty(t, a.pools.Target("acme").URL, "the worker has no ClickHouse to insert its queued rows into")
	assert.True(t, dlqFor(a.tenants)("acme", "events"), "and parks them")
	livez := get(t, a.Handler(), "/livez")
	assert.Equal(t, http.StatusServiceUnavailable, livez.Code)
	assert.Contains(t, livez.Body.String(), "no tenant has completed a first discovery yet", "the diagnostic went with its tenant")

	fetched := fetches.Load()
	require.NoError(t, os.Rename(writeSettings(t, acmeSettings), filepath.Join(root, "acme")))
	a.tenants.Reload("test")
	assert.Contains(t, pipe("acme"), "pipe not found", "served again")
	assert.NotNil(t, a.pools.For("acme"))
	assert.NotSame(t, acme, a.pools.For("acme"), "over a fresh pool")
	assert.NotNil(t, a.discoveries.For("acme"))
	assert.NotSame(t, acmeRegistry, a.discoveries.For("acme"), "and a fresh registry")
	assert.Eventually(t, func() bool { return fetches.Load() > fetched }, 5*time.Second, 10*time.Millisecond, "and a fresh verifier, fetching the key set again")
	dup, err = dedupetest.Mark(t.Context(), a.dedup.For("acme"), eventKey)
	require.NoError(t, err)
	assert.True(t, dup, "an id acme sent before the removal is still a duplicate")
}

// Over a nested directory the probes read every tenant together: /livez is
// degraded while no tenant has completed a first discovery, names the tenant
// in its diagnostic, and turns 200 for good at the first success, whatever
// another tenant's discovery does afterwards; /readyz then pings every open
// pool and names each one that does not answer.
func TestNew_NestedProbesFollowTheFirstTenantToLoad(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil})
	a := newApp(t, testConfig(t, root), Options{})

	rec := get(t, a.Handler(), "/livez")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "schema discovery")
	// The loops' first attempts fail at once against closed ports and
	// name their tenant; boot itself starts with the no-tenant diagnostic.
	assert.Eventually(t, func() bool {
		body := get(t, a.Handler(), "/livez").Body.String()
		return strings.Contains(body, "tenant acme") || strings.Contains(body, "tenant globex")
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, http.StatusServiceUnavailable, get(t, a.Handler(), "/readyz").Code, "not ready while degraded, before any ping")

	// The first success anywhere, as the loops report it.
	a.discoveries.onLoaded("acme")
	assert.Equal(t, http.StatusOK, get(t, a.Handler(), "/livez").Code)
	a.discoveries.onAttempt("globex", errors.New("connection refused"))
	assert.Equal(t, http.StatusOK, get(t, a.Handler(), "/livez").Code, "sticky: another tenant's outage is not a probe failure")
	online := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/health", nil)
	online.Header.Set(tenant.Header, "globex")
	rec = httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, online)
	assert.Equal(t, http.StatusOK, rec.Code, "the SDK ping mirrors /livez, for the tenant still failing too")

	rec = get(t, a.Handler(), "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	for _, id := range []tenant.ID{"acme", "globex"} {
		assert.Contains(t, rec.Body.String(), a.pools.For(id).Identity().Addr, "every pool that did not answer is named")
	}
}

// Before a tenant's first discovery a table lookup is a 503 with
// Retry-After, not a 404: in a flat directory during the degraded boot, and
// in a nested one per tenant.
func TestNew_SchemaNotLoadedIs503(t *testing.T) {
	ingest := func(t *testing.T, a *App, id string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ingest?table=clicks", strings.NewReader(`{"page": "/"}`))
		req.Header.Set("Content-Type", "application/json")
		if id != "" {
			req.Header.Set(tenant.Header, id)
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, req)
		return rec
	}
	t.Run("flat, degraded boot", func(t *testing.T) {
		a := newApp(t, testConfig(t, writeSettings(t, nil)), Options{})
		rec := ingest(t, a, "")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
		assert.Equal(t, "5", rec.Header().Get("Retry-After"))
		assert.Contains(t, rec.Body.String(), "schema not loaded yet")
	})
	t.Run("nested, per tenant", func(t *testing.T) {
		a := newApp(t, testConfig(t, writeNestedSettings(t, map[string]map[string]any{"acme": nil})), Options{})
		rec := ingest(t, a, "acme")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
		assert.Equal(t, "5", rec.Header().Get("Retry-After"))
		rec = get(t, a.Handler(), "/v1/ops/schema?tenant=acme")
		assert.Equal(t, http.StatusForbidden, rec.Code, "the ops tree keeps its gate")
	})
}

// Close stops every tenant's discovery loop within the release budget, and
// the pools after them.
func TestClose_StopsTheDiscoveryLoops(t *testing.T) {
	a := newApp(t, testConfig(t, writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil})), Options{})
	loops := *a.discoveries.cur.Load()
	require.Len(t, loops, 2)
	require.NoError(t, a.Close(context.Background()))
	for id, td := range loops {
		select {
		case <-td.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("the loop of tenant %s did not stop", id)
		}
	}
	assert.Nil(t, a.discoveries.For("acme"))
	assert.Nil(t, a.pools.For("acme"))
}

// eventKey is the one dedupe key the tenant-lifecycle tests mark.
var eventKey = dedupe.Key{Table: "events", ID: "e1"}
