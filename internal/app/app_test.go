package app

import (
	"context"
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
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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
		Cache:    config.Cache{L1MaxCost: 1 << 20},
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

// The wired registry holds the default tenant only: no header and "0" reach
// the route, any other well-formed id is a 404, a malformed one a 400, and
// the ops tree never looks at the header. A 503 is the handler's own answer —
// boot is degraded without ClickHouse — so it proves the tenant resolved.
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
			assert.Equal(t, tt.enabled, a.dedup.Open())
			_, err := os.Stat(filepath.Join(cfg.DataDir, "pebble"))
			assert.Equal(t, tt.enabled, err == nil, "pebble directory exists iff dedupe is on")
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
	require.False(t, a.dedup.Open())
	require.Equal(t, int64(1<<30), a.mq.MaxBytes())

	rewriteSettings(t, dir, map[string]any{
		"dedupe": map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}},
		"mq":     map[string]any{"max_bytes_gb": 2},
	})
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.True(t, a.dedup.Open(), "dedupe hook opened the store")
	// How the budget is split across the MQ's queues is internal/mq's to test.
	assert.Equal(t, int64(2<<30), a.mq.MaxBytes(), "mq hook applied the new byte budget")

	rewriteSettings(t, dir, map[string]any{"mq": map[string]any{"max_bytes_gb": 2}})
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.False(t, a.dedup.Open(), "dedupe hook closed the store")
}

func TestNew_RefusesInvalidSettingsDirectory(t *testing.T) {
	guardGlobals(t)
	cfg := testConfig(t, t.TempDir()) // empty: every required file is missing
	a, err := New(t.Context(), Options{Config: cfg})
	require.Error(t, err)
	assert.Nil(t, a)
	assert.Contains(t, err.Error(), "settings directory")
}

func TestNew_AuthBootFailureReleasesEverything(t *testing.T) {
	guardGlobals(t)
	// An unreachable JWKS endpoint fails boot loudly; the stores opened
	// before it must be released, so the same data_dir boots again.
	dir := writeSettings(t, map[string]any{"auth": map[string]any{
		"jwks_url": "http://" + closedAddr(t) + "/jwks.json", "role_claim": "role",
	}})
	cfg := testConfig(t, dir)
	a, err := New(t.Context(), Options{Config: cfg})
	require.Error(t, err)
	assert.Nil(t, a)
	assert.Contains(t, err.Error(), "auth middleware init")

	cfg.Settings.Dir = writeSettings(t, nil)
	a, err = New(t.Context(), Options{Config: cfg})
	require.NoError(t, err)
	assert.NoError(t, a.Close(context.Background()))
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

func TestRun_StopEndsOpenStreams(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := testConfig(t, writeSettings(t, nil))
	a := newApp(t, cfg, Options{Listener: ln})
	baseURL, stop := runApp(t, a, ln)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/v1/stream?table=events", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	buf := make([]byte, 64)
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	require.Contains(t, string(buf[:n]), ": connected", "the stream is open")

	// A stream is a connection to close, not work to drain: the stop ends it
	// at once rather than waiting out server.shutdown_timeout and then
	// force-closing it anyway.
	started := time.Now()
	assert.NoError(t, stop())
	assert.Less(t, time.Since(started), time.Second, "the open stream held the stop for the drain budget")
	_, err = io.ReadAll(resp.Body)
	assert.NoError(t, err, "the server ended the stream cleanly")
}
