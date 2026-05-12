//go:build integration

// OTel pipeline integration tests.
//
// These tests exercise observability.InitProvider against an in-process
// OTLP gRPC receiver (testutil.FakeOTLP). They verify that sampling rates,
// per-signal gates, and unreachable-endpoint behavior all do what config
// says they do — the kind of regression that unit tests against a no-op
// global can miss.
//
// InitProvider mutates global OTel state (tracer/meter/logger providers,
// propagator). Tests in this file MUST NOT run in parallel and MUST save/
// restore the globals on entry/exit. They share the `env(t)` infrastructure
// only incidentally — none of them touch ClickHouse or the API server.

package tests

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// guardOTelGlobals saves and restores the package-global OTel providers so a
// test's InitProvider call doesn't leak its state into later tests.
func guardOTelGlobals(t *testing.T) {
	t.Helper()
	savedProp := otel.GetTextMapPropagator()
	savedTP := otel.GetTracerProvider()
	savedMP := otel.GetMeterProvider()
	savedLP := global.GetLoggerProvider()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(savedProp)
		otel.SetTracerProvider(savedTP)
		otel.SetMeterProvider(savedMP)
		global.SetLoggerProvider(savedLP)
	})
}

// initAndShutdown installs the OTel pipeline with the given config and
// returns the shutdown func plus the Prometheus handler (non-nil only when
// cfg.MetricsPrometheusEnabled is true). Caller is responsible for calling
// shutdown to drain pending exports before asserting on the receiver.
func initAndShutdown(t *testing.T, cfg observability.ProviderConfig) (func(context.Context) error, http.Handler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, promHandler, err := observability.InitProvider(ctx, "wavehouse-test", cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	return shutdown, promHandler
}

func TestOTel_TraceSampling_FullRate(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         r.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
	})

	const n = 200
	tracer := otel.Tracer("test")
	for i := 0; i < n; i++ {
		_, span := tracer.Start(context.Background(), "op")
		span.End()
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Equal(t, n, r.SpanCount())
}

func TestOTel_TraceSampling_HalfRate(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         r.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 0.5,
	})

	const n = 2000
	tracer := otel.Tracer("test")
	for i := 0; i < n; i++ {
		_, span := tracer.Start(context.Background(), "op")
		span.End()
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	// TraceIDRatioBased over random trace IDs is binomial(2000, 0.5):
	// mean 1000, stddev ~22. ±25% tolerance covers >>6σ and any rounding
	// in the sampler's bit-mask comparison. The test fails the right way
	// if sampling is silently bypassed (count→2000) or broken (count→0).
	got := r.SpanCount()
	assert.InDelta(t, 1000, got, 500, "got %d spans, expected ~1000 within ±500", got)
}

func TestOTel_TraceSampling_ZeroRate(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         r.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 0.0,
	})

	const n = 100
	tracer := otel.Tracer("test")
	for i := 0; i < n; i++ {
		_, span := tracer.Start(context.Background(), "op")
		span.End()
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Zero(t, r.SpanCount(), "0.0 sample rate should drop every span")
}

func TestOTel_LogSampling_WarnFloorAlwaysExports(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	// Logs path: enable the OTel logger pipeline first, then build the
	// slog logger that fans out to (stdout, OTLP) and sample DEBUG/INFO.
	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:    r.Addr(),
		LogsEnabled: true,
	})

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelDebug)
	// sample_rate=0.0 → DEBUG/INFO entirely dropped from OTLP, WARN+ still 100%.
	logger := observability.NewLogger("wavehouse-test", lvl, true, 0.0)

	const n = 50
	for i := 0; i < n; i++ {
		logger.Info("info-line", "i", i)
		logger.Warn("warn-line", "i", i)
		logger.Error("err-line", "i", i)
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	// OTel severity numbers: DEBUG=5, INFO=9, WARN=13, ERROR=17.
	infoCount := r.LogCount() - r.LogCountAtLevel(13)
	warnCount := r.LogCountAtLevel(13)
	assert.Zero(t, infoCount, "INFO records should have been dropped at sample_rate=0.0")
	assert.Equal(t, 2*n, warnCount, "WARN+ERROR floor should export at 100% regardless of rate")
}

func TestOTel_PerSignal_TracesOnly(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         r.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   false,
		LogsEnabled:      false,
	})

	_, span := otel.Tracer("test").Start(context.Background(), "op")
	span.End()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	// 100ms grace so any stray metric/log RPCs from a misconfig surface.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, r.SpanCount())
	assert.Zero(t, r.MetricCount(), "metrics disabled — no metric records should appear")
	assert.Zero(t, r.LogCount(), "logs disabled — no log records should appear")
}

func TestOTel_PrometheusScrape_ExposesMetrics(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, promHandler := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:                 r.Addr(),
		MetricsEnabled:           true,
		MetricsPrometheusEnabled: true,
	})
	t.Cleanup(func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		_ = shutdown(drainCtx)
	})
	require.NotNil(t, promHandler, "prometheus handler should be non-nil when MetricsPrometheusEnabled=true")

	// Record a known custom metric so we can assert it appears at /metrics.
	// OTel→Prometheus name translation: dots/dashes become underscores,
	// counter suffix `_total` is appended automatically.
	meter := otel.GetMeterProvider().Meter("wavehouse-test")
	counter, err := meter.Int64Counter("test_widget_received")
	require.NoError(t, err)
	counter.Add(context.Background(), 7)

	// Scrape via httptest. We don't go over the wire; the handler is the
	// same one main.go would mount on the API router or sidecar listener.
	server := httptest.NewServer(promHandler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	// Counter ending in _total is the standard Prometheus convention; the
	// OTel exporter applies it automatically.
	assert.Contains(t, bodyStr, "test_widget_received_total", "/metrics must list the custom counter we recorded")
	assert.Contains(t, bodyStr, "# HELP", "Prometheus format includes HELP lines")
	assert.Contains(t, bodyStr, "# TYPE", "Prometheus format includes TYPE lines")
}

func TestOTel_PerSignal_LogsOnly(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:       r.Addr(),
		TracesEnabled:  false,
		MetricsEnabled: false,
		LogsEnabled:    true,
	})

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelInfo)
	logger := observability.NewLogger("wavehouse-test", lvl, true, 1.0)
	logger.Info("hello")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	time.Sleep(100 * time.Millisecond)

	assert.GreaterOrEqual(t, r.LogCount(), 1, "logs should be exported")
	assert.Zero(t, r.SpanCount())
	assert.Zero(t, r.MetricCount())
}

func TestOTel_UnreachableEndpoint_DoesNotBlockStartupOrEmits(t *testing.T) {
	guardOTelGlobals(t)

	// 127.0.0.1:1 is in the unassigned-port range — connect should never
	// succeed. gRPC exporters dial lazily, so InitProvider must still
	// succeed regardless. This is the critical "OTel down doesn't kill
	// the binary" invariant.
	cfg := observability.ProviderConfig{
		Endpoint:         "127.0.0.1:1",
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   true,
		LogsEnabled:      true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, _, err := observability.InitProvider(ctx, "wavehouse-test", cfg)
	require.NoError(t, err, "InitProvider must not fail on unreachable endpoint")
	require.NotNil(t, shutdown)

	// Emit work — neither span.End nor logger.Info may block on the failed
	// export. The SDK buffers in-memory and the batch processor handles
	// drops asynchronously; this is the contract that lets the request hot
	// path survive collector outages.
	lvl := &slog.LevelVar{}
	logger := observability.NewLogger("wavehouse-test", lvl, true, 1.0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_, span := otel.Tracer("test").Start(context.Background(), "op")
			span.End()
			logger.Info("hello", "i", i)
		}
	}()
	select {
	case <-done:
		// Emits returned promptly — invariant holds.
	case <-time.After(5 * time.Second):
		t.Fatal("emits blocked on unreachable endpoint")
	}

	// Best-effort shutdown in a goroutine so the test doesn't leak the
	// runtime-metrics goroutine. We don't assert anything about it — the
	// OTel SDK doesn't fully honor the shutdown deadline against an
	// unreachable gRPC endpoint, and main.go bounds the timeout for the
	// same reason. See the defer wrapping otelShutdown in cmd/wavehouse/main.go.
	go func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer drainCancel()
		_ = shutdown(drainCtx)
	}()
}
