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
// cfg.PrometheusEnabled is true). Caller is responsible for calling
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

func TestOTel_TraceSampling(t *testing.T) {
	// TraceIDRatioBased over random trace IDs is binomial(n, rate). For
	// rate=0.5 / n=2000 the stddev is ~22; ±200 is ~9σ — flake-proof but tight
	// enough to catch a sampler accidentally pinned at 25% / 75% (would land
	// at 500 / 1500 and slip past a wider window). Bypass (count→n) and
	// broken (count→0) failure modes still fail loud regardless of tolerance.
	cases := []struct {
		name   string
		rate   float64
		n      int
		assert func(t *testing.T, got int)
	}{
		{
			name: "full rate", rate: 1.0, n: 200,
			assert: func(t *testing.T, got int) { assert.Equal(t, 200, got) },
		},
		{
			name: "half rate", rate: 0.5, n: 2000,
			assert: func(t *testing.T, got int) {
				assert.InDelta(t, 1000, got, 200, "got %d spans, expected ~1000 within ±200", got)
			},
		},
		{
			name: "zero rate", rate: 0.0, n: 100,
			assert: func(t *testing.T, got int) {
				assert.Zero(t, got, "0.0 sample rate should drop every span")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No t.Parallel: each case mutates OTel globals via initAndShutdown.
			guardOTelGlobals(t)
			r := testutil.NewFakeOTLP(t)

			shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
				Endpoint:         r.Addr(),
				TracesEnabled:    true,
				TracesSampleRate: tc.rate,
			})

			tracer := otel.Tracer("test")
			for i := 0; i < tc.n; i++ {
				_, span := tracer.Start(context.Background(), "op")
				span.End()
			}

			drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer drainCancel()
			require.NoError(t, shutdown(drainCtx))

			tc.assert(t, r.SpanCount())
		})
	}
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
	// `LogCountAtLevel(13)` is "WARN and above" (testutil semantics), so the
	// complement is DEBUG+INFO — the records the rate=0.0 sampler should drop.
	lowSevCount := r.LogCount() - r.LogCountAtLevel(13)
	warnAndAboveCount := r.LogCountAtLevel(13)
	assert.Zero(t, lowSevCount, "DEBUG+INFO records should have been dropped at sample_rate=0.0")
	assert.Equal(t, 2*n, warnAndAboveCount, "WARN+ERROR floor should export at 100% regardless of rate")
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

	// Span emitted before shutdown — count should be set on return.
	assert.Equal(t, 1, r.SpanCount())
	// Negative assertions: poll for 100ms in case a misconfigured exporter
	// would emit a stray RPC. require.Never is the testify-native primitive
	// for "this must stay false for window X" — clearer than a bare sleep.
	require.Never(t, func() bool { return r.MetricCount() > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"metrics disabled — no metric records should appear")
	require.Never(t, func() bool { return r.LogCount() > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"logs disabled — no log records should appear")
}

func TestOTel_PrometheusScrape_ExposesMetrics(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	shutdown, promHandler := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:          r.Addr(),
		MetricsEnabled:    true,
		PrometheusEnabled: true,
	})
	t.Cleanup(func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		_ = shutdown(drainCtx)
	})
	require.NotNil(t, promHandler, "prometheus handler should be non-nil when PrometheusEnabled=true")

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

	// Log emitted before shutdown — count should be set on return.
	assert.GreaterOrEqual(t, r.LogCount(), 1, "logs should be exported")
	require.Never(t, func() bool { return r.SpanCount() > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"traces disabled — no span records should appear")
	require.Never(t, func() bool { return r.MetricCount() > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"metrics disabled — no metric records should appear")
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

// TestOTel_TLSPath_Traces locks in the https:// → TLS dial path. The fake
// receiver listens with an ephemeral self-signed cert; the exporter is given
// the matching client tls.Config via ProviderConfig.TLSConfig. If TLS wiring
// regresses (e.g. someone re-adds WithInsecure unconditionally), the dial
// will TLS-handshake against a plaintext server and the export drops.
func TestOTel_TLSPath_Traces(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLPTLS(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         "https://" + r.Addr(),
		TLSConfig:        r.TLSConfig(),
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
	})

	_, span := otel.Tracer("test").Start(context.Background(), "tls-op")
	span.End()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Equal(t, 1, r.SpanCount(), "TLS path must deliver the span end-to-end")
}

// TestOTel_Headers_AppliedToAllSignals verifies that ProviderConfig.Headers
// propagates as gRPC metadata on every OTLP exporter (traces, metrics, logs).
// Direct-to-cloud auth depends on this — Honeycomb/Grafana Cloud both
// authenticate per-RPC via a header, so a single missing exporter would 401
// silently for that signal.
func TestOTel_Headers_AppliedToAllSignals(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)

	headers := map[string]string{
		"authorization":    "Bearer test-token",
		"x-honeycomb-team": "abc123",
	}
	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         r.Addr(),
		Headers:          headers,
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   true,
		LogsEnabled:      true,
	})

	// Emit one of each signal.
	_, span := otel.Tracer("test").Start(context.Background(), "auth-op")
	span.End()

	counter, err := otel.GetMeterProvider().Meter("test").Int64Counter("hdr_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelInfo)
	logger := observability.NewLogger("wavehouse-test", lvl, true, 1.0)
	logger.Info("auth-log")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	// gRPC metadata keys are lowercased on the wire — assert in lowercase.
	for _, sig := range []struct {
		name string
		md   func() []string
	}{
		{"traces", func() []string { return r.LastTraceHeaders().Get("authorization") }},
		{"metrics", func() []string { return r.LastMetricHeaders().Get("authorization") }},
		{"logs", func() []string { return r.LastLogHeaders().Get("authorization") }},
	} {
		t.Run(sig.name, func(t *testing.T) {
			vals := sig.md()
			require.NotEmpty(t, vals, "%s exporter dropped the authorization header", sig.name)
			assert.Equal(t, "Bearer test-token", vals[0])
		})
	}
	// Spot-check the second header on at least one signal — same map flows
	// through all three so one check confirms multi-header support.
	assert.Equal(t, []string{"abc123"}, r.LastTraceHeaders().Get("x-honeycomb-team"))
}

// TestOTel_PerSignalEndpoint_Override verifies that TracesEndpoint,
// MetricsEndpoint, and LogsEndpoint each route their own signal to a distinct
// receiver while the default Endpoint sees none. Grafana Cloud's per-signal
// gateway hosts are the headline use case. All three signal overrides go
// through the same pickEndpoint() helper in provider.go, so a copy-paste bug
// (e.g. the metrics exporter being wired to TracesEndpoint, or the logs
// exporter inheriting TracesEndpoint) would surface here as a signal landing
// on the wrong receiver — caught by the cross-asserts below.
func TestOTel_PerSignalEndpoint_Override(t *testing.T) {
	guardOTelGlobals(t)
	rDefault := testutil.NewFakeOTLP(t)
	rTraces := testutil.NewFakeOTLP(t)
	rMetrics := testutil.NewFakeOTLP(t)
	rLogs := testutil.NewFakeOTLP(t)

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:         rDefault.Addr(), // all three signals overridden → default sees nothing
		TracesEndpoint:   rTraces.Addr(),
		MetricsEndpoint:  rMetrics.Addr(),
		LogsEndpoint:     rLogs.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   true,
		LogsEnabled:      true,
	})

	_, span := otel.Tracer("test").Start(context.Background(), "split-op")
	span.End()

	counter, err := otel.GetMeterProvider().Meter("test").Int64Counter("split_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelInfo)
	logger := observability.NewLogger("wavehouse-test", lvl, true, 1.0)
	logger.Info("split-log")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Equal(t, 1, rTraces.SpanCount(), "TracesEndpoint should receive the span")
	assert.GreaterOrEqual(t, rMetrics.MetricCount(), 1, "MetricsEndpoint should receive the metric")
	assert.GreaterOrEqual(t, rLogs.LogCount(), 1, "LogsEndpoint should receive the log")
	assert.Zero(t, rDefault.SpanCount(), "default endpoint must NOT receive traces when overridden")
	assert.Zero(t, rDefault.MetricCount(), "default endpoint must NOT receive metrics when overridden")
	assert.Zero(t, rDefault.LogCount(), "default endpoint must NOT receive logs when overridden")
	assert.Zero(t, rTraces.MetricCount(), "TracesEndpoint should NOT receive metrics (catches metrics-to-traces wiring bug)")
	assert.Zero(t, rTraces.LogCount(), "TracesEndpoint should NOT receive logs (catches logs-to-traces wiring bug)")
	assert.Zero(t, rMetrics.SpanCount(), "MetricsEndpoint should NOT receive spans (catches traces-to-metrics wiring bug)")
	assert.Zero(t, rMetrics.LogCount(), "MetricsEndpoint should NOT receive logs (catches logs-to-metrics wiring bug)")
	assert.Zero(t, rLogs.SpanCount(), "LogsEndpoint should NOT receive spans (catches traces-to-logs wiring bug)")
	assert.Zero(t, rLogs.MetricCount(), "LogsEndpoint should NOT receive metrics (catches metrics-to-logs wiring bug)")
}
