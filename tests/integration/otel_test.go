//go:build integration

// OTel pipeline integration tests against testutil.FakeOTLP.
//
// InitProvider mutates global OTel state; tests must NOT run in parallel
// and must save/restore globals via guardOTelGlobals.

package tests

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"google.golang.org/grpc/metadata"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// guardOTelGlobals saves and restores the OTel package-global providers so
// a test's InitProvider call doesn't leak into later tests.
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

// initAndShutdown installs the OTel pipeline and returns shutdown + the
// Prometheus handler (non-nil iff cfg.PrometheusEnabled). Caller drains via
// shutdown before asserting on the receiver.
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
	// Binomial(n, rate); ±200 over n=2000 is ~9σ — flake-proof and still
	// catches a sampler pinned at 25%/75% (would land at 500/1500).
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

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		Endpoint:    r.Addr(),
		LogsEnabled: true,
	})

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelDebug)
	// rate=0.0 → DEBUG/INFO dropped from OTLP; WARN+ still 100%.
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

	// OTel severity: DEBUG=5, INFO=9, WARN=13, ERROR=17. LogCountAtLevel(13)
	// = WARN+; complement is DEBUG+INFO (what rate=0.0 should drop).
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

	assert.Equal(t, 1, r.SpanCount())
	// require.Never polls for 100ms to catch any stray RPC from a
	// misconfigured exporter.
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

	meter := otel.GetMeterProvider().Meter("wavehouse-test")
	counter, err := meter.Int64Counter("test_widget_received")
	require.NoError(t, err)
	counter.Add(context.Background(), 7)

	server := httptest.NewServer(promHandler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	// The OTel→Prometheus exporter appends _total to counters automatically.
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

	// 127.0.0.1:1 never connects. Pins the "OTel down doesn't kill the
	// binary" invariant — gRPC exporters dial lazily.
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

	// span.End / logger.Info must never block on the failed export — the
	// SDK buffers in-memory and the batch processor handles drops async.
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

	// Best-effort shutdown in a goroutine — the OTel SDK doesn't fully
	// honor shutdown deadlines against an unreachable endpoint (same reason
	// main.go bounds its shutdown context).
	go func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer drainCancel()
		_ = shutdown(drainCtx)
	}()
}

// TestOTel_TLSPath_AllSignals pins the https:// → TLS dial path on every
// OTLP exporter (traces, metrics, logs). A regression that re-adds
// WithInsecure on one branch would TLS-handshake against the plaintext side
// and surface as a missing count on the corresponding receiver.
func TestOTel_TLSPath_AllSignals(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLPTLS(t)

	cfg := observability.ProviderConfig{
		Endpoint:         "https://" + r.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   true,
		LogsEnabled:      true,
	}
	cfg.SetTLSConfigForTesting(r.TLSConfig())
	shutdown, _ := initAndShutdown(t, cfg)

	_, span := otel.Tracer("test").Start(context.Background(), "tls-op")
	span.End()

	counter, err := otel.GetMeterProvider().Meter("test").Int64Counter("tls_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelInfo)
	logger := observability.NewLogger("wavehouse-test", lvl, true, 1.0)
	logger.Info("tls-log")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Equal(t, 1, r.SpanCount(), "TLS path must deliver the span end-to-end")
	assert.GreaterOrEqual(t, r.MetricCount(), 1, "TLS path must deliver metrics end-to-end")
	assert.GreaterOrEqual(t, r.LogCount(), 1, "TLS path must deliver logs end-to-end")
}

// TestOTel_Headers_AppliedToAllSignals verifies ProviderConfig.Headers
// propagates as gRPC metadata on every OTLP exporter — a single missing
// exporter would silently 401 against cloud OTLP gateways.
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

	// gRPC metadata keys are lowercased on the wire.
	signals := []struct {
		name string
		md   func() metadata.MD
	}{
		{"traces", func() metadata.MD { return r.LastTraceHeaders() }},
		{"metrics", func() metadata.MD { return r.LastMetricHeaders() }},
		{"logs", func() metadata.MD { return r.LastLogHeaders() }},
	}
	for _, sig := range signals {
		t.Run(sig.name, func(t *testing.T) {
			md := sig.md()
			authz := md.Get("authorization")
			require.NotEmpty(t, authz, "%s exporter dropped the authorization header", sig.name)
			assert.Equal(t, "Bearer test-token", authz[0])
			assert.Equal(t, []string{"abc123"}, md.Get("x-honeycomb-team"),
				"%s exporter dropped the x-honeycomb-team header", sig.name)
		})
	}
}

// TestOTel_PerSignalEndpoint_Override verifies each per-signal endpoint
// routes its own signal to a distinct receiver. The cross-asserts catch
// copy-paste wiring bugs (e.g. metrics exporter pointed at TracesEndpoint).
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

// TestOTel_TLSPath_PerSignalEndpoint pins the production Grafana-Cloud
// wiring: distinct https:// per-signal endpoints (traces / metrics / logs
// going to separate hosts) AND TLS on every leg. Each receiver mints its
// own self-signed cert; the test merges all three certs into one client
// trust pool because ProviderConfig holds a single tlsConfig that the SDK
// applies to every exporter. A regression where per-signal endpoint parsing
// silently drops the `https://` scheme would fall back to plaintext and the
// gRPC handshake against the TLS receivers would fail.
func TestOTel_TLSPath_PerSignalEndpoint(t *testing.T) {
	guardOTelGlobals(t)
	rTraces := testutil.NewFakeOTLPTLS(t)
	rMetrics := testutil.NewFakeOTLPTLS(t)
	rLogs := testutil.NewFakeOTLPTLS(t)

	pool := x509.NewCertPool()
	for _, r := range []*testutil.FakeOTLP{rTraces, rMetrics, rLogs} {
		pool.AddCert(r.Cert())
	}
	clientCfg := &tls.Config{
		RootCAs:    pool,
		ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS13,
	}

	cfg := observability.ProviderConfig{
		// Endpoint stays plaintext intentionally — it must never receive
		// traffic because every signal is overridden. If a future regression
		// causes a per-signal override to be ignored, the exporter would
		// dial the plaintext default and the test's TLS-only receivers
		// would record nothing.
		Endpoint:         "127.0.0.1:1", // unreachable; deliberately not a fake
		TracesEndpoint:   "https://" + rTraces.Addr(),
		MetricsEndpoint:  "https://" + rMetrics.Addr(),
		LogsEndpoint:     "https://" + rLogs.Addr(),
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   true,
		LogsEnabled:      true,
	}
	cfg.SetTLSConfigForTesting(clientCfg)
	shutdown, _ := initAndShutdown(t, cfg)

	_, span := otel.Tracer("test").Start(context.Background(), "tls-split-op")
	span.End()

	counter, err := otel.GetMeterProvider().Meter("test").Int64Counter("tls_split_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelInfo)
	logger := observability.NewLogger("wavehouse-test", lvl, true, 1.0)
	logger.Info("tls-split-log")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Equal(t, 1, rTraces.SpanCount(), "TLS TracesEndpoint should receive the span")
	assert.GreaterOrEqual(t, rMetrics.MetricCount(), 1, "TLS MetricsEndpoint should receive the metric")
	assert.GreaterOrEqual(t, rLogs.LogCount(), 1, "TLS LogsEndpoint should receive the log")
	assert.Zero(t, rTraces.MetricCount(), "TLS TracesEndpoint should NOT receive metrics")
	assert.Zero(t, rTraces.LogCount(), "TLS TracesEndpoint should NOT receive logs")
	assert.Zero(t, rMetrics.SpanCount(), "TLS MetricsEndpoint should NOT receive spans")
	assert.Zero(t, rMetrics.LogCount(), "TLS MetricsEndpoint should NOT receive logs")
	assert.Zero(t, rLogs.SpanCount(), "TLS LogsEndpoint should NOT receive spans")
	assert.Zero(t, rLogs.MetricCount(), "TLS LogsEndpoint should NOT receive metrics")
}
