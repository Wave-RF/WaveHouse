//go:build integration

// OTel pipeline integration tests.
//
// These tests exercise observability.InitProvider against an in-process
// OTLP gRPC receiver (testutil.FakeOTLP). They verify that sampling rates,
// per-signal gates, and unreachable-endpoint behavior all do what config
// says they do — the kind of regression that unit tests against a no-op
// global can miss.
//
// The OTLP destination is configured the way production configures it: via the
// standard OTEL_EXPORTER_OTLP_* env vars (set per-test with t.Setenv), which
// the SDK reads. InitProvider itself passes no endpoint/TLS/header options.
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
	"google.golang.org/grpc/metadata"

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
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+r.Addr())

			shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
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
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+r.Addr())

	// Logs path: enable the OTel logger pipeline first, then build the
	// slog logger that fans out to (stdout, OTLP) and sample DEBUG/INFO.
	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
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
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+r.Addr())

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
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
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+r.Addr())

	shutdown, promHandler := initAndShutdown(t, observability.ProviderConfig{
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
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+r.Addr())

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
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
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	cfg := observability.ProviderConfig{
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

// TestOTel_TLSPath_TracesAndMetrics pins the https:// → TLS dial path with
// trust supplied via OTEL_EXPORTER_OTLP_CERTIFICATE — the same custom-CA path
// a real operator uses for a private gateway. A regression that broke the TLS
// dial would surface as a missing count on the corresponding receiver.
//
// Logs are deliberately excluded: the pinned otlploggrpc exporter ignores the
// env TLS-cert vars (upstream bug open-telemetry/opentelemetry-go#6661), so a
// custom CA does not apply to the logs signal. Log delivery + header
// propagation are covered by TestOTel_Headers_AppliedToAllSignals (plaintext).
func TestOTel_TLSPath_TracesAndMetrics(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLPTLS(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://"+r.Addr())
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", r.CertFile(t))

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
		TracesEnabled:    true,
		TracesSampleRate: 1.0,
		MetricsEnabled:   true,
	})

	_, span := otel.Tracer("test").Start(context.Background(), "tls-op")
	span.End()

	counter, err := otel.GetMeterProvider().Meter("test").Int64Counter("tls_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 1)

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	require.NoError(t, shutdown(drainCtx))

	assert.Equal(t, 1, r.SpanCount(), "TLS path must deliver the span end-to-end")
	assert.GreaterOrEqual(t, r.MetricCount(), 1, "TLS path must deliver metrics end-to-end")
}

// TestOTel_Headers_AppliedToAllSignals verifies OTEL_EXPORTER_OTLP_HEADERS
// propagates as gRPC metadata on every OTLP exporter — a single missing
// exporter would silently 401 against cloud OTLP gateways.
func TestOTel_Headers_AppliedToAllSignals(t *testing.T) {
	guardOTelGlobals(t)
	r := testutil.NewFakeOTLP(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+r.Addr())
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer test-token,x-honeycomb-team=abc123")

	shutdown, _ := initAndShutdown(t, observability.ProviderConfig{
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
