package observability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
)

// TestInitProvider_Shutdown verifies that the provider pipeline initializes
// (the gRPC exporters dial lazily, so no collector need be reachable) and the
// returned shutdown function drains both pipelines without error.
func TestInitProvider_Shutdown(t *testing.T) {
	// No t.Parallel(): InitProvider mutates global OTel state. Save the
	// pre-test globals and restore them in cleanup so this test doesn't leak
	// its provider/propagator into the parallel tests in this package.
	savedProp := otel.GetTextMapPropagator()
	savedTP := otel.GetTracerProvider()
	savedMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(savedProp)
		otel.SetTracerProvider(savedTP)
		otel.SetMeterProvider(savedMP)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// No OTEL_EXPORTER_OTLP_ENDPOINT set → the SDK uses its default target and
	// the gRPC exporters dial lazily, so InitProvider succeeds regardless of
	// whether anything is listening.
	shutdown, promHandler, err := InitProvider(ctx, "wavehouse-test", ProviderConfig{
		TracesEnabled:    true,
		TracesSampleRate: 0.10,
		MetricsEnabled:   true,
		LogsEnabled:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.Nil(t, promHandler, "prometheus exporter not requested → handler must be nil")

	// Drain the pipeline. We tolerate flush errors here because the OTLP
	// exporter can't reach a collector, so the metric exporter times out its
	// final upload. What matters for coverage is that the shutdown path runs
	// through both providers.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	_ = shutdown(shutdownCtx)

	// Second call should be a genuine no-op since shutdownFuncs is cleared
	// on first invocation.
	assert.NoError(t, shutdown(shutdownCtx))
}

// TestInitProvider_ShutdownParallelBounded is the #288 regression guard: with
// an unreachable collector and buffered telemetry in every signal, shutdown
// must stay bounded by its context deadline. That needs two things together —
// fanning the traces/metrics/logs providers out concurrently (so their flushes
// overlap instead of summing) AND returning when the deadline passes even
// though the experimental logs SDK's BatchProcessor.Shutdown ignores ctx and
// blocks for the exporter's full ~10s timeout during gRPC backoff. Drop either
// and the shutdown overruns the budget: serial stacks the flushes, and a plain
// wg.Wait blocks on the logs straggler.
func TestInitProvider_ShutdownParallelBounded(t *testing.T) {
	// Pin a definitely-unreachable collector so every exporter spends the
	// shutdown budget in gRPC retry/backoff. t.Setenv also enforces
	// non-parallel, which we need anyway — InitProvider mutates OTel globals.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:14317")

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

	shutdown, _, err := InitProvider(context.Background(), "wavehouse-test", ProviderConfig{
		TracesEnabled:  true,
		MetricsEnabled: true,
		LogsEnabled:    true,
	})
	require.NoError(t, err)

	// Buffer telemetry in every signal so shutdown must attempt a network
	// flush on each provider — an empty batch would short-circuit first.
	_, span := otel.Tracer("test").Start(context.Background(), "s")
	span.End()
	ctr, err := otel.Meter("test").Int64Counter("c")
	require.NoError(t, err)
	ctr.Add(context.Background(), 1)
	var rec otellog.Record
	rec.SetBody(attribute.StringValue("shutdown-regression"))
	global.GetLoggerProvider().Logger("test").Emit(context.Background(), rec)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_ = shutdown(shutdownCtx) // flush errors expected — collector is unreachable
	elapsed := time.Since(start)

	// A correctly bounded shutdown returns at the ~2s deadline; an
	// unconcurrent or unbounded one overruns by several seconds while the
	// logs flush drags on. Generous 3s ceiling keeps this stable in CI.
	assert.Less(t, elapsed, 3*time.Second,
		"OTel shutdown must stay bounded by its context deadline (#288); took %s", elapsed)
}
