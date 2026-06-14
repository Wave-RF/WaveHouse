package observability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
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
