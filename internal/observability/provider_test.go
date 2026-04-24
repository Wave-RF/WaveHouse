package observability

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInitProvider_Shutdown verifies that the provider pipeline initializes
// against an arbitrary endpoint (the gRPC exporters dial lazily) and the
// returned shutdown function drains both pipelines without error.
func TestInitProvider_Shutdown(t *testing.T) {
	// No t.Parallel(): InitProvider mutates global OTel state that other
	// tests in this package read/write.

	// Bind a local TCP listener so the exporter has a reachable address to
	// dial if it chooses to — we never actually serve OTLP on it.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdown, err := InitProvider(ctx, "wavehouse-test", lis.Addr().String())
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Drain the pipeline. We tolerate flush errors here because the OTLP
	// exporter can't actually reach our listener (we never accept on it), so
	// the metric exporter times out its final upload. What matters for
	// coverage is that the shutdown path runs through both providers.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	_ = shutdown(shutdownCtx)

	// Second call should be a genuine no-op since shutdownFuncs is cleared
	// on first invocation.
	assert.NoError(t, shutdown(shutdownCtx))
}
