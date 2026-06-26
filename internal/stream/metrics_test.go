package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics_RecordsInstruments(t *testing.T) {
	// No t.Parallel(): NewMetrics reads the global meter provider, which this
	// test swaps. Save and restore it so cleanup doesn't leave the package
	// pointing at a shut-down provider.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	m := NewMetrics()
	m.ConnOpened()
	m.FrameSent(KindKeepalive, 3)
	m.FrameSent(KindEvent, 42)
	m.ConnClosed(2 * time.Second)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			names = append(names, md.Name)
		}
	}
	assert.Contains(t, names, "wavehouse_sse_active_streams")
	assert.Contains(t, names, "wavehouse_sse_stream_duration_seconds")
	assert.Contains(t, names, "wavehouse_sse_frames_sent_total")
	assert.Contains(t, names, "wavehouse_sse_bytes_sent_total")
}

func TestMetrics_NilIsNoop(t *testing.T) {
	t.Parallel()
	var m *Metrics // nil receiver — the handler's unwired path
	assert.NotPanics(t, func() {
		m.ConnOpened()
		m.FrameSent(KindEvent, 1)
		m.ConnClosed(time.Second)
	})
}
