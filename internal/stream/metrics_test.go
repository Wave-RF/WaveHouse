package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	byName := collectByName(rm)

	// active: +1 open, -1 close → net 0.
	active := byName["wavehouse_sse_active_streams"].Data.(metricdata.Sum[int64])
	require.Len(t, active.DataPoints, 1)
	assert.Equal(t, int64(0), active.DataPoints[0].Value)

	// duration: exactly one closed stream, lifetime recorded.
	dur := byName["wavehouse_sse_stream_duration_seconds"].Data.(metricdata.Histogram[float64])
	require.Len(t, dur.DataPoints, 1)
	assert.Equal(t, uint64(1), dur.DataPoints[0].Count)
	assert.InDelta(t, 2.0, dur.DataPoints[0].Sum, 0.001)

	// frames + bytes: one of each kind, carrying the kind attribute and the
	// exact count/size recorded.
	frames := byName["wavehouse_sse_frames_sent_total"].Data.(metricdata.Sum[int64])
	assert.Equal(t, int64(1), byKind(t, frames.DataPoints, KindKeepalive))
	assert.Equal(t, int64(1), byKind(t, frames.DataPoints, KindEvent))

	bytesSent := byName["wavehouse_sse_bytes_sent_total"].Data.(metricdata.Sum[int64])
	assert.Equal(t, int64(3), byKind(t, bytesSent.DataPoints, KindKeepalive))
	assert.Equal(t, int64(42), byKind(t, bytesSent.DataPoints, KindEvent))
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

// collectByName indexes the collected metrics by instrument name.
func collectByName(rm metricdata.ResourceMetrics) map[string]metricdata.Metrics {
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			out[md.Name] = md
		}
	}
	return out
}

// byKind returns the value of the datapoint carrying kind=want, failing if absent.
func byKind(t *testing.T, dps []metricdata.DataPoint[int64], want string) int64 {
	t.Helper()
	for _, dp := range dps {
		if v, ok := dp.Attributes.Value(attribute.Key("kind")); ok && v.AsString() == want {
			return dp.Value
		}
	}
	t.Fatalf("no datapoint with kind=%q", want)
	return 0
}
