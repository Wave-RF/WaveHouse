package stream

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Frame kinds for the "kind" attribute on the frame and byte counters.
const (
	KindKeepalive = "keepalive"
	KindEvent     = "event"
)

// Metrics records SSE stream activity: how many streams are open, how long they
// last, and how much is written out. The zero value (a nil *Metrics) is a no-op,
// so the handler can hold one unconditionally and tests can skip wiring it.
type Metrics struct {
	active   metric.Int64UpDownCounter
	duration metric.Float64Histogram
	frames   metric.Int64Counter
	bytes    metric.Int64Counter
}

// NewMetrics builds the SSE instruments on the global meter provider. Call it
// after observability.InitProvider, otherwise the instruments bind to the no-op
// default provider for the process lifetime.
func NewMetrics() *Metrics {
	meter := otel.Meter("wavehouse-stream")
	active, _ := meter.Int64UpDownCounter("wavehouse_sse_active_streams",
		metric.WithDescription("Currently open SSE streams"))
	duration, _ := meter.Float64Histogram("wavehouse_sse_stream_duration_seconds",
		metric.WithDescription("SSE stream lifetime"), metric.WithUnit("s"))
	frames, _ := meter.Int64Counter("wavehouse_sse_frames_sent_total",
		metric.WithDescription("SSE frames written to clients"))
	bytes, _ := meter.Int64Counter("wavehouse_sse_bytes_sent_total",
		metric.WithDescription("SSE bytes written to clients"), metric.WithUnit("By"))
	return &Metrics{active: active, duration: duration, frames: frames, bytes: bytes}
}

// ConnOpened records a newly established stream.
func (m *Metrics) ConnOpened() {
	if m == nil {
		return
	}
	m.active.Add(context.Background(), 1)
}

// ConnClosed records a stream closing and its total lifetime.
func (m *Metrics) ConnClosed(d time.Duration) {
	if m == nil {
		return
	}
	m.active.Add(context.Background(), -1)
	m.duration.Record(context.Background(), d.Seconds())
}

// FrameSent records one frame of the given kind (KindKeepalive or KindEvent) and
// its size in bytes.
func (m *Metrics) FrameSent(kind string, n int) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("kind", kind))
	m.frames.Add(context.Background(), 1, attrs)
	m.bytes.Add(context.Background(), int64(n), attrs)
}
