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
	KindKeepalive = "keepalive" // wheel keepalive comment
	KindEvent     = "event"     // live event off the hub
	KindReplay    = "replay"    // historical event from gap-fill on (re)connect
)

// Metrics records SSE stream activity: how many streams are open, how long they
// last, and how much is written out. The zero value (a nil *Metrics) is a no-op,
// so the handler can hold one unconditionally and tests can skip wiring it.
type Metrics struct {
	active   metric.Int64UpDownCounter
	duration metric.Float64Histogram
	frames   metric.Int64Counter
	bytes    metric.Int64Counter
	dropped  metric.Int64Counter
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
	dropped, _ := meter.Int64Counter("wavehouse_sse_dropped_frames_total",
		metric.WithDescription("SSE frames dropped to a full subscriber queue (slow consumer)"))
	return &Metrics{active: active, duration: duration, frames: frames, bytes: bytes, dropped: dropped}
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

// FrameSent records one frame of the given kind (a Kind* constant) and its size
// in bytes.
func (m *Metrics) FrameSent(kind string, n int) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("kind", kind))
	m.frames.Add(context.Background(), 1, attrs)
	m.bytes.Add(context.Background(), int64(n), attrs)
}

// FrameDropped records one frame dropped because a subscriber's queue was full —
// the slow-consumer signal that was silent before #294. kind is a Kind* constant.
func (m *Metrics) FrameDropped(kind string) {
	if m == nil {
		return
	}
	m.dropped.Add(context.Background(), 1, metric.WithAttributes(attribute.String("kind", kind)))
}
