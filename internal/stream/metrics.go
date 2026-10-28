package stream

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// Frame kinds for the "kind" attribute on the frame and byte counters.
const (
	KindKeepalive = "keepalive" // wheel keepalive comment
	KindEvent     = "event"     // live event off the hub
	KindReplay    = "replay"    // historical event from gap-fill on (re)connect
	KindSchema    = "schema"    // column list for the rows that follow
)

// Drop reasons for the "reason" attribute on wavehouse_sse_dropped_frames_total:
// whether the stream could not keep up, or ended with frames still owed to it.
const (
	DropBufferFull   = "buffer_full"   // the subscriber's queue was full: the client reads slower than events arrive
	DropClientGone   = "client_gone"   // the client went away (a write failed, or it disconnected) with the frame unwritten
	DropStreamClosed = "stream_closed" // the server ended the stream (shutdown, or its tenant no longer served) with the frame unwritten
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
	withheld metric.Int64Counter

	meter     metric.Meter
	queuePeak metric.Int64ObservableGauge

	// tenant is the tenant label every per-stream measurement carries (#891),
	// set by Tenant; none on the process-wide Metrics.
	tenant []attribute.KeyValue
}

// Tenant returns m's instruments labeled with tenant id, for the measurements
// of one stream of that tenant. A nil m stays nil.
func (m *Metrics) Tenant(id tenant.ID) *Metrics {
	if m == nil {
		return nil
	}
	c := *m
	c.tenant = []attribute.KeyValue{observability.TenantLabel(id.String())}
	return &c
}

// attrs is attrs plus m's tenant label, if it has one.
func (m *Metrics) attrs(attrs ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributes(append(attrs, m.tenant...)...)
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
		metric.WithDescription("SSE frames never written to a client, by why (reason) and the streamed table"))
	withheld, _ := meter.Int64Counter("wavehouse_sse_rows_withheld_total",
		metric.WithDescription("Event rows withheld from a subscriber by the role's row-level-security filter (including fail-closed evaluations)"))
	queuePeak, _ := meter.Int64ObservableGauge("wavehouse_sse_queue_peak_frames",
		metric.WithDescription("Deepest any open SSE stream's outbound queue has been since the stream opened, in frames"))
	return &Metrics{
		active: active, duration: duration, frames: frames, bytes: bytes, dropped: dropped, withheld: withheld,
		meter: meter, queuePeak: queuePeak,
	}
}

// ConnOpened records a newly established stream.
func (m *Metrics) ConnOpened() {
	if m == nil {
		return
	}
	m.active.Add(context.Background(), 1, m.attrs())
}

// ConnClosed records a stream closing and its total lifetime.
func (m *Metrics) ConnClosed(d time.Duration) {
	if m == nil {
		return
	}
	m.active.Add(context.Background(), -1, m.attrs())
	m.duration.Record(context.Background(), d.Seconds(), m.attrs())
}

// FrameSent records one frame of the given kind (a Kind* constant) and its size
// in bytes.
func (m *Metrics) FrameSent(kind string, n int) {
	if m == nil {
		return
	}
	attrs := m.attrs(attribute.String("kind", kind))
	m.frames.Add(context.Background(), 1, attrs)
	m.bytes.Add(context.Background(), int64(n), attrs)
}

// FrameDropped records one frame of the given kind (a Kind* constant) on a
// stream of table that never reached the client, and why (a Drop* constant).
func (m *Metrics) FrameDropped(table, kind, reason string) {
	if m == nil {
		return
	}
	m.dropped.Add(context.Background(), 1, m.attrs(
		attribute.String("table", table), attribute.String("kind", kind), attribute.String("reason", reason)))
}

// observeQueuePeak reports peak, read at each collection, as the queue-peak
// gauge. A peak only grows while its stream is open, so a burst that filled a
// queue between two scrapes still shows at the next one.
func (m *Metrics) observeQueuePeak(peak func() int64) {
	if m == nil {
		return
	}
	_, _ = m.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(m.queuePeak, peak())
		return nil
	}, m.queuePeak)
}

// RowWithheld records one event row of tenant id withheld from one subscriber
// (live or replay) by the role's row-level-security filter, including
// fail-closed evaluations. Labeled by table and role (policy-bounded, not
// data-bounded) so an operator can tell "no matching rows" from "a
// misconfigured filter withholding everything".
func (m *Metrics) RowWithheld(id tenant.ID, table, role string) {
	if m == nil {
		return
	}
	m.withheld.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("table", table), attribute.String("role", role), observability.TenantLabel(id.String())))
}
