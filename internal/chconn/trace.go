package chconn

import (
	"context"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// WithTraceContext returns ctx with its span context set as the query's
// trace context, which the native protocol sends in the query packet, so a
// ClickHouse server with OpenTelemetry tracing on can join its spans for the
// query to the engine's trace. ctx with no span of WaveHouse's own is
// returned unchanged (see ownSpan). Other query options already on ctx, such
// as settings, are kept.
func WithTraceContext(ctx context.Context) context.Context {
	sc, ok := ownSpan(ctx)
	if !ok {
		return ctx
	}
	return clickhouse.Context(ctx, clickhouse.WithSpan(sc))
}

// InjectTraceContext sets the traceparent (and any tracestate) header for
// the span in ctx on h, the HTTP-interface counterpart of WithTraceContext.
// Only the W3C trace context is sent, not baggage. ctx with no span of
// WaveHouse's own sets nothing (see ownSpan). Call it after any configured
// headers: it replaces both names, so a configured tracestate is not left
// beside another trace's traceparent.
func InjectTraceContext(ctx context.Context, h http.Header) {
	if _, ok := ownSpan(ctx); !ok {
		return
	}
	h.Del("Tracestate")
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(h))
}

// ownSpan is the span context in ctx when WaveHouse opened that span. A
// remote one is a caller's, passed through unchanged: with tracing off, the
// global tracer's spans carry the incoming traceparent of any client, trusted
// proxy or not, and forwarding it would let that client choose the trace
// ClickHouse records under.
func ownSpan(ctx context.Context) (trace.SpanContext, bool) {
	sc := trace.SpanContextFromContext(ctx)
	return sc, sc.IsValid() && !sc.IsRemote()
}
