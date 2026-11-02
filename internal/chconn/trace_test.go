package chconn_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/chconn/chconntest"
)

func spanContext(flags trace.TraceFlags) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: flags,
	})
}

// A caller's span context, as the global tracer passes it through with
// tracing off, is not sent on either protocol.
func TestTraceContext_RemoteSpanIsNotSent(t *testing.T) {
	t.Parallel()
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), spanContext(trace.FlagsSampled))

	span, _ := chconntest.QueryOptions(chconn.WithTraceContext(ctx), t)
	assert.False(t, span.IsValid())

	h := http.Header{"Traceparent": {"configured"}, "Tracestate": {"configured=1"}}
	chconn.InjectTraceContext(ctx, h)
	assert.Equal(t, http.Header{"Traceparent": {"configured"}, "Tracestate": {"configured=1"}}, h)
}

// The native protocol's query context carries the span in ctx, and keeps the
// settings already on it.
func TestWithTraceContext(t *testing.T) {
	t.Parallel()
	sc := spanContext(trace.FlagsSampled)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"max_result_rows": 10}))
	ctx = chconn.WithTraceContext(trace.ContextWithSpanContext(ctx, sc))

	span, settings := chconntest.QueryOptions(ctx, t)
	assert.Equal(t, sc, span)
	assert.Equal(t, 10, settings["max_result_rows"], "the settings already on the context are kept")
}

// No span, no trace context: the context is returned as it came.
func TestWithTraceContext_NoSpan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert.Equal(t, ctx, chconn.WithTraceContext(ctx))
	span, _ := chconntest.QueryOptions(chconn.WithTraceContext(ctx), t)
	assert.False(t, span.IsValid())
}

// The HTTP interface gets a traceparent for the span in ctx, which wins over
// a configured header of the same name, and no baggage.
func TestInjectTraceContext(t *testing.T) {
	t.Parallel()
	m, err := baggage.NewMember("tenant", "acme")
	require.NoError(t, err)
	b, err := baggage.New(m)
	require.NoError(t, err)
	ctx := baggage.ContextWithBaggage(context.Background(), b)

	h := http.Header{}
	h.Set("Traceparent", "configured")
	h.Set("Tracestate", "configured=1")
	chconn.InjectTraceContext(trace.ContextWithSpanContext(ctx, spanContext(trace.FlagsSampled)), h)
	assert.Equal(t, "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01", h.Get("traceparent"))
	assert.Empty(t, h.Values("tracestate"), "a configured tracestate goes with the traceparent it replaced")
	assert.Empty(t, h.Get("baggage"))

	h = http.Header{}
	chconn.InjectTraceContext(trace.ContextWithSpanContext(ctx, spanContext(0)), h)
	assert.Equal(t, "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-00", h.Get("traceparent"), "an unsampled span is sent unsampled")

	ts, err := trace.ParseTraceState("vendor=value")
	require.NoError(t, err)
	h = http.Header{"Tracestate": {"configured=1"}}
	chconn.InjectTraceContext(trace.ContextWithSpanContext(ctx, spanContext(trace.FlagsSampled).WithTraceState(ts)), h)
	assert.Equal(t, []string{"vendor=value"}, h.Values("tracestate"), "a span's tracestate replaces the configured one")

	h = http.Header{"Traceparent": {"configured"}}
	chconn.InjectTraceContext(ctx, h)
	assert.Equal(t, http.Header{"Traceparent": {"configured"}}, h, "no span, no change")
}
