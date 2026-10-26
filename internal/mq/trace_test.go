package mq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// recordSpans installs a span recorder as the global tracer provider, and the
// W3C propagator, until the test ends. Like every test that calls it, the
// caller must not run in parallel.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	return rec
}

// publishSpans are the ended mq.publish spans rec holds.
func publishSpans(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "mq.publish" {
			out = append(out, s)
		}
	}
	return out
}

// assertPublishSpan checks s is a producer span of kind under parent, after
// attempts tries.
func assertPublishSpan(t *testing.T, s sdktrace.ReadOnlySpan, parent trace.SpanContext, kind string, attempts int) {
	t.Helper()
	assert.Equal(t, trace.SpanKindProducer, s.SpanKind())
	assert.Equal(t, parent.SpanID(), s.Parent().SpanID(), "a child of the caller's span")
	assert.Equal(t, parent.TraceID(), s.SpanContext().TraceID())
	attrs := s.Attributes()
	assert.Contains(t, attrs, attribute.String("messaging.system", "nats"))
	assert.Contains(t, attrs, attribute.String("messaging.operation", "publish"))
	assert.Contains(t, attrs, attribute.String("kind", kind))
	assert.Contains(t, attrs, attribute.Int("attempts", attempts))
	assert.Equal(t, codes.Unset, s.Status().Code)
}

// An embedded publish is one producer span under the caller's, and the
// delivered message carries that span, not the caller's, so the flush links
// to the publish (#897). A dead-letter park is a span of its own kind.
func TestEmbeddedNATS_PublishSpan(t *testing.T) {
	rec := recordSpans(t)
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	got := make(chan trace.SpanContext, 1)
	require.NoError(t, e.Subscribe(ctx, "span-consumer", func(msg *Message) error {
		_ = msg.Ack()
		select {
		case got <- trace.SpanContextFromContext(msg.Ctx):
		default:
		}
		return nil
	}))

	pctx, parent := otel.Tracer("test").Start(ctx, "request")
	topic := Topic{Tenant: tenant.Default, Table: "default.events"}
	require.NoError(t, e.Publish(pctx, topic, []byte("x")))
	parent.End()

	spans := publishSpans(rec)
	require.Len(t, spans, 1)
	assertPublishSpan(t, spans[0], parent.SpanContext(), publishKindIngest, 1)

	select {
	case sc := <-got:
		assert.Equal(t, spans[0].SpanContext().SpanID(), sc.SpanID(), "the message carries the publish span")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	require.NoError(t, e.DeadLetter(pctx, NewMessage(pctx, topic, []byte("y"), time.Now(), nil, nil, nil)))
	spans = publishSpans(rec)
	require.Len(t, spans, 2)
	assertPublishSpan(t, spans[1], parent.SpanContext(), publishKindDLQ, 1)
}

// A failed publish sets error status and records the error; one whose caller
// went away is not a failure.
func TestEndPublishSpan_Status(t *testing.T) {
	rec := recordSpans(t)
	boom := errors.New("boom")

	_, span := startPublishSpan(t.Context(), publishKindIngest)
	endPublishSpan(t.Context(), span, 2, boom)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, span = startPublishSpan(canceled, publishKindIngest)
	endPublishSpan(canceled, span, 1, context.Canceled)

	spans := rec.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	assert.Equal(t, "boom", spans[0].Status().Description)
	require.Len(t, spans[0].Events(), 1, "the error is recorded")
	assert.Equal(t, codes.Unset, spans[1].Status().Code)
	assert.Empty(t, spans[1].Events())
}

// A publish that retries after its stream went away counts both attempts, and
// one that cannot be stored marks its span failed with the error recorded.
func TestEmbeddedNATS_PublishSpanAttemptsAndFailure(t *testing.T) {
	rec := recordSpans(t)
	// globex keeps the streams directory occupied across acme's reopen (see
	// TestEmbeddedNATS_Publish_RestartsDeliveryOnAReopenedQueue's note on
	// nats-server#8725).
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	acme := Topic{Tenant: "acme", Table: "t"}
	require.NoError(t, e.Publish(ctx, acme, []byte("x")))
	for _, name := range []string{"INGEST_acme", "DLQ_acme"} {
		require.NoError(t, e.js.DeleteStream(ctx, name))
	}
	rec.Reset()
	require.NoError(t, e.Publish(ctx, acme, []byte("y")))
	spans := publishSpans(rec)
	require.Len(t, spans, 1)
	assert.Contains(t, spans[0].Attributes(), attribute.Int("attempts", 2))
	assert.Equal(t, codes.Unset, spans[0].Status().Code)

	rec.Reset()
	require.ErrorIs(t, e.Publish(ctx, Topic{Tenant: "initech", Table: "t"}, []byte("z")), ErrQueueFull)
	spans = publishSpans(rec)
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	require.Len(t, spans[0].Events(), 1, "the error is recorded")
}

// The publishes of a batch are one span, carried by every message, with the
// batch's size and the attempts its publishes made; a dead-letter park under
// it is still a span of its own.
func TestStartPublishBatch(t *testing.T) {
	rec := recordSpans(t)
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	got := make(chan trace.SpanContext, 3)
	require.NoError(t, e.Subscribe(ctx, "batch-consumer", func(msg *Message) error {
		_ = msg.Ack()
		got <- trace.SpanContextFromContext(msg.Ctx)
		return nil
	}))

	pctx, parent := otel.Tracer("test").Start(ctx, "request")
	bctx, end := StartPublishBatch(pctx, 3)
	topic := Topic{Tenant: tenant.Default, Table: "default.events"}
	for _, data := range []string{"a", "b", "c"} {
		require.NoError(t, e.Publish(bctx, topic, []byte(data)))
	}
	require.NoError(t, e.DeadLetter(bctx, NewMessage(bctx, topic, []byte("d"), time.Now(), nil, nil, nil)))
	end(nil)
	parent.End()

	spans := publishSpans(rec)
	require.Len(t, spans, 2)
	dlq, batch := spans[0], spans[1]
	assertPublishSpan(t, batch, parent.SpanContext(), publishKindIngest, 3)
	assert.Contains(t, batch.Attributes(), attribute.Int("messaging.batch.message_count", 3))
	assert.Contains(t, dlq.Attributes(), attribute.String("kind", publishKindDLQ))
	assert.Equal(t, batch.SpanContext().SpanID(), dlq.Parent().SpanID())

	for range 3 {
		select {
		case sc := <-got:
			assert.Equal(t, batch.SpanContext().SpanID(), sc.SpanID(), "each message carries the batch's span")
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for delivery")
		}
	}
}
