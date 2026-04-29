package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// newTestTracer returns a locally-scoped tracer that always samples. Using
// the global tracer here would be flaky because other tests in this package
// (InitProvider) swap the global for a ratio-sampled one.
func newTestTracer(_ *testing.T) trace.Tracer {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	return tp.Tracer("observability-test")
}

func TestNewLogger_JSON(t *testing.T) {
	t.Parallel()

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelDebug)

	log := NewLogger("api", lvl, true)
	require.NotNil(t, log)
	// Component tag is added via With; confirm the attribute is present by
	// emitting a record and re-parsing it through a capturing handler isn't
	// practical — the fanout writes to stdout. Instead, just confirm the
	// factory returns a usable *slog.Logger.
	log.Info("hello")
}

func TestNewLogger_Text(t *testing.T) {
	t.Parallel()

	lvl := &slog.LevelVar{}
	log := NewLogger("worker", lvl, false)
	require.NotNil(t, log)
	log.Debug("debug-msg")
	log.Warn("warn-msg")
}

// capturingHandler records the last slog.Record it saw for assertions.
type capturingHandler struct {
	slog.Handler
	last slog.Record
}

func (c *capturingHandler) Handle(ctx context.Context, r slog.Record) error {
	c.last = r
	return c.Handler.Handle(ctx, r)
}

func TestTraceHandler_AddsTraceIDs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	recorder := &capturingHandler{Handler: base}
	h := &TraceHandler{Handler: recorder}

	ctx, span := newTestTracer(t).Start(context.Background(), "op")
	defer span.End()

	log := slog.New(h)
	log.InfoContext(ctx, "hello")

	out := buf.String()
	assert.Contains(t, out, "trace_id=")
	assert.Contains(t, out, "span_id=")
	assert.Contains(t, out, span.SpanContext().TraceID().String())
}

func TestTraceHandler_NoSpan(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: base}

	log := slog.New(h)
	log.InfoContext(context.Background(), "hello")

	out := buf.String()
	// Without an active span, the handler should not fabricate trace/span IDs.
	assert.False(t, strings.Contains(out, "trace_id="), "unexpected trace_id in %q", out)
}

func TestTraceHandler_InvalidSpan(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, nil)
	h := &TraceHandler{Handler: base}

	// Manually install an invalid span context in the context. IsValid() returns
	// false when either TraceID or SpanID is zero.
	sc := trace.NewSpanContext(trace.SpanContextConfig{})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	log := slog.New(h)
	log.InfoContext(ctx, "hello")
	assert.NotContains(t, buf.String(), "trace_id=")
}
