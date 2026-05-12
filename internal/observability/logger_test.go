package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

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

	log := NewLogger("api", lvl, true, 0.10)
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
	log := NewLogger("worker", lvl, false, 0.10)
	require.NotNil(t, log)
	log.Debug("debug-msg")
	log.Warn("warn-msg")
}

func TestTraceHandler_AddsTraceIDs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: base}

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

func TestOTLPSamplerFn_WarnFloor(t *testing.T) {
	t.Parallel()

	// At the most aggressive setting (rate=0.0) WARN+ must still report
	// 1.0 — this is the safety floor that makes the configurable rate
	// safe to expose. If this ever returns the rate for WARN/ERROR,
	// production loses error visibility silently.
	s := otlpSamplerFn(0.0)

	cases := []struct {
		name  string
		level slog.Level
		want  float64
	}{
		{"debug dropped", slog.LevelDebug, 0.0},
		{"info dropped", slog.LevelInfo, 0.0},
		{"warn floored", slog.LevelWarn, 1.0},
		{"error floored", slog.LevelError, 1.0},
		{"above-error floored", slog.LevelError + 4, 1.0}, // FATAL-ish
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := slog.NewRecord(time.Time{}, tc.level, "msg", 0)
			got := s(context.Background(), r)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}

func TestOTLPSamplerFn_PassesThroughRateForBelowWarn(t *testing.T) {
	t.Parallel()

	for _, rate := range []float64{0.0, 0.1, 0.5, 1.0} {
		s := otlpSamplerFn(rate)
		for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo} {
			r := slog.NewRecord(time.Time{}, level, "msg", 0)
			got := s(context.Background(), r)
			assert.InDelta(t, rate, got, 1e-9, "rate=%v level=%v", rate, level)
		}
	}
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
