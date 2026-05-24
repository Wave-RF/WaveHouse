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

	log := NewLogger("api", lvl, LogFormatJSON, 0.10)
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
	log := NewLogger("worker", lvl, LogFormatText, 0.10)
	require.NotNil(t, log)
	log.Debug("debug-msg")
	log.Warn("warn-msg")
}

func TestResolveLogFormat(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want LogFormat
	}{
		{"text", LogFormatText},
		{"TEXT", LogFormatText},
		{" Text ", LogFormatText},
		{"json", LogFormatJSON},
		{"JSON", LogFormatJSON},
		// "auto" resolves dynamically based on whether stdout is a TTY when
		// the test runs. In `go test` without a -v terminal, stdout is a
		// pipe, so we get JSON. Asserting concrete TTY behavior would
		// require swapping os.Stdout, which is more setup than the value
		// adds — we instead assert resolution is one of the two known
		// values.
		// The explicit non-auto cases above cover the determinism.
		{"unknown-thing", LogFormatJSON}, // defense-in-depth fallthrough
		{"", LogFormatJSON},              // empty string → auto → tested separately
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got := ResolveLogFormat(tc.in)
			if tc.in == "" || tc.in == "auto" {
				assert.True(t, got == LogFormatText || got == LogFormatJSON, "auto should resolve to text or json, got %q", got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNewBootstrapLogger(t *testing.T) {
	t.Parallel()

	lvl := &slog.LevelVar{}
	lvl.Set(slog.LevelDebug)
	// Bootstrap logger must produce a non-nil *slog.Logger with the requested
	// component attached, and must not panic for either format. We don't
	// inspect stdout because the JSON handler writes there directly.
	for _, format := range []LogFormat{LogFormatText, LogFormatJSON} {
		log := NewBootstrapLogger("boot", format, lvl)
		require.NotNil(t, log)
		log.Info("bootstrap message", "format", format)
	}
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

	cases := []struct {
		name  string
		rate  float64
		level slog.Level
	}{
		{"debug rate=0.0", 0.0, slog.LevelDebug},
		{"info rate=0.0", 0.0, slog.LevelInfo},
		{"debug rate=0.1", 0.1, slog.LevelDebug},
		{"info rate=0.1", 0.1, slog.LevelInfo},
		{"debug rate=0.5", 0.5, slog.LevelDebug},
		{"info rate=0.5", 0.5, slog.LevelInfo},
		{"debug rate=1.0", 1.0, slog.LevelDebug},
		{"info rate=1.0", 1.0, slog.LevelInfo},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := otlpSamplerFn(tc.rate)
			r := slog.NewRecord(time.Time{}, tc.level, "msg", 0)
			got := s(context.Background(), r)
			assert.InDelta(t, tc.rate, got, 1e-9)
		})
	}
}

// Regression: NewLogger ends with `.With("component", ...)`, which calls
// Handler.WithAttrs. If TraceHandler doesn't implement WithAttrs/WithGroup,
// promotion to the embedded interface returns the unwrapped inner handler and
// stdout trace-ID injection silently breaks.
func TestTraceHandler_WithAttrsPreservesWrapping(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: base}

	ctx, span := newTestTracer(t).Start(context.Background(), "op")
	defer span.End()

	log := slog.New(h).With("component", "test")
	log.InfoContext(ctx, "hello")

	out := buf.String()
	assert.Contains(t, out, "trace_id=")
	assert.Contains(t, out, "span_id=")
	assert.Contains(t, out, "component=test")
}

func TestTraceHandler_WithGroupPreservesWrapping(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: base}

	ctx, span := newTestTracer(t).Start(context.Background(), "op")
	defer span.End()

	log := slog.New(h).WithGroup("g")
	log.InfoContext(ctx, "hello")

	out := buf.String()
	assert.Contains(t, out, "trace_id=")
	assert.Contains(t, out, "span_id=")
}

func TestWithComponent_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := WithComponent(context.Background(), "api/ingest")
	assert.Equal(t, "api/ingest", ComponentFromContext(ctx))
	// No-component context returns "".
	assert.Empty(t, ComponentFromContext(context.Background()))
}

func TestTraceHandler_InjectsComponentFromContext(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: base}

	ctx := WithComponent(context.Background(), "api/ingest")
	slog.New(h).InfoContext(ctx, "hello")

	out := buf.String()
	assert.Contains(t, out, "component=api/ingest")
}

func TestTraceHandler_NoComponentWhenContextLacks(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: base}

	slog.New(h).InfoContext(context.Background(), "hello")
	assert.NotContains(t, buf.String(), "component=")
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
