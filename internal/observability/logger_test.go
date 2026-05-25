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

// newTestTracer is a locally-scoped, always-sampling tracer — the global
// would be flaky because InitProvider tests swap it for ratio-sampled.
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
		// "auto" resolves at runtime based on stdout TTY — `go test` pipes,
		// so we get JSON. Assert membership only for "auto"/"".
		{"unknown-thing", LogFormatJSON},
		{"", LogFormatJSON},
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
	for _, format := range []LogFormat{LogFormatText, LogFormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			log := NewBootstrapLogger("boot", format, lvl)
			require.NotNil(t, log)
			log.Info("bootstrap message", "format", format)
		})
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

	assert.False(t, strings.Contains(buf.String(), "trace_id="))
}

func TestOTLPSamplerFn_WarnFloor(t *testing.T) {
	t.Parallel()

	// rate=0.0 is the most aggressive setting; WARN+ must still return 1.0.
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

// Regression: without WithAttrs/WithGroup overrides, Logger.With promotes
// to the embedded handler and trace-ID injection silently breaks.
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

	// Zero TraceID/SpanID → IsValid() is false → no injection.
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{}))
	slog.New(h).InfoContext(ctx, "hello")
	assert.NotContains(t, buf.String(), "trace_id=")
}
