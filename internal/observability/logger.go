package observability

import (
	"context"
	"log/slog"
	"os"

	slogmulti "github.com/samber/slog-multi"
	slogsampling "github.com/samber/slog-sampling"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// TraceHandler intercepts every log call to inject OpenTelemetry IDs
type TraceHandler struct {
	slog.Handler
}

// Handle pulls the trace_id and span_id from the context if they exist
func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		r.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}

	return h.Handler.Handle(ctx, r)
}

// WithAttrs and WithGroup must re-wrap so chained Logger.With / WithGroup calls
// keep going through TraceHandler.Handle. Without these, Go promotes the calls
// to the embedded slog.Handler and the returned handler is the unwrapped inner
// handler — silently dropping stdout trace-ID injection.
func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{Handler: h.Handler.WithGroup(name)}
}

// otlpSamplerFn returns the per-record sample rate for the OTLP log exporter.
// WARN+ records always return 1.0 (non-configurable safety floor — silently
// dropping errors during incidents would be a worse failure mode than the
// cost of forwarding them all). DEBUG/INFO records return otlpSampleRate.
// Extracted from NewLogger so the policy is unit-testable independently of
// the surrounding slogmulti / slogsampling plumbing.
func otlpSamplerFn(otlpSampleRate float64) func(context.Context, slog.Record) float64 {
	return func(_ context.Context, r slog.Record) float64 {
		if r.Level >= slog.LevelWarn {
			return 1.0
		}
		return otlpSampleRate
	}
}

// NewLogger creates a production-ready logger with component tags and trace
// support. otlpSampleRate (in [0.0, 1.0]) caps OTLP export of DEBUG/INFO
// records; WARN/ERROR always export at 100% — dropping them silently during
// incidents is too dangerous to expose as a knob. Stdout receives 100% of
// records regardless.
func NewLogger(component string, level *slog.LevelVar, isJSON bool, otlpSampleRate float64) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	var consoleHandler slog.Handler
	if isJSON {
		consoleHandler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		consoleHandler = slog.NewTextHandler(os.Stdout, opts)
	}

	otelHandler := otelslog.NewHandler(component, otelslog.WithLoggerProvider(global.GetLoggerProvider()))

	sampler := slogsampling.CustomSamplingOption{
		Sampler: otlpSamplerFn(otlpSampleRate),
	}.NewMiddleware()

	handler := slogmulti.Fanout(
		consoleHandler,
		slogmulti.Pipe(sampler).Handler(otelHandler),
	)

	handler = &TraceHandler{handler}
	return slog.New(handler).With("component", component)
}
