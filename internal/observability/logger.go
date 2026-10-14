package observability

import (
	"context"
	"log/slog"
	"os"

	slogmulti "github.com/samber/slog-multi"
	slogsampling "github.com/samber/slog-sampling"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TraceHandler intercepts every log call to inject OpenTelemetry IDs and the
// request's correlation fields.
type TraceHandler struct {
	slog.Handler
}

// Handle pulls the trace_id and span_id from the context if they exist, and
// request_id and tenant from the request scope (WithRequest) when there is one.
func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	addRequestAttrs(ctx, &r)
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

// levelGate drops records below level before they reach the wrapped handler.
// The OTLP leg needs it because neither the otelslog bridge nor the log SDK
// filters by level; reading the shared LevelVar keeps it in step with stdout.
type levelGate struct {
	level   slog.Leveler
	handler slog.Handler
}

func (g *levelGate) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= g.level.Level() && g.handler.Enabled(ctx, l)
}

func (g *levelGate) Handle(ctx context.Context, r slog.Record) error {
	return g.handler.Handle(ctx, r)
}

func (g *levelGate) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelGate{level: g.level, handler: g.handler.WithAttrs(attrs)}
}

func (g *levelGate) WithGroup(name string) slog.Handler {
	return &levelGate{level: g.level, handler: g.handler.WithGroup(name)}
}

// otlpLeg is the OTLP branch of the fan-out: the level gate, then the
// sampler, then the exporter handler.
func otlpLeg(exporter slog.Handler, level slog.Leveler, otlpSampleRate float64) slog.Handler {
	sampler := slogsampling.CustomSamplingOption{
		Sampler: otlpSamplerFn(otlpSampleRate),
	}.NewMiddleware()
	return &levelGate{level: level, handler: slogmulti.Pipe(sampler).Handler(exporter)}
}

// NewLogger creates a production-ready logger with component tags and trace
// support. level gates both stdout and OTLP. otlpSampleRate (in [0.0, 1.0])
// caps OTLP export of the DEBUG/INFO records level admits; WARN/ERROR always
// export at 100% — dropping them silently during incidents is too dangerous to
// expose as a knob. Stdout receives 100% of the records level admits.
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

	otelHandler := otelslog.NewHandler(component, otelslog.WithLoggerProvider(otel.GetLoggerProvider()))

	handler := slogmulti.Fanout(
		consoleHandler,
		otlpLeg(otelHandler, level, otlpSampleRate),
	)

	handler = &TraceHandler{handler}
	return slog.New(handler).With("component", component)
}
