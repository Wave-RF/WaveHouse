package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lmittmann/tint"
	slogmulti "github.com/samber/slog-multi"
	slogsampling "github.com/samber/slog-sampling"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// LogFormat is the resolved stdout handler format. The user-facing
// `logging.format` config value ("auto" | "text" | "json") goes through
// ResolveLogFormat to produce one of these two concrete choices.
type LogFormat string

const (
	LogFormatText LogFormat = "text"
	LogFormatJSON LogFormat = "json"
)

// ResolveLogFormat translates the user-facing format string into a concrete
// handler choice. "auto" picks LogFormatText when stdout is a TTY and
// LogFormatJSON otherwise — that gives `make dev` colored output in a
// terminal while a containerized prod deployment (whose stdout is captured
// by a log shipper) keeps machine-readable JSON. Unknown values fall through
// to LogFormatJSON since the safe default for production is the machine
// format. Validation in config.Validate rejects unknown values at boot, so
// this fallthrough is defense-in-depth, not a user-visible behavior.
func ResolveLogFormat(configValue string) LogFormat {
	switch strings.ToLower(strings.TrimSpace(configValue)) {
	case "text":
		return LogFormatText
	case "json":
		return LogFormatJSON
	case "auto", "":
		if stdoutIsTTY() {
			return LogFormatText
		}
		return LogFormatJSON
	default:
		return LogFormatJSON
	}
}

// stdoutIsTTY reports whether os.Stdout is connected to a terminal. Uses the
// portable os.ModeCharDevice check rather than pulling in mattn/go-isatty or
// golang.org/x/term as a direct dependency.
func stdoutIsTTY() bool {
	stat, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

// newConsoleHandler builds the stdout handler for the given format. JSON
// uses the stdlib slog handler; text uses tint for level coloring and
// human-friendly attribute rendering.
func newConsoleHandler(format LogFormat, opts *slog.HandlerOptions) slog.Handler {
	switch format {
	case LogFormatText:
		return tint.NewHandler(os.Stdout, &tint.Options{
			Level:      opts.Level,
			AddSource:  opts.AddSource,
			TimeFormat: time.RFC3339,
		})
	default:
		return slog.NewJSONHandler(os.Stdout, opts)
	}
}

// componentCtxKey is the (unexported) context key under which a subsystem
// component name is stashed. WithComponent stores; TraceHandler.Handle reads.
// Unexported so callers must go through the WithComponent helper, which keeps
// the key type private and prevents accidental collisions with other ctx values.
type componentCtxKey struct{}

// WithComponent returns a context derived from parent that carries the given
// subsystem name. TraceHandler.Handle stamps it onto every log record produced
// under this context as `component=<name>`. Call at the entry point of each
// handler or background worker (e.g. "api/ingest", "ingest/sweeper") so log
// queries can filter on the producing subsystem.
//
// Pairs with the `service=wavehouse` attribute on the root logger — service
// identifies the process, component identifies the area within it. JSON
// consumers see both fields side by side.
func WithComponent(parent context.Context, name string) context.Context {
	return context.WithValue(parent, componentCtxKey{}, name)
}

// ComponentFromContext returns the subsystem name previously stashed via
// WithComponent, or "" if none. Useful in middleware that wants to log under
// a different component name than the request scope.
func ComponentFromContext(ctx context.Context) string {
	v, _ := ctx.Value(componentCtxKey{}).(string)
	return v
}

// TraceHandler intercepts every log call to inject OpenTelemetry IDs and the
// per-request component name from context (see WithComponent).
type TraceHandler struct {
	slog.Handler
}

// Handle pulls the trace_id, span_id, and component from the context if set.
func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		r.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	if comp := ComponentFromContext(ctx); comp != "" {
		r.AddAttrs(slog.String("component", comp))
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

// NewBootstrapLogger returns a stdout-only logger for the pre-OTLP phase of
// boot (config load, OTel init failures). The returned logger honors the
// format selection but does not fan out to OTLP — NewLogger handles that
// once InitProvider has succeeded.
func NewBootstrapLogger(component string, format LogFormat, level *slog.LevelVar) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}
	handler := &TraceHandler{Handler: newConsoleHandler(format, opts)}
	return slog.New(handler).With("service", component)
}

// NewLogger creates a production-ready logger with component tags and trace
// support. The stdout handler is selected by `format` (TEXT for human-readable
// colored output, JSON for log-shipper consumption). otlpSampleRate (in
// [0.0, 1.0]) caps OTLP export of DEBUG/INFO records; WARN/ERROR always
// export at 100% — dropping them silently during incidents is too dangerous
// to expose as a knob. Stdout receives 100% of records regardless.
func NewLogger(component string, level *slog.LevelVar, format LogFormat, otlpSampleRate float64) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	consoleHandler := newConsoleHandler(format, opts)
	otelHandler := otelslog.NewHandler(component, otelslog.WithLoggerProvider(global.GetLoggerProvider()))

	sampler := slogsampling.CustomSamplingOption{
		Sampler: otlpSamplerFn(otlpSampleRate),
	}.NewMiddleware()

	handler := slogmulti.Fanout(
		consoleHandler,
		slogmulti.Pipe(sampler).Handler(otelHandler),
	)

	handler = &TraceHandler{handler}
	return slog.New(handler).With("service", component)
}
