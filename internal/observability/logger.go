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
// `logging.format` config ("auto"|"text"|"json") resolves through
// ResolveLogFormat to one of these two concrete values.
type LogFormat string

const (
	LogFormatText LogFormat = "text"
	LogFormatJSON LogFormat = "json"
)

// ResolveLogFormat picks the stdout handler. "auto" → text on TTY, JSON
// otherwise. Unknown values fall through to JSON (defense in depth; config
// validation rejects them at boot).
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

func stdoutIsTTY() bool {
	stat, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

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

type componentCtxKey struct{}

// WithComponent stamps a subsystem name onto ctx; TraceHandler.Handle reads
// it back and adds `component=<name>` to every record produced under ctx.
// Call once at the entry of each handler / background worker.
func WithComponent(parent context.Context, name string) context.Context {
	return context.WithValue(parent, componentCtxKey{}, name)
}

func ComponentFromContext(ctx context.Context) string {
	v, _ := ctx.Value(componentCtxKey{}).(string)
	return v
}

// TraceHandler injects trace_id, span_id, and component from the request
// context onto every record.
type TraceHandler struct {
	slog.Handler
}

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

// WithAttrs/WithGroup must re-wrap so chained Logger.With keeps routing
// through Handle. Without them Go promotes to the embedded handler and the
// returned chain silently drops trace-ID injection.
func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{Handler: h.Handler.WithGroup(name)}
}

// otlpSamplerFn returns the per-record sample rate for OTLP log export.
// WARN+ always returns 1.0 — silently dropping errors during incidents is
// worse than the cost of forwarding them. DEBUG/INFO use otlpSampleRate.
func otlpSamplerFn(otlpSampleRate float64) func(context.Context, slog.Record) float64 {
	return func(_ context.Context, r slog.Record) float64 {
		if r.Level >= slog.LevelWarn {
			return 1.0
		}
		return otlpSampleRate
	}
}

// NewBootstrapLogger is a stdout-only logger for pre-OTLP boot (config load,
// OTel init failures). NewLogger replaces it once InitProvider succeeds.
func NewBootstrapLogger(component string, format LogFormat, level *slog.LevelVar) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}
	handler := &TraceHandler{Handler: newConsoleHandler(format, opts)}
	return slog.New(handler).With("service", component)
}

// NewLogger fans out to stdout + OTLP. Stdout receives 100%; otlpSampleRate
// (in [0.0, 1.0]) caps OTLP DEBUG/INFO export. WARN/ERROR always export at
// 100% via otlpSamplerFn — non-configurable.
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
