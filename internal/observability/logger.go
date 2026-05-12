package observability

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"

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

	// Attach Stack Trace for Errors
	if r.Level >= slog.LevelError {
		r.AddAttrs(slog.String("stacktrace", string(debug.Stack())))
	}

	return h.Handler.Handle(ctx, r)
}

// NewLogger creates a production-ready logger with component tags and trace support
func NewLogger(component string, level *slog.LevelVar, isJSON bool) *slog.Logger {
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

	sampler := slogsampling.UniformSamplingOption{
		Rate: 0.1, // Keep 10% of logs
	}.NewMiddleware()

	handler := slogmulti.Fanout(
		consoleHandler,
		slogmulti.Pipe(sampler).Handler(otelHandler),
	)

	handler = &TraceHandler{handler}
	return slog.New(handler).With("component", component)
}
