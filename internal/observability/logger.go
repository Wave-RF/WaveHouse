package observability

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"

	slogmulti "github.com/samber/slog-multi"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// TraceHandler handles Trace Correlation and Stack Traces
type TraceHandler struct {
	slog.Handler
}

func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	// Attach Trace IDs if a span exists in context
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		r.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}

	// Attach Stack Trace for Errors
	if r.Level >= slog.LevelError && h.Handler.Enabled(ctx, slog.LevelDebug) {
		r.AddAttrs(slog.String("stacktrace", string(debug.Stack())))
	}

	return h.Handler.Handle(ctx, r)
}

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

	handler := slogmulti.Fanout(
		consoleHandler,
		otelHandler,
	)

	return slog.New(&TraceHandler{handler}).With("component", component)
}
