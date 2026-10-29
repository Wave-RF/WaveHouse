package observability

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel"
)

// crashFlushWait bounds Crash's flush of the OTLP log exporter, so a collector
// that does not answer cannot hold up the exit.
const crashFlushWait = 2 * time.Second

// Crash records a panic in a long-lived goroutine, then panics again with the
// same value: the goroutine may have left the process inconsistent, so the
// process must still exit (#902). The record and its flush are described in
// the deployment guide's Panics section. Defer it first in the goroutine's
// function, directly, since recover only stops a panic there:
//
//	go func() {
//		defer observability.Crash("ingest_worker", slog.String("table", table))
//		...
//	}()
func Crash(component string, attrs ...slog.Attr) {
	r := recover()
	if r == nil {
		return
	}
	rec := make([]slog.Attr, 0, len(attrs)+3)
	rec = append(rec,
		slog.String("component", component),
		slog.String("error", fmt.Sprint(r)),
		slog.String("stack", string(debug.Stack())))
	rec = append(rec, attrs...)
	slog.LogAttrs(context.Background(), slog.LevelError, "panic in background goroutine", rec...)
	if f, ok := otel.GetLoggerProvider().(interface{ ForceFlush(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(context.Background(), crashFlushWait)
		_ = f.ForceFlush(ctx)
		cancel()
	}
	panic(r)
}
