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
// same value, so the process still exits as it would have (#902). Defer it
// first in the goroutine's function, directly (recover only stops a panic
// when the deferred function itself calls it):
//
//	go func() {
//		defer observability.Crash("ingest_worker", slog.String("table", table))
//		...
//	}()
//
// On a panic it logs one ERROR, "panic in background goroutine", through the
// default logger with component, error, stack and attrs (the tenant or table
// the goroutine serves, where it knows them), and tries to flush the OTLP
// log exporter for up to crashFlushWait so the record reaches OTLP too; that
// is best effort (the error is ignored: stdout has the record). Without a
// panic it does nothing. It never carries on after one: a goroutine that
// panicked may have left the process's state inconsistent, so exiting is
// right; Go's own panic trace still follows on stderr.
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
