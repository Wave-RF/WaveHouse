// The profiling listener, apart from wire.go so the HTTP wiring there only
// gains its call (#143).

package app

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// wireProfiling adds, with profiling.enabled, the net/http/pprof listener on
// profiling.addr. Every role serves it: a worker leaks memory as readily as
// an API process.
func (a *App) wireProfiling() {
	p := a.cfg.Profiling
	if !p.Enabled {
		return
	}
	// Cancelled as the drain begins: a CPU profile or trace sleeps on its
	// request's context, so one in flight ends there instead of holding the
	// stop to server.shutdown_timeout.
	base, cancel := context.WithCancel(context.Background())
	srv := &http.Server{
		Addr:              p.Addr,
		Handler:           observability.PprofHandler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       30 * time.Second,
		// No WriteTimeout: /debug/pprof/profile and /trace write for the
		// ?seconds= asked, and pprof refuses a duration past the timeout.
		BaseContext: func(net.Listener) context.Context { return base },
	}
	srv.RegisterOnShutdown(cancel)
	a.add(component{name: "profiling server", run: func(ctx context.Context) error {
		defer cancel()
		// Not fatal, like the metrics sidecar: the process serves without it.
		if err := a.serve(ctx, "profiling server", srv, nil); err != nil {
			slog.Error("profiling server error", "error", err)
		}
		return nil
	}})
}
