// Package app wires the WaveHouse process. New builds every component from
// the boot config and the settings directory, Run drives the long-lived ones
// under one errgroup until the context is cancelled or one of them fails, and
// Close releases what New opened, in reverse order. A stop is two bounded
// phases, each within server.shutdown_timeout: Run drains the in-flight
// request/response work and ingest batches (open SSE streams end at once —
// they are connections to close, not work to finish), then Close releases
// the stores and flushes telemetry under the context its caller passes.
// cmd/wavehouse is the argv/signal/exit-code shell around it;
// tests/integration builds the same wiring against a ClickHouse
// testcontainer.
//
// Each component is wired in one place, as one component value: what New
// opens, what Run loops, and what Close releases. The settings store is
// handed whole to each component's wiring function, which derives the
// per-call getters the internal packages take — so when the one store
// becomes a per-tenant registry (#583), the injection points are those
// wiring functions, not main.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"golang.org/x/sync/errgroup"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
)

// BuildInfo is the ldflags-stamped identity of the binary, served by
// /version and attached to every OTLP log record.
type BuildInfo struct {
	Version   string
	GitCommit string
	BuildTime string
}

// Options is what New needs beyond the settings directory itself.
type Options struct {
	// Config is the boot config, already validated by config.Load.
	Config *config.Config
	// Build stamps /version and the OTLP logger.
	Build BuildInfo
	// LogLevel is the process log level; the OTLP-aware logger that replaces
	// the default one when otel logs are enabled shares it, so WH_LOG_LEVEL
	// keeps applying. Nil means Info.
	LogLevel *slog.LevelVar
	// Listener, when set, is what Run serves the API on instead of binding
	// server.port — a harness's 127.0.0.1:0.
	Listener net.Listener
}

// component is one wired subsystem: an optional long-lived loop and an
// optional release step. run blocks until ctx is done and returns nil on a
// clean stop; close releases what New opened within ctx, the caller's stop
// budget. Either may be nil.
type component struct {
	name  string
	run   func(ctx context.Context) error
	close func(ctx context.Context) error
}

// App is the wired process. Construct with New; the zero value is unusable.
type App struct {
	cfg      *config.Config
	build    BuildInfo
	logLevel *slog.LevelVar
	listener net.Listener

	store       *settings.Store
	policies    policy.Source
	promHandler http.Handler
	ch          *chconn.Manager
	bootState   *api.BootState
	registry    *discovery.SchemaRegistry
	dedup       *dedupe.Managed
	mq          *mq.EmbeddedNATS
	cache       cache.Cache
	sseMetrics  *stream.Metrics
	hub         *stream.Hub
	heartbeater *stream.Heartbeater
	handler     http.Handler

	// components in wiring order; Close walks them backwards.
	components []component
}

// New wires every component. ctx bounds construction only — the boot-time
// schema refresh and the JetStream stream setup; the loops start in Run. A
// failure releases whatever was already opened and returns the error, so
// the caller never holds a half-built App.
func New(ctx context.Context, opts Options) (app *App, err error) {
	a := &App{cfg: opts.Config, build: opts.Build, logLevel: opts.LogLevel, listener: opts.Listener}
	if a.logLevel == nil {
		a.logLevel = &slog.LevelVar{}
	}
	defer func() {
		if err != nil {
			closeCtx, cancel := a.shutdownContext()
			defer cancel()
			_ = a.Close(closeCtx)
		}
	}()

	if err := a.wireSettings(); err != nil {
		return nil, err
	}
	a.wireObservability(ctx)
	if err := a.wireClickHouse(); err != nil {
		return nil, err
	}
	a.wireDiscovery(ctx)
	if err := a.wireDedupe(); err != nil {
		return nil, err
	}
	if err := a.wireMQ(ctx); err != nil {
		return nil, err
	}
	if err := a.wireCache(); err != nil {
		return nil, err
	}
	a.wireSweeper()
	a.wireStreaming()
	a.wireIngestWorker()
	authMW, err := a.wireAuth()
	if err != nil {
		return nil, err
	}
	a.wireReloadTriggers()
	a.wireHTTP(authMW)
	return a, nil
}

// add appends a component; Close releases them in reverse order.
func (a *App) add(c component) { a.components = append(a.components, c) }

// Run starts every component's loop under one errgroup and blocks until ctx
// is cancelled — a clean stop, returning nil once every loop has drained —
// or a component fails, which stops the rest and returns that error. Call
// Close afterwards to release what New opened.
func (a *App) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, c := range a.components {
		if c.run == nil {
			continue
		}
		g.Go(func() error {
			err := c.run(gctx)
			// A stop the caller asked for is clean whatever the loop
			// reported on its way out (a consumer creation cut short, say).
			if err == nil || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%s: %w", c.name, err)
		})
	}
	return g.Wait()
}

// Close releases every resource New opened, newest first, and reports every
// failure joined. ctx is the release budget: the local stores (Pebble,
// ristretto, embedded NATS) close in milliseconds and ignore it, while a
// remote implementation's close is a network round trip that gives up at
// the deadline rather than hanging the exit on a dead peer, and the
// telemetry flush — last, so the lines the other closes log still reach the
// collector — takes whatever is left of it. Safe to call more than once.
func (a *App) Close(ctx context.Context) error {
	var errs []error
	for i := len(a.components) - 1; i >= 0; i-- {
		c := a.components[i]
		if c.close == nil {
			continue
		}
		if err := c.close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", c.name, err))
		}
	}
	a.components = nil
	return errors.Join(errs...)
}

// Handler is the API router, for a harness that serves it itself.
func (a *App) Handler() http.Handler { return a.handler }

// Registry is the schema registry, for a harness that refreshes it after
// creating tables.
func (a *App) Registry() *discovery.SchemaRegistry { return a.registry }

// MQ is the embedded NATS, for a harness that publishes straight onto the
// ingest stream.
func (a *App) MQ() *mq.EmbeddedNATS { return a.mq }
