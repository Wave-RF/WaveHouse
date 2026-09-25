// Package app wires the WaveHouse process. New builds every component from
// the boot config and the settings directory, Run drives the long-lived ones
// under one errgroup until the context is cancelled or one of them fails, and
// Close releases what New opened, in reverse order. A stop is three bounded
// phases, and their budgets add rather than multiply: Run drains the
// in-flight request/response work and ingest batches within
// server.shutdown_timeout (open SSE streams end at once — they are
// connections to close, not work to finish), then Close releases the stores
// under the context its caller passes (ReleaseTimeout), then flushes
// telemetry under a short budget of its own so the flush that reports on
// the stop is never starved by a slow close.
// cmd/wavehouse is the argv/signal/exit-code shell around it;
// tests/integration builds the same wiring against a ClickHouse
// testcontainer.
//
// Each component is wired in one place, as one component value: what New
// opens, what Run loops, and what Close releases. The settings registry is
// handed whole to each component's wiring function, which derives the
// per-call getters the internal packages take: keyed by the request's store
// for the handlers, by tenant id for the async paths (perTenant), and fixed
// to the default tenant for the ops gate of a flat directory (defaultPolicy).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

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
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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

	// tenants is the registry every tenant-aware path resolves through, and
	// the owner of every reload.
	tenants *settings.Registry
	// policies is the default tenant's policy, for the ops gate of a flat
	// directory.
	policies    policy.Source
	promHandler http.Handler
	// pools is one ClickHouse pool per tuple the served tenants name, and
	// discoveries one schema registry per served tenant, each resolved per
	// call by the tenant.
	pools       *chconn.Pools
	bootState   *api.BootState
	discoveries *discoveries
	// dedup is one store per tenant, each following its own folder's switch,
	// and dedupeStats the figures of the one Pebble instance they share.
	dedup       *dedupe.Stores
	dedupeStats func() map[string]int64
	mq          mq.Broker
	cache       cache.Cache
	sseMetrics  *stream.Metrics
	hub         *stream.Hub
	heartbeater *stream.Heartbeater
	handler     http.Handler

	// components in wiring order; Close walks them backwards.
	components []component
	// flush is the telemetry shutdown, run by Close after every component
	// has released so the lines they log still reach the collector.
	flush func(ctx context.Context) error
	// hup is the SIGHUP registration, held until Close has released every
	// component so a hangup during the stop is ignored rather than fatal.
	hup chan os.Signal
	// stopCtx is cancelled the moment a stop begins (Run's context is
	// cancelled, or Close is called), so work that outlives its component's
	// loop — a settings reload mid-hook — gives up with it instead of
	// stretching the drain past its budget.
	stopCtx    context.Context
	stopCancel context.CancelFunc
}

const (
	// ReleaseTimeout bounds Close's release of the stores. A fixed budget
	// rather than server.shutdown_timeout: only the drain scales with the
	// deployment's workload, so the stop's worst case is shutdown_timeout
	// plus these constants, not a multiple of it.
	ReleaseTimeout = 5 * time.Second
	// flushTimeout bounds the telemetry flush that ends the stop. Short and
	// its own: the flush reports on the stop, so it must neither be starved
	// by a slow close nor hold the exit for an unreachable collector.
	flushTimeout = 3 * time.Second
)

// New wires every component. ctx bounds construction only — the boot-time
// schema refresh and the opening of each served tenant's queue; the loops
// start in Run. A failure releases whatever was already opened and returns the
// error, so the caller never holds a half-built App.
func New(ctx context.Context, opts Options) (app *App, err error) {
	a := &App{cfg: opts.Config, build: opts.Build, logLevel: opts.LogLevel, listener: opts.Listener}
	if a.logLevel == nil {
		a.logLevel = &slog.LevelVar{}
	}
	a.stopCtx, a.stopCancel = context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), ReleaseTimeout)
			defer cancel()
			// The boot error is the one the caller acts on; a release
			// failure on the way out is still the evidence for a leaked
			// handle, so it is logged rather than dropped.
			if cerr := a.Close(closeCtx); cerr != nil {
				slog.Warn("cleanup after failed boot", "error", cerr)
			}
		}
	}()

	if err := a.wireSettings(); err != nil {
		return nil, err
	}
	a.wireObservability(ctx)
	// After observability, so an OTLP log pipeline carries them too.
	for _, w := range a.cfg.Warnings() {
		slog.Warn(w)
	}
	if err := a.wireClickHouse(); err != nil {
		return nil, err
	}
	a.wireDiscovery(ctx)
	if err := a.wireDedupe(ctx); err != nil {
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
	authMW := a.wireAuth()
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
	// On gctx, not ctx: a component failure cancels only gctx, and the stop
	// it begins must reach a reload mid-hook too, or the hook holds Wait.
	unhook := context.AfterFunc(gctx, a.stopCancel)
	defer unhook()
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
// failure joined. ctx is the release budget (ReleaseTimeout from main), and
// it is a real bound: a remote implementation's close gives up at the
// deadline itself, and a close that ignores the context — the local stores
// (Pebble, ristretto, embedded NATS), which normally finish in milliseconds
// — is abandoned at it, and the components below it are not released at
// all (a close started then would overlap the abandoned one and break the
// reverse order), since the process is exiting either way and the flush
// that follows reports both. The telemetry flush then runs
// on its own flushTimeout, so it is never handed a budget a slow close has
// already spent. Safe to call more than once.
func (a *App) Close(ctx context.Context) error {
	// A stop is under way from here even if Run never saw a cancel (a
	// component failed): anything still waiting on stopCtx gives up.
	a.stopCancel()
	var errs []error
	for i := len(a.components) - 1; i >= 0; i-- {
		c := a.components[i]
		if c.close == nil {
			continue
		}
		if ctx.Err() != nil {
			// The budget is spent: a close started now would overlap the
			// one abandoned above and break the reverse order, so the rest
			// are left to the exit and named.
			errs = append(errs, fmt.Errorf("%s: not released, budget spent: %w", c.name, ctx.Err()))
			continue
		}
		if err := closeWithin(ctx, c.name, c.close); err != nil {
			errs = append(errs, err)
		}
	}
	a.components = nil
	if a.flush != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		if err := a.flush(flushCtx); err != nil {
			errs = append(errs, fmt.Errorf("observability: %w", err))
		}
		cancel()
		a.flush = nil
	}
	// Last, deliberately, and not a close hook on the sighup component:
	// hooks run in reverse wiring order, so that would restore SIGHUP's
	// default disposition (terminate) while the stores and the flush were
	// still closing, and a hangup — closing the terminal after Ctrl-C
	// signals the process group — would kill the process mid-release.
	// TestRun_ServesUntilCancelled sends the test binary a SIGHUP between
	// Run and Close to pin this.
	if a.hup != nil {
		signal.Stop(a.hup)
		a.hup = nil
	}
	return errors.Join(errs...)
}

// closeWithin runs release and gives up on it at ctx's deadline, so the
// release budget holds even for a close that never looks at its context.
// An abandoned close keeps running in the background until the process
// exits; the error names it so the flush can report which store was stuck.
func closeWithin(ctx context.Context, name string, release func(context.Context) error) error {
	done := make(chan error, 1)
	go func() { done <- release(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: abandoned at the release deadline: %w", name, ctx.Err())
	}
}

// Handler is the API router, for a harness that serves it itself.
func (a *App) Handler() http.Handler { return a.handler }

// Registry is the default tenant's schema registry, for a harness that
// refreshes it after creating tables; nil over a nested directory serving
// no tenant 0.
func (a *App) Registry() *discovery.SchemaRegistry { return a.discoveries.For(tenant.Default) }

// MQ is the broker, for a harness that publishes straight onto the ingest
// queue.
func (a *App) MQ() mq.Broker { return a.mq }
