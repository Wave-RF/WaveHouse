package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// BootState tracks a one-shot startup diagnostic surfaced by /livez. While
// Err() returns non-nil the binary is considered to be in degraded-boot mode:
// /livez responds 503 with the diagnostic message instead of 200, so an
// operator can curl the endpoint to learn why the gateway isn't accepting
// traffic yet. Once boot work (today: the first tenant's ClickHouse schema
// discovery) succeeds, Set(nil) flips /livez back to 200.
//
// BootState is safe for concurrent use.
type BootState struct {
	mu  sync.RWMutex
	err error
}

// NewBootState returns a BootState seeded with initialErr. Pass nil if the
// binary is fully ready at construction time; pass a non-nil error to start
// in degraded mode (the goroutine that performs boot work calls Set(nil) on
// success).
func NewBootState(initialErr error) *BootState {
	return &BootState{err: initialErr}
}

// Set replaces the current diagnostic. Pass nil to mark the binary ready.
func (b *BootState) Set(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

// Err returns the current diagnostic, or nil if the binary is ready.
func (b *BootState) Err() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.err
}

// Check is one dependency Readiness consults, named for the 503 body: the
// ClickHouse pools, the external message queue.
type Check struct {
	Name string
	// Run reports why the dependency cannot do its part, nil while it can.
	Run func(context.Context) error
}

// HealthHandler provides liveness and readiness probes.
type HealthHandler struct {
	// Checks are Readiness's dependency checks, every one run on every call
	// so the 503 names each one that failed, not just the first. Which
	// dependencies a process has is internal/app's to wire: the ClickHouse
	// pools (chconn.Pools.Ping: every open pool at once, ready at the first
	// answer) and the external NATS; a handler with none is ready once
	// booted.
	Checks []Check
	// Boot is consulted by both Liveness and Readiness. When non-nil and
	// its Err() is non-nil, both endpoints report 503 with the diagnostic
	// — used while boot-time schema discovery is still failing in the
	// retry loop. A nil Boot preserves the pre-retry-loop behaviour
	// (Liveness always 200; Readiness 503 only on a failed check).
	Boot *BootState
	// CheckTimeout bounds each check; one that has not answered by then
	// fails. Zero means DefaultCheckTimeout.
	CheckTimeout time.Duration

	// results is each check's last outcome, for the readiness metrics and
	// the edge-triggered logs (#901).
	results readinessResults
}

// DefaultCheckTimeout is how long a readiness check has to answer when
// HealthHandler.CheckTimeout is unset: long enough for a ping across regions,
// short enough that a hung dependency reads as failing within a probe period.
const DefaultCheckTimeout = 5 * time.Second

// bootCheck is the check name the boot diagnostic reports under.
const bootCheck = "boot"

// readinessResults holds each readiness check's last outcome. Readiness
// records into it on every call; the checks are run only when probed, so
// what it holds is as old as the last probe.
type readinessResults struct {
	mu sync.Mutex
	// byName is each check's outcome, keyed by Check.Name or bootCheck.
	byName map[string]*checkResult
	// last is when Readiness was last called, zero before the first call.
	last time.Time
}

// checkResult is one check's last outcome.
type checkResult struct {
	ran bool
	ok  bool
	// at is when the outcome was taken, so a slower concurrent probe that
	// started earlier cannot overwrite a newer one.
	at time.Time
	// failingSince is when the check went from passing (or not yet run) to
	// failing, zero while it passes.
	failingSince time.Time
}

// result returns name's entry, creating it. Callers hold r.mu.
func (r *readinessResults) result(name string) *checkResult {
	if r.byName == nil {
		r.byName = make(map[string]*checkResult)
	}
	c, ok := r.byName[name]
	if !ok {
		c = &checkResult{}
		r.byName[name] = c
	}
	return c
}

// called marks a /readyz call made at started. A call that read the clock
// earlier but got here later does not move the time back.
func (r *readinessResults) called(started time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if started.After(r.last) {
		r.last = started
	}
}

// record stores name's outcome, taken at started, and logs only a change: one
// WARN when it starts failing, one INFO when it passes again with how long it
// failed, nothing while the state holds. An outcome older than the one
// recorded is dropped. It logs under r.mu, so concurrent probes' lines come
// out in the order their outcomes were recorded; it logs only on a change, so
// the lock is rarely held for it.
func (r *readinessResults) record(ctx context.Context, name string, started time.Time, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.result(name)
	if c.ran && started.Before(c.at) {
		return
	}
	wasRun, wasOK, since := c.ran, c.ok, c.failingSince
	c.ran, c.ok, c.at = true, err == nil, started
	switch {
	case err != nil && (wasOK || !wasRun):
		c.failingSince = started
		slog.WarnContext(ctx, "readiness check failing", "check", name, "error", err.Error())
	case err == nil:
		c.failingSince = time.Time{}
		if wasRun && !wasOK {
			slog.InfoContext(ctx, "readiness check passing again", "check", name,
				"failed_for", started.Sub(since).Round(time.Millisecond).String())
		}
	}
}

// run runs c to completion even if the probe client disconnects, bounded by
// CheckTimeout. Under the request's context, a ClickHouse that stops
// answering without refusing the connection would be cut off by the probe's
// own timeout (kubelet's is 1s by default) as context.Canceled: recorded,
// that reads as a failure for every client that merely went away; skipped, a
// hung dependency never reads 0. Its own deadline makes a hang a recorded
// failure and a disconnect irrelevant (#901).
func (h *HealthHandler) run(ctx context.Context, c Check) error {
	timeout := h.CheckTimeout
	if timeout <= 0 {
		timeout = DefaultCheckTimeout
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	return c.Run(ctx)
}

func NewHealthHandler(checks ...Check) *HealthHandler {
	return &HealthHandler{Checks: checks}
}

func (h *HealthHandler) Liveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	if h.Boot != nil {
		if err := h.Boot.Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "degraded", "error": err.Error()})
			return
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Readiness answers 200 once booted and every check passes, else 503 naming
// each failure. While the boot diagnostic fails, the checks are not run, so
// their recorded outcomes keep the last probe's.
func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	started := time.Now()
	h.results.called(started)
	if h.Boot != nil {
		err := h.Boot.Err()
		h.results.record(r.Context(), bootCheck, started, err)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "error": err.Error()})
			return
		}
	}
	var failed []error
	for _, c := range h.Checks {
		err := h.run(r.Context(), c)
		h.results.record(r.Context(), c.Name, started, err)
		if err != nil {
			failed = append(failed, fmt.Errorf("%s: %w", c.Name, err))
		}
	}
	if len(failed) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "error": errors.Join(failed...).Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

// RegisterReadinessMetrics registers the readiness gauges on meter (#901),
// read on every collection from what Readiness last recorded:
// wavehouse_readiness_check_ok{check} for each check and, with Boot set, the
// boot diagnostic as check="boot", 1 if it passed the last time it ran and
// 0 if it failed or has not run; and
// wavehouse_readiness_last_check_timestamp_seconds, the time of the last
// Readiness call (0 before the first). Call it once Checks and Boot are set.
func (h *HealthHandler) RegisterReadinessMetrics(meter metric.Meter) (metric.Registration, error) {
	names := make([]string, 0, len(h.Checks)+1)
	if h.Boot != nil {
		names = append(names, bootCheck)
	}
	for _, c := range h.Checks {
		names = append(names, c.Name)
	}
	attrs := make([]metric.ObserveOption, len(names))
	for i, n := range names {
		attrs[i] = metric.WithAttributeSet(attribute.NewSet(attribute.String("check", n)))
	}

	var errs [3]error
	var checkOK, lastCheck metric.Int64ObservableGauge
	checkOK, errs[0] = meter.Int64ObservableGauge("wavehouse_readiness_check_ok",
		metric.WithDescription("1 if the /readyz check passed the last time it ran, else 0 (failed, or not run yet); check is clickhouse, mq or boot"))
	lastCheck, errs[1] = meter.Int64ObservableGauge("wavehouse_readiness_last_check_timestamp_seconds",
		metric.WithDescription("Unix time of the last /readyz call; 0 before the first. The checks run only when probed"))
	var reg metric.Registration
	reg, errs[2] = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		h.results.mu.Lock()
		defer h.results.mu.Unlock()
		for i, n := range names {
			var v int64
			if c := h.results.result(n); c.ran && c.ok {
				v = 1
			}
			o.ObserveInt64(checkOK, v, attrs[i])
		}
		var at int64
		if !h.results.last.IsZero() {
			at = h.results.last.Unix()
		}
		o.ObserveInt64(lastCheck, at)
		return nil
	}, checkOK, lastCheck)
	if err := errors.Join(errs[:]...); err != nil {
		return nil, err
	}
	return reg, nil
}

// Online is a content-free public liveness ping served at /v1/health for the
// SDK's "is this server reachable / accepting data" check (and for picking
// among servers in a distributed setup). It mirrors Liveness's status logic —
// 200 once boot completes, 503 while boot-time schema discovery is still
// failing — but writes no body: the caller only branches on the status code,
// so there's nothing to JSON-encode or cache per request.
//
// It deliberately lives under /v1 rather than reusing /livez: /livez (and
// /readyz, /healthz) are Kubernetes probe paths an operator may filter out at
// the reverse proxy, whereas /v1/health is documented public API surface the
// SDK can rely on staying reachable. It does NOT ping ClickHouse — readiness-
// based load balancing is the proxy/LB's job (via /readyz), not the client's.
func (h *HealthHandler) Online(w http.ResponseWriter, _ *http.Request) {
	if h.Boot != nil && h.Boot.Err() != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
