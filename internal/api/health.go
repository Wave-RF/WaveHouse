package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
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
	// answer) and the external NATS; a process with neither is ready once
	// booted.
	Checks []Check
	// Boot is consulted by both Liveness and Readiness. When non-nil and
	// its Err() is non-nil, both endpoints report 503 with the diagnostic
	// — used while boot-time schema discovery is still failing in the
	// retry loop. A nil Boot preserves the pre-retry-loop behaviour
	// (Liveness always 200; Readiness 503 only on a failed check).
	Boot *BootState
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

func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	if h.Boot != nil {
		if err := h.Boot.Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "error": err.Error()})
			return
		}
	}
	var failed []error
	for _, c := range h.Checks {
		if err := c.Run(r.Context()); err != nil {
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
