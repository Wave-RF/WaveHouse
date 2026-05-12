package api

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BootState tracks a one-shot startup diagnostic surfaced by /health. While
// Err() returns non-nil the binary is considered to be in degraded-boot mode:
// /health responds 503 with the diagnostic message instead of 200, so an
// operator can curl the endpoint to learn why the gateway isn't accepting
// traffic yet. Once boot work (today: ClickHouse schema discovery) succeeds,
// Set(nil) flips /health back to 200.
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

// HealthHandler provides liveness and readiness probes.
type HealthHandler struct {
	CHConn driver.Conn
	// Boot is consulted by Liveness. When non-nil and its Err() is non-nil,
	// Liveness reports 503 with the diagnostic message — used while boot-
	// time schema discovery is still failing in the retry loop. A nil Boot
	// preserves the pre-retry-loop behaviour (always 200).
	Boot *BootState
}

func NewHealthHandler(chConn driver.Conn) *HealthHandler {
	return &HealthHandler{CHConn: chConn}
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
	if h.CHConn != nil {
		if err := h.CHConn.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "error": err.Error()})
			return
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}
