package api

import (
	"encoding/json"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// HealthHandler provides liveness and readiness probes.
type HealthHandler struct {
	CHConn driver.Conn
}

func NewHealthHandler(chConn driver.Conn) *HealthHandler {
	return &HealthHandler{CHConn: chConn}
}

func (h *HealthHandler) Liveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if h.CHConn != nil {
		if err := h.CHConn.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "error": err.Error()})
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}
