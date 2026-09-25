package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// DLQHandler exposes Dead Letter Queue statistics.
type DLQHandler struct {
	Counts mq.DeadLetterStats
}

func NewDLQHandler(stats mq.DeadLetterStats) *DLQHandler {
	return &DLQHandler{Counts: stats}
}

// Stats returns per-table message counts on one tenant's dead-letter queue:
// the tenant ?tenant= names (read strictly, as every ops read does —
// opsTenant), tenant.Default without it. The queue is the MQ's, not the
// settings', so it is looked up there: a tenant whose folder was rejected or
// removed is read like one being served, for as long as its queue is kept,
// and an id with no queue is a 404. Supports optional ?table= query parameter
// to filter by table name.
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	id, named, ok := opsTenant(w, r)
	if !ok {
		return
	}
	if !named {
		id = tenant.Default
	}
	counts, err := h.Counts.DeadLetterCounts(r.Context(), id, r.URL.Query().Get("table"))
	if err != nil {
		if errors.Is(err, mq.ErrNoDeadLetterQueue) {
			writeJSONError(w, http.StatusNotFound, "no dead-letter queue for tenant: "+id.String())
			return
		}
		slog.ErrorContext(r.Context(), "dlq stats failed", "tenant", id, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "stream info failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"tables": counts.Tables, "total": counts.Total})
}
