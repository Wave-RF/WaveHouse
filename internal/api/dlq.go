package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// DLQHandler exposes Dead Letter Queue statistics.
type DLQHandler struct {
	Counts mq.DeadLetterStats
}

func NewDLQHandler(stats mq.DeadLetterStats) *DLQHandler {
	return &DLQHandler{Counts: stats}
}

// Stats returns per-table message counts on the dead-letter queue.
// Supports optional ?table= query parameter to filter by table name.
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	counts, err := h.Counts.DeadLetterCounts(r.Context(), r.URL.Query().Get("table"))
	if err != nil {
		if !errors.Is(err, mq.ErrNoDeadLetterQueue) {
			slog.ErrorContext(r.Context(), "dlq stats failed", "error", err)
			writeJSONError(w, http.StatusInternalServerError, "stream info failed")
			return
		}
		// No dead-letter queue: nothing can have been parked.
		counts = mq.DeadLetterCounts{Tables: map[string]uint64{}}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"tables": counts.Tables, "total": counts.Total})
}
