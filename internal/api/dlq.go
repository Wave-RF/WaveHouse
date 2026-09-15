package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
)

// DLQHandler exposes Dead Letter Queue statistics.
type DLQHandler struct {
	Streams mq.StreamManager
	Logger  *slog.Logger
}

func NewDLQHandler(streams mq.StreamManager, logger *slog.Logger) *DLQHandler {
	return &DLQHandler{Streams: streams, Logger: logger}
}

// Stats returns per-table message counts in the DLQ stream.
// Supports optional ?table= query parameter to filter by table name.
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	stream, err := h.Streams.Stream(r.Context(), mq.DLQStreamName())
	if err != nil { // TODO: catch by error type
		// Stream may not exist yet if no failures have occurred.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tables": map[string]any{}, "total": 0})
		return
	}

	// If table filter is specified, only query that subject.
	subjectFilter := ">"
	tableFilter := r.URL.Query().Get("table")
	if tableFilter != "" {
		subjectFilter = "dlq." + query.SafeEncodeNATS(tableFilter)
	}

	state, err := stream.State(r.Context(), subjectFilter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "stream info failed")
		return
	}

	tables := make(map[string]uint64)
	for subject, count := range state.Subjects {
		// TODO: do we need to break out scopes here?
		decodedSubject, err := query.SafeDecodeNATS(strings.TrimPrefix(subject, "dlq."))
		if err != nil {
			continue
		}
		tables[decodedSubject] = count
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"tables": tables, "total": state.Msgs})
}
