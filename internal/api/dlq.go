package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/nats-io/nats.go/jetstream"
)

// DLQHandler exposes Dead Letter Queue statistics.
type DLQHandler struct {
	JS            jetstream.JetStream
	DLQStreamName string
	Logger        *slog.Logger
}

func NewDLQHandler(js jetstream.JetStream, baseStreamName string, logger *slog.Logger) *DLQHandler {
	return &DLQHandler{JS: js, DLQStreamName: baseStreamName + "_DLQ", Logger: logger}
}

// Stats returns per-table message counts in the DLQ stream.
// Supports optional ?table= query parameter to filter by table name.
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	stream, err := h.JS.Stream(r.Context(), h.DLQStreamName)
	if err != nil {
		// Stream may not exist yet if no failures have occurred.
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"tables": map[string]any{}, "total": 0})
		return
	}

	// If table filter is specified, only query that subject.
	subjectFilter := ">"
	tableFilter := r.URL.Query().Get("table")
	if tableFilter != "" {
		subjectFilter = "dlq." + tableFilter
	}

	info, err := stream.Info(r.Context(), jetstream.WithSubjectFilter(subjectFilter))
	if err != nil {
		http.Error(w, `{"error":"stream info failed"}`, http.StatusInternalServerError)
		return
	}

	tables := make(map[string]uint64)
	for subject, count := range info.State.Subjects {
		tables[subject] = count
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"tables": tables, "total": info.State.Msgs})
}

// EnsureDLQStream creates the DLQ JetStream stream if it doesn't exist.
func EnsureDLQStream(ctx context.Context, js jetstream.JetStream, baseStreamName string, maxBytes int64) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      baseStreamName + "_DLQ",
		Subjects:  []string{"dlq.>"},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	})
	return err
}
