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
	JS     jetstream.JetStream
	Logger *slog.Logger
}

func NewDLQHandler(js jetstream.JetStream, logger *slog.Logger) *DLQHandler {
	return &DLQHandler{JS: js, Logger: logger}
}

// DLQStreamName is the NATS JetStream stream used for dead-lettered events.
const DLQStreamName = "BEACHHOUSE_DLQ"

// Stats returns per-table message counts in the DLQ stream.
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	stream, err := h.JS.Stream(r.Context(), DLQStreamName)
	if err != nil {
		// Stream may not exist yet if no failures have occurred.
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"tables": map[string]any{}, "total": 0})
		return
	}

	info, err := stream.Info(r.Context())
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
func EnsureDLQStream(ctx context.Context, js jetstream.JetStream, maxBytes int64) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      DLQStreamName,
		Subjects:  []string{"dlq.>"},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	})
	return err
}
