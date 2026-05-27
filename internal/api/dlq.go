package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
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

// Stats returns per-table message counts in the DLQ stream.
// Supports optional ?table= query parameter to filter by table name.
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	stream, err := h.JS.Stream(r.Context(), mq.DLQStreamName())
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

	info, err := stream.Info(r.Context(), jetstream.WithSubjectFilter(subjectFilter))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "stream info failed")
		return
	}

	tables := make(map[string]uint64)
	for subject, count := range info.State.Subjects {
		// TODO: do we need to break out scopes here?
		decodedSubject, err := query.SafeDecodeNATS(strings.TrimPrefix(subject, "dlq."))
		if err != nil {
			continue
		}
		tables[decodedSubject] = count
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"tables": tables, "total": info.State.Msgs})
}

// EnsureDLQStream creates the DLQ JetStream stream if it doesn't exist.
func EnsureDLQStream(ctx context.Context, js jetstream.JetStream, maxBytes int64) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      mq.DLQStreamName(),
		Subjects:  []string{"dlq.>"},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	})
	return err
}
