package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/go-chi/chi/v5"
)

// IngestHandler handles POST /v1/ingest/{table}.
type IngestHandler struct {
	Registry  *discovery.SchemaRegistry
	Dedup     dedupe.Deduplicator // nil if dedup disabled
	IDField   string              // dedup key field name (e.g. "event_id")
	Publisher mq.Publisher
}

func NewIngestHandler(registry *discovery.SchemaRegistry, pub mq.Publisher) *IngestHandler {
	return &IngestHandler{Registry: registry, Publisher: pub}
}

func (h *IngestHandler) Handle(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	if table == "" {
		http.Error(w, `{"error":"missing table"}`, http.StatusBadRequest)
		return
	}

	schema := h.Registry.Get(table)
	if schema == nil {
		http.Error(w, fmt.Sprintf(`{"error":"unknown table: %s"}`, table), http.StatusNotFound)
		return
	}

	var data map[string]any
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if err := discovery.Validate(schema, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Optional deduplication.
	if h.Dedup != nil && h.IDField != "" {
		if idVal, ok := data[h.IDField]; ok {
			eventID := fmt.Sprint(idVal)
			dup, err := h.Dedup.CheckAndMark(r.Context(), eventID)
			if err != nil {
				http.Error(w, `{"error":"dedupe failed"}`, http.StatusInternalServerError)
				return
			}
			if dup {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
				return
			}
		}
	}

	now := time.Now().UTC()
	evt := ingest.EventMessage{
		TableName:         table,
		ReceivedTimestamp: now.Format(time.RFC3339Nano),
		Data:              data,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		http.Error(w, `{"error":"marshal failed"}`, http.StatusInternalServerError)
		return
	}

	subject := "ingest." + table
	if err := h.Publisher.Publish(r.Context(), subject, payload); err != nil {
		if strings.Contains(err.Error(), "maximum bytes exceeded") {
			w.Header().Set("Retry-After", "30")
			http.Error(w, `{"error":"service unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		http.Error(w, `{"error":"publish failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
