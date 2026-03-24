package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Wave-RF/BeachHouse/internal/dedupe"
	"github.com/Wave-RF/BeachHouse/internal/ingest"
	"github.com/Wave-RF/BeachHouse/internal/mq"
	"github.com/Wave-RF/BeachHouse/internal/schema"
)

// IngestHandler handles POST /v1/ingest.
type IngestHandler struct {
	Dedup     dedupe.Deduplicator
	Publisher mq.Publisher
}

func NewIngestHandler(d dedupe.Deduplicator, pub mq.Publisher) *IngestHandler {
	return &IngestHandler{Dedup: d, Publisher: pub}
}

type ingestRequest struct {
	ID        string          `json:"id"`
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

func (h *IngestHandler) Handle(w http.ResponseWriter, r *http.Request) {
	tenantID := TenantIDFromContext(r.Context())
	if tenantID == "" {
		http.Error(w, `{"error":"no tenant"}`, http.StatusForbidden)
		return
	}

	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
		return
	}

	// Deduplicate using tenant-scoped key.
	dup, err := h.Dedup.CheckAndMark(r.Context(), tenantID, req.ID)
	if err != nil {
		http.Error(w, `{"error":"dedupe failed"}`, http.StatusInternalServerError)
		return
	}
	if dup {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
		return
	}

	// Flatten data to EAV. User-supplied fields (including any "tenant_id" in the
	// payload) are flattened as normal data — they never affect the system tenant_id.
	var mapKeys, mapValues []string
	if req.Data != nil {
		mapKeys, mapValues, err = schema.Flatten(req.Data)
		if err != nil {
			http.Error(w, `{"error":"flatten failed"}`, http.StatusBadRequest)
			return
		}
	}

	ts := req.Timestamp
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	// Build MQ message with system tenant_id from JWT.
	evt := ingest.EventMessage{
		TenantID:  tenantID,
		EventID:   req.ID,
		Timestamp: ts,
		EventType: req.Type,
		MapKeys:   mapKeys,
		MapValues: mapValues,
	}
	data, err := json.Marshal(evt)
	if err != nil {
		http.Error(w, `{"error":"marshal failed"}`, http.StatusInternalServerError)
		return
	}

	if err := h.Publisher.Publish(r.Context(), "ingest.events", data); err != nil {
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
