package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/go-chi/chi/v5"


	"go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/trace"
)

// IngestHandler handles POST /v1/ingest/{table}.
type IngestHandler struct {
	Registry    *discovery.SchemaRegistry
	Dedup       dedupe.Deduplicator // nil if dedup disabled
	IDField     string              // dedup key field name (e.g. "event_id")
	Publisher   mq.Publisher
	PolicyStore *policy.Store
}

func NewIngestHandler(registry *discovery.SchemaRegistry, pub mq.Publisher) *IngestHandler {
	return &IngestHandler{Registry: registry, Publisher: pub}
}

func (h *IngestHandler) Handle(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")

    // Force the use of the GLOBAL provider
    tracer := otel.GetTracerProvider().Tracer("internal/api")

    ctx, span := tracer.Start(r.Context(), "IngestHandler.Handle",
        trace.WithAttributes(attribute.String("table", table)),
    )
    defer span.End()

    // Add a standard log to prove we are inside the span logic
    slog.InfoContext(ctx, "debug: span started for ingest", "table", table)

    r = r.WithContext(ctx)

	if table == "" {
		slog.ErrorContext(ctx, "missing table parameter in request")
		http.Error(w, `{"error":"missing table"}`, http.StatusBadRequest)
		return
	}
	
	schema := h.Registry.Get(table)
	if schema == nil {
		slog.WarnContext(ctx, "unknown table requested", "table", table)
		http.Error(w, fmt.Sprintf(`{"error":"unknown table: %s"}`, table), http.StatusNotFound)
		return
	}

	var data map[string]any
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		slog.ErrorContext(ctx, "invalid json payload", "error", err, "table", table)
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if err := discovery.Validate(schema, data); err != nil {
		slog.WarnContext(ctx, "schema validation failed", "error", err, "table", table)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Policy enforcement for inserts.
	if h.PolicyStore != nil {
		role := RoleFromContext(ctx)
		claims, _ := ClaimsFromContext(ctx)
		p := h.PolicyStore.Get()
		perms := policy.Evaluate(p, role, table, "insert", claims)
		if !perms.Allowed {
			slog.WarnContext(ctx, "policy enforcement rejected request", "role", role, "table", table)
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		// Check column permissions — reject disallowed columns.
		for col := range data {
			if !perms.IsColumnAllowed(col) {
				slog.WarnContext(ctx, "column insertion forbidden", "column", col, "role", role)
				http.Error(w, fmt.Sprintf(`{"error":"column %q not allowed for insert"}`, col), http.StatusForbidden)
				return
			}
		}
		// Enforce check rules — validate claim-derived field values.
		for col, requiredVal := range perms.CheckClauses {
			if actual, ok := data[col]; ok {
				if fmt.Sprint(actual) != fmt.Sprint(requiredVal) {
					slog.WarnContext(ctx, "check clause failed", "column", col, "expected", requiredVal, "actual", actual)
					http.Error(w, fmt.Sprintf(`{"error":"check failed for column %q"}`, col), http.StatusForbidden)
					return
				}
			} else {
				// Auto-inject claim-derived value if not provided.
				data[col] = requiredVal
			}
		}
	}

	// Optional deduplication.
	if h.Dedup != nil && h.IDField != "" {
		if idVal, ok := data[h.IDField]; ok {
			eventID := fmt.Sprint(idVal)
			dup, err := h.Dedup.CheckAndMark(ctx, eventID)
			if err != nil {
				slog.ErrorContext(ctx, "dedupe check failed", "error", err, "event_id", eventID)
				http.Error(w, `{"error":"dedupe failed"}`, http.StatusInternalServerError)
				return
			}
			if dup {
				slog.InfoContext(ctx, "duplicate event skipped", "event_id", eventID)
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
		slog.ErrorContext(ctx, "failed to marshal event message", "error", err)
		http.Error(w, `{"error":"marshal failed"}`, http.StatusInternalServerError)
		return
	}

	subject := "ingest." + table
	if err := h.Publisher.Publish(ctx, subject, payload); err != nil {
		if strings.Contains(err.Error(), "maximum bytes exceeded") {
			slog.WarnContext(ctx, "nats maximum bytes exceeded", "subject", subject)
			w.Header().Set("Retry-After", "30")
			http.Error(w, `{"error":"service unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		slog.ErrorContext(ctx, "failed to publish to NATS", "error", err, "subject", subject)
		http.Error(w, `{"error":"publish failed"}`, http.StatusInternalServerError)
		return
	}

	slog.InfoContext(ctx, "event successfully ingested", "table", table, "subject", subject)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
