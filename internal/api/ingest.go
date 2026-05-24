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
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// IngestHandler handles POST /v1/ingest?table={table}
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
	table := r.URL.Query().Get("table")

	tracer := observability.Tracer()

	ctx, span := tracer.Start(r.Context(), "IngestHandler.Handle",
		trace.WithAttributes(attribute.String("table", table)),
	)
	defer span.End()
	ctx = observability.WithComponent(ctx, "api/ingest")

	r = r.WithContext(ctx)

	if table == "" {
		slog.WarnContext(ctx, "ingest rejected: missing table parameter")
		observability.SchemaRejected.Add(ctx, 1, metric.WithAttributes(
			attribute.String("table", ""),
			attribute.String("reason", "missing_table"),
		))
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	schema := h.Registry.Get(table)
	if schema == nil {
		slog.WarnContext(ctx, "unknown table requested", "table", table)
		observability.SchemaRejected.Add(ctx, 1, metric.WithAttributes(
			attribute.String("table", table),
			attribute.String("reason", "unknown_table"),
		))
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("unknown table: %s", table))
		return
	}

	var data map[string]any
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		slog.WarnContext(ctx, "ingest rejected: invalid json payload", "error", err, "table", table)
		observability.SchemaRejected.Add(ctx, 1, metric.WithAttributes(
			attribute.String("table", table),
			attribute.String("reason", "invalid_json"),
		))
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	// Schema validation span — bounded work, gives operators a way to spot
	// validation cost when ingest latency unexpectedly spikes (e.g. very
	// wide tables, deep nested validation).
	_, validateSpan := tracer.Start(ctx, "schema_validation",
		trace.WithAttributes(attribute.String("table", table)),
	)
	validateErr := discovery.Validate(schema, data)
	validateSpan.End()
	if validateErr != nil {
		slog.WarnContext(ctx, "schema validation failed", "error", validateErr, "table", table)
		observability.SchemaRejected.Add(ctx, 1, metric.WithAttributes(
			attribute.String("table", table),
			attribute.String("reason", discovery.ClassifyValidationError(validateErr)),
		))
		writeJSONError(w, http.StatusBadRequest, validateErr.Error())
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
			writeJSONError(w, http.StatusForbidden, "forbidden")
			return
		}
		// Check column permissions — reject disallowed columns.
		for col := range data {
			if !perms.IsColumnAllowed(col) {
				slog.WarnContext(ctx, "column insertion forbidden", "column", col, "role", role)
				writeJSONError(w, http.StatusForbidden, fmt.Sprintf("column %q not allowed for insert", col))
				return
			}
		}
		// Enforce check rules — validate claim-derived field values.
		for col, requiredVal := range perms.CheckClauses {
			if actual, ok := data[col]; ok {
				if fmt.Sprint(actual) != fmt.Sprint(requiredVal) {
					slog.WarnContext(ctx, "check clause failed", "column", col, "expected", requiredVal, "actual", actual)
					writeJSONError(w, http.StatusForbidden, fmt.Sprintf("check failed for column %q", col))
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
				observability.DedupeLookups.Add(ctx, 1, metric.WithAttributes(
					attribute.String("table", table),
					attribute.String("outcome", "err"),
				))
				writeJSONError(w, http.StatusInternalServerError, "dedupe failed")
				return
			}
			if dup {
				slog.DebugContext(ctx, "duplicate event skipped", "event_id", eventID)
				observability.DedupeLookups.Add(ctx, 1, metric.WithAttributes(
					attribute.String("table", table),
					attribute.String("outcome", "hit"),
				))
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
				return
			}
			observability.DedupeLookups.Add(ctx, 1, metric.WithAttributes(
				attribute.String("table", table),
				attribute.String("outcome", "miss"),
			))
		}
	}

	// TODO: set a scope (e.g., "org_id:123") – but scope requires us to know if a table is globally shared or scoped fully by roles/org/tenant. Currently we have no real way to set this, so scopes will be empty.
	scope := ""

	now := time.Now().UTC()
	evt := ingest.EventMessage{
		TableName:         table,
		Scope:             scope,
		ReceivedTimestamp: now.Format(time.RFC3339Nano),
		Data:              data,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal event message", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "marshal failed")
		return
	}

	subject := "ingest." + query.SafeEncodeNATS(table)
	if scope != "" {
		subject += "." + query.SafeEncodeNATS(scope)
	}

	if err := h.Publisher.Publish(ctx, subject, payload); err != nil {
		if strings.Contains(err.Error(), "maximum bytes exceeded") {
			slog.WarnContext(ctx, "nats maximum bytes exceeded — returning 503", "subject", subject)
			observability.IngestPublishThrottled.Add(ctx, 1, metric.WithAttributes(
				attribute.String("table", table),
			))
			w.Header().Set("Retry-After", "30")
			writeJSONError(w, http.StatusServiceUnavailable, "service unavailable")
			return
		}
		slog.ErrorContext(ctx, "failed to publish to NATS", "error", err, "subject", subject)
		writeJSONError(w, http.StatusInternalServerError, "publish failed")
		return
	}

	// Per-request success was previously logged at INFO — at ingest scale
	// that's one stdout line per row. Demoted to DEBUG; success is the
	// expected path and is already visible via the wavehouse_ingest_*
	// histograms and the HTTP-request middleware metric.
	slog.DebugContext(ctx, "event ingested", "table", table, "subject", subject)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
