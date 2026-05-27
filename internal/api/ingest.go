package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	now := time.Now().UTC()
	table := r.URL.Query().Get("table")

	// Force the use of the GLOBAL provider
	tracer := otel.GetTracerProvider().Tracer("internal/api")

	ctx, span := tracer.Start(r.Context(), "IngestHandler.Handle",
		trace.WithAttributes(attribute.String("table", table)),
	)
	defer span.End()

	// Add a standard log to prove we are inside the span logic
	slog.DebugContext(ctx, "debug: span started for ingest", "table", table)

	r = r.WithContext(ctx)

	// TODO: what should the order of these be to maximize speed + limit risk of data leakage or DoS/resource exhaustion?

	if table == "" {
		slog.ErrorContext(ctx, "missing table parameter in request")
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	// TODO: prevent table-enumeration...
	schema := h.Registry.Get(table)
	if schema == nil {
		slog.WarnContext(ctx, "unknown table requested", "table", table)
		writeJSONError(w, http.StatusNotFound, "unknown table: "+table)
		return
	}

	// FAST AUTH: Table-level policy check (Before spending CPU parsing JSON)
	var perms *policy.ResolvedPermissions
	var role string

	if h.PolicyStore != nil {
		p := h.PolicyStore.Get()
		role = policy.ResolveRole(p, auth.RoleFromContext(ctx))
		claims, _ := auth.ClaimsFromContext(ctx)
		perms = policy.Evaluate(p, role, table, "insert", claims)
		if !perms.Allowed {
			writeAuthzDenied(w, r, role, nil)
			return
		}
	}

	// TODO: accept NDJSON
	var data map[string]any
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		slog.ErrorContext(ctx, "invalid json payload", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if err := discovery.Validate(schema, data); err != nil {
		slog.WarnContext(ctx, "schema validation failed", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	// DEEP AUTH: Column-level & check clauses
	if h.PolicyStore != nil {
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
				writeJSONError(w, http.StatusInternalServerError, "dedupe failed")
				return
			}
			if dup {
				slog.InfoContext(ctx, "duplicate event skipped", "event_id", eventID)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
				return
			}
		}
	}

	// TODO: set a scope (e.g., "org_id:123") – but scope requires us to know if a table is globally shared or scoped fully by roles/org/tenant. Currently we have no real way to set this, so scopes will be empty.
	scope := ""

	evt := ingest.EventMessage{
		TableName:         table,
		Scope:             scope,
		ReceivedTimestamp: now.Format(time.RFC3339Nano),
		Data:              data, // Put the data back in the envelope
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

	slog.DebugContext(ctx, "publishing event to NATS", "subject", subject, "table", table, "scope", scope)
	if err := h.Publisher.Publish(ctx, subject, payload); err != nil {
		if strings.Contains(err.Error(), "maximum bytes exceeded") {
			slog.WarnContext(ctx, "nats maximum bytes exceeded", "subject", subject)
			w.Header().Set("Retry-After", "30")
			writeJSONError(w, http.StatusServiceUnavailable, "service unavailable")
			return
		}
		slog.ErrorContext(ctx, "failed to publish to NATS", "error", err, "subject", subject)
		writeJSONError(w, http.StatusInternalServerError, "publish failed")
		return
	}

	slog.InfoContext(ctx, "event successfully ingested", "table", table, "subject", subject)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
