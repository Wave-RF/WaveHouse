package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// maxReportedResults caps how many per-record entries are echoed back in a batch
// response. The four counts (total/succeeded/failed/duplicates) stay
// authoritative; the results array is truncated so a very large batch can't
// produce an unbounded response body. (The 16 MiB inbound body cap already
// bounds input, but full per-record fidelity amplifies even a capped body, so
// keep an explicit ceiling.) The body cap itself, maxRequestBodyBytes, is shared
// with the admin query handler — see internal/api/query.go.
const maxReportedResults = 10000

// IngestHandler handles POST /v1/ingest?table={table}
type IngestHandler struct {
	Registry    *discovery.SchemaRegistry
	Dedup       dedupe.Deduplicator // nil if dedup disabled
	IDField     string              // dedup key field name (e.g. "event_id")
	RequireID   bool                // reject rows missing IDField instead of publishing un-deduped (dedupe.require_id)
	Publisher   mq.Publisher
	PolicyStore *policy.Store
	logger      *slog.Logger

	// maxRequestBytes optionally overrides the default inbound request body cap
	// (maxRequestBodyBytes). When 0, the default applies. Exists so same-package
	// tests can pin the cap-overflow path without allocating 16 MiB per run; not
	// a production tuning knob, hence unexported. Mirrors QueryHandler.
	maxRequestBytes int64
}

func NewIngestHandler(registry *discovery.SchemaRegistry, pub mq.Publisher, logger *slog.Logger) *IngestHandler {
	return &IngestHandler{Registry: registry, Publisher: pub, logger: logger}
}

var dedupeMissingIDCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_dedupe_missing_id_total",
	metric.WithDescription("Ingested records missing the configured dedupe id_field (idempotency skipped)"),
)

// batchResult is the response body for any multi-record ingest (a JSON array,
// an NDJSON batch, and — later — CSV). The status is 200 whenever the body was
// readable and the records were processed; per-record rejections (malformed
// JSON, schema or permission failures) are reported in Results without failing
// the whole request, so one bad record never obscures the rest of the batch
// (issue #195). Whole-request conditions (backpressure, publish/dedup backend
// failure) abort with the usual non-200 status instead.
type batchResult struct {
	Total      int            `json:"total"`      // records read from the body
	Succeeded  int            `json:"succeeded"`  // records validated + published
	Failed     int            `json:"failed"`     // records rejected (see Results)
	Duplicates int            `json:"duplicates"` // records skipped by dedup
	Results    []recordResult `json:"results"`    // per-record outcomes (may be truncated; counts stay authoritative)
}

// recordResult is the outcome of a single record in a batch. It mirrors the
// single-object response shape (ok / duplicate / error) plus a 1-based index
// pinning it to its position in the submitted batch. Exactly one of Ok /
// Duplicate / Error is meaningful per entry.
type recordResult struct {
	Index     int    `json:"index"`
	Ok        bool   `json:"ok,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Error     string `json:"error,omitempty"`
}

// recordReject is a per-record rejection: this record is bad (failed schema
// validation or a column/check permission rule), but the rest of the batch can
// still proceed. The single-object path maps Status to the HTTP code; the batch
// path records Message against the index and keeps going.
type recordReject struct {
	Status  int
	Message string
}

// requestAbort is a whole-request failure: the system can't accept this record
// (or any that follow) right now — publish backpressure (503), a publish/marshal
// failure (500), or a dedup backend error (500). Both paths stop and return the
// status; the batch path abandons the remaining records so the caller can retry
// the batch rather than silently lose the tail.
type requestAbort struct {
	Status     int
	Message    string
	RetryAfter string // non-empty → emit a Retry-After header (503 backpressure)
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
	h.logger.DebugContext(ctx, "debug: span started for ingest", "table", table)

	r = r.WithContext(ctx)

	// TODO: what should the order of these be to maximize speed + limit risk of data leakage or DoS/resource exhaustion?

	if table == "" {
		h.logger.ErrorContext(ctx, "missing table parameter in request")
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	// TODO: prevent table-enumeration...
	schema := h.Registry.Get(table)
	if schema == nil {
		h.logger.WarnContext(ctx, "unknown table requested", "table", table)
		writeJSONError(w, http.StatusNotFound, "unknown table: "+table)
		return
	}

	// FAST AUTH: Table-level policy check (before spending CPU parsing the body).
	// The insert grant depends only on role+table+action, not on record
	// contents, so it is evaluated once for the whole request — including every
	// record of a batch.
	var perms *policy.ResolvedPermissions
	var role string

	if h.PolicyStore != nil {
		p := h.PolicyStore.Get()
		role = policy.ResolveRole(p, auth.RoleFromContext(ctx))
		claims, _ := auth.ClaimsFromContext(ctx)
		perms = policy.Evaluate(p, role, table, "insert", claims)
		if !perms.Allowed {
			writeAuthzDenied(w, r, h.logger, role, nil,
				slog.String("gate", "policy"),
				slog.String("table", table),
				slog.String("action", "insert"),
			)
			return
		}
	}

	// TODO: set a scope (e.g., "org_id:123") – but scope requires us to know if a table is globally shared or scoped fully by roles/org/tenant. Currently we have no real way to set this, so scopes will be empty.
	scope := ""

	// Bound the inbound body (parity with /v1/admin/query; also caps the
	// array/stream decode vectors). See query.go for maxRequestBodyBytes.
	reqCap := int64(maxRequestBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)

	// Pick a reader from the body shape (Content-Type is only a hint).
	rr, batch, err := newRecordReader(r.Header.Get("Content-Type"), r.Body)
	if err != nil {
		h.logger.ErrorContext(ctx, "empty ingest body", "table", table)
		writeJSONError(w, http.StatusBadRequest, emptyBodyMessage(r.Header.Get("Content-Type")))
		return
	}

	if batch {
		h.handleBatch(ctx, w, rr, reqCap, table, scope, schema, perms, role, now)
		return
	}
	h.handleSingle(ctx, w, rr, reqCap, table, scope, schema, perms, role, now)
}

// handleSingle ingests a lone flat JSON object and preserves the GA response
// contract: 200 {"ok":true} (or {"duplicate":true} when dedup skips it), or the
// matching non-200 on validation / permission / whole-request failure.
func (h *IngestHandler) handleSingle(
	ctx context.Context,
	w http.ResponseWriter,
	rr recordReader,
	reqCap int64,
	table, scope string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
	role string,
	now time.Time,
) {
	data, err := rr.Next()
	if err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		h.logger.ErrorContext(ctx, "invalid json payload", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	dup, reject, abort := h.processRecord(ctx, table, scope, schema, perms, role, data, now)
	if abort != nil {
		writeAbort(w, abort)
		return
	}
	if reject != nil {
		writeJSONError(w, reject.Status, reject.Message)
		return
	}
	if dup {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
		return
	}

	h.logger.InfoContext(ctx, "event successfully ingested", "table", table)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// handleBatch ingests a multi-record body (JSON array or NDJSON), running each
// record through the same validate → authorize → dedup → publish pipeline as a
// single insert. A record that fails validation or a permission rule — or that
// the reader couldn't decode — is recorded against its index and the batch
// continues; a whole-request condition (backpressure, publish/dedup backend
// failure) aborts the batch. Returns 200 with a per-record summary once the body
// is consumed.
func (h *IngestHandler) handleBatch(
	ctx context.Context,
	w http.ResponseWriter,
	rr recordReader,
	reqCap int64,
	table, scope string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
	role string,
	now time.Time,
) {
	result := batchResult{Results: []recordResult{}}

	for {
		data, err := rr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if rse, ok := errors.AsType[*recordSyntaxError](err); ok {
				result.Total++
				result.Failed++
				appendResult(&result, recordResult{Index: result.Total, Error: rse.Error()})
				continue
			}
			if writeMaxBytesError(w, err, reqCap) {
				return
			}
			// A fatal stream error (JSON array syntax error, oversized NDJSON
			// line, or body read error) — the reader can't resume, so fail the
			// request rather than report a misleading partial summary.
			h.logger.WarnContext(ctx, "ingest read error", "error", err, "table", table)
			writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}

		result.Total++
		idx := result.Total
		dup, reject, abort := h.processRecord(ctx, table, scope, schema, perms, role, data, now)
		if abort != nil {
			// Whole-request failure (backpressure / publish / dedup backend):
			// stop and surface the status so the caller retries the batch
			// instead of treating a system outage as per-record loss.
			writeAbort(w, abort)
			return
		}
		if reject != nil {
			result.Failed++
			appendResult(&result, recordResult{Index: idx, Error: reject.Message})
			continue
		}
		if dup {
			result.Duplicates++
			appendResult(&result, recordResult{Index: idx, Duplicate: true})
			continue
		}
		result.Succeeded++
		appendResult(&result, recordResult{Index: idx, Ok: true})
	}

	h.logger.InfoContext(ctx, "batch ingested", "table", table,
		"total", result.Total, "succeeded", result.Succeeded,
		"failed", result.Failed, "duplicates", result.Duplicates)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// appendResult records a per-record outcome up to maxReportedResults. The
// batchResult counts are incremented by the caller and stay authoritative even
// when the Results slice is truncated.
func appendResult(result *batchResult, entry recordResult) {
	if len(result.Results) < maxReportedResults {
		result.Results = append(result.Results, entry)
	}
}

// writeAbort emits a whole-request failure response: the status and message,
// plus a Retry-After header when one is set (503 backpressure). Shared by the
// single-object and batch paths.
func writeAbort(w http.ResponseWriter, abort *requestAbort) {
	if abort.RetryAfter != "" {
		w.Header().Set("Retry-After", abort.RetryAfter)
	}
	writeJSONError(w, abort.Status, abort.Message)
}

// writeMaxBytesError writes a 413 if err is the inbound body-cap overflow and
// reports whether it did. Mirrors the mapping in query.go.
func writeMaxBytesError(w http.ResponseWriter, err error, limit int64) bool {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		writeJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeded %d bytes", limit))
		return true
	}
	return false
}

// processRecord runs the per-record pipeline shared by the single-object and
// batch ingest paths: schema validation → column/check permission enforcement
// (with claim-derived auto-injection) → optional dedup → publish. The
// table-level insert grant is checked once by the caller before any record is
// processed, so perms here drives only the per-column and per-row checks (it is
// nil when no policy store is configured). data may be mutated to auto-inject
// check-clause values.
//
// Exactly one of the outcomes is meaningful per call:
//   - duplicate true: the record was skipped by dedup (reject/abort nil).
//   - reject non-nil: the record is bad; the rest of a batch may still proceed.
//   - abort non-nil: a whole-request failure; the caller stops and returns it.
func (h *IngestHandler) processRecord(
	ctx context.Context,
	table, scope string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
	role string,
	data map[string]any,
	now time.Time,
) (duplicate bool, reject *recordReject, abort *requestAbort) {
	if err := discovery.Validate(schema, data); err != nil {
		h.logger.WarnContext(ctx, "schema validation failed", "error", err, "table", table)
		return false, &recordReject{Status: http.StatusBadRequest, Message: err.Error()}, nil
	}

	// DEEP AUTH: column-level allow/deny + check clauses.
	if perms != nil {
		for col := range data {
			if !perms.IsColumnAllowed(col) {
				h.logger.WarnContext(ctx, "column insertion forbidden", "column", col, "role", role)
				return false, &recordReject{
					Status:  http.StatusForbidden,
					Message: fmt.Sprintf("column %q not allowed for insert", col),
				}, nil
			}
		}
		for col, requiredVal := range perms.CheckClauses {
			// A []any value is an _in check: the inserted value must be present and
			// one of the allowed set. Unlike the scalar _eq case there is no single
			// value to auto-inject, so an absent column fails closed.
			if set, isSet := requiredVal.([]any); isSet {
				actual, ok := data[col]
				if !ok || !valueInSet(actual, set) {
					h.logger.WarnContext(ctx, "check clause failed", "column", col, "allowed", set, "actual", actual, "present", ok)
					return false, &recordReject{
						Status:  http.StatusForbidden,
						Message: fmt.Sprintf("check failed for column %q", col),
					}, nil
				}
				continue
			}
			if actual, ok := data[col]; ok {
				if fmt.Sprint(actual) != fmt.Sprint(requiredVal) {
					h.logger.WarnContext(ctx, "check clause failed", "column", col, "expected", requiredVal, "actual", actual)
					return false, &recordReject{
						Status:  http.StatusForbidden,
						Message: fmt.Sprintf("check failed for column %q", col),
					}, nil
				}
			} else {
				// Auto-inject claim-derived value if not provided.
				data[col] = requiredVal
			}
		}
	}

	// Canonicalize timestamp columns to the RFC 3339 UTC wire form, so the one
	// payload published below reaches every consumer — SSE subscribers, the
	// ClickHouse insert, the DLQ — in a single unambiguous spelling (#372). After
	// the permission checks so check-clause comparisons keep their pre-#372
	// semantics (and claim-injected values are covered); before dedup so a
	// rejected record never marks a dedup key. Each column's precision and zone
	// were resolved at schema discovery, so this is pure computation per record.
	if err := discovery.CanonicalizeTimestamps(schema, data); err != nil {
		h.logger.WarnContext(ctx, "timestamp canonicalization failed", "error", err, "table", table)
		return false, &recordReject{Status: http.StatusBadRequest, Message: err.Error()}, nil
	}

	// Optional deduplication.
	if h.Dedup != nil && h.IDField != "" {
		idVal, ok := data[h.IDField]
		if !ok {
			dedupeMissingIDCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", table)))
			if h.RequireID {
				h.logger.WarnContext(ctx, "dedupe id_field missing; rejecting", "id_field", h.IDField, "table", table)
				return false, &recordReject{
					Status:  http.StatusBadRequest,
					Message: fmt.Sprintf("missing dedupe id field %q", h.IDField),
				}, nil
			}
			h.logger.WarnContext(ctx, "dedupe id_field missing; publishing without idempotency", "id_field", h.IDField, "table", table)
		} else {
			eventID := fmt.Sprint(idVal)
			dup, err := h.Dedup.CheckAndMark(ctx, eventID)
			if err != nil {
				h.logger.ErrorContext(ctx, "dedupe check failed", "error", err, "event_id", eventID)
				return false, nil, &requestAbort{Status: http.StatusInternalServerError, Message: "dedupe failed"}
			}
			if dup {
				h.logger.InfoContext(ctx, "duplicate event skipped", "event_id", eventID)
				return true, nil, nil
			}
		}
	}

	evt := ingest.EventMessage{
		TableName:         table,
		Scope:             scope,
		ReceivedTimestamp: now.Format(time.RFC3339Nano),
		Data:              data, // Put the data back in the envelope
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		h.logger.ErrorContext(ctx, "failed to marshal event message", "error", err)
		return false, nil, &requestAbort{Status: http.StatusInternalServerError, Message: "marshal failed"}
	}

	subject := "ingest." + query.SafeEncodeNATS(table)
	if scope != "" {
		subject += "." + query.SafeEncodeNATS(scope)
	}

	h.logger.DebugContext(ctx, "publishing event to NATS", "subject", subject, "table", table, "scope", scope)
	if err := h.Publisher.Publish(ctx, subject, payload); err != nil {
		if strings.Contains(err.Error(), "maximum bytes exceeded") {
			h.logger.WarnContext(ctx, "nats maximum bytes exceeded", "subject", subject)
			return false, nil, &requestAbort{Status: http.StatusServiceUnavailable, Message: "service unavailable", RetryAfter: "30"}
		}
		h.logger.ErrorContext(ctx, "failed to publish to NATS", "error", err, "subject", subject)
		return false, nil, &requestAbort{Status: http.StatusInternalServerError, Message: "publish failed"}
	}

	return false, nil, nil
}

// valueInSet reports whether v matches any member of set, comparing by string
// form to mirror the scalar check's fmt.Sprint equality — so a JSON number in
// the insert body matches a stringified claim value the same way _eq does.
func valueInSet(v any, set []any) bool {
	vs := fmt.Sprint(v)
	for _, s := range set {
		if fmt.Sprint(s) == vs {
			return true
		}
	}
	return false
}
