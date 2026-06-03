package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
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

const (
	// maxNDJSONLineBytes caps a single NDJSON record so one pathological line
	// can't force an unbounded read buffer. 10 MiB is far above any realistic
	// flat ingest record; a line larger than this aborts the whole request.
	maxNDJSONLineBytes = 10 << 20 // 10 MiB

	// maxReportedNDJSONErrors caps how many per-line errors are echoed back in
	// the batch response. `failed` still reflects the true count; the errors
	// array is truncated so a batch of all-bad lines can't produce a huge
	// response body.
	maxReportedNDJSONErrors = 100
)

// IngestHandler handles POST /v1/ingest?table={table}
type IngestHandler struct {
	Registry    *discovery.SchemaRegistry
	Dedup       dedupe.Deduplicator // nil if dedup disabled
	IDField     string              // dedup key field name (e.g. "event_id")
	Publisher   mq.Publisher
	PolicyStore *policy.Store
	logger      *slog.Logger
}

func NewIngestHandler(registry *discovery.SchemaRegistry, pub mq.Publisher, logger *slog.Logger) *IngestHandler {
	return &IngestHandler{Registry: registry, Publisher: pub, logger: logger}
}

// ndjsonResult is the response body for an NDJSON batch ingest. The status is
// 200 whenever the body was readable and at least one record was attempted;
// per-record rejections (malformed JSON, schema or permission failures) are
// reported in Errors without failing the whole request, so one bad line never
// obscures the rest of the batch (issue #195). Whole-request conditions
// (backpressure, publish/dedup backend failure) abort with the usual non-200
// status instead.
type ndjsonResult struct {
	Total      int               `json:"total"`      // records read from the body (non-blank lines)
	Succeeded  int               `json:"succeeded"`  // records validated + published
	Failed     int               `json:"failed"`     // records rejected (see Errors)
	Duplicates int               `json:"duplicates"` // records skipped by dedup
	Errors     []ndjsonLineError `json:"errors,omitempty"`
}

// ndjsonLineError pins a per-record rejection to its position in the request.
type ndjsonLineError struct {
	Line  int    `json:"line"` // 1-based index of the record in the request body
	Error string `json:"error"`
}

// recordReject is a per-record rejection: this record is bad (failed schema
// validation or a column/check permission rule), but the rest of the batch can
// still proceed. The single-object path maps Status to the HTTP code; the
// NDJSON path records Message against the line number and keeps going.
type recordReject struct {
	Status  int
	Message string
}

// requestAbort is a whole-request failure: the system can't accept this record
// (or any that follow) right now — publish backpressure (503), a publish/marshal
// failure (500), or a dedup backend error (500). Both paths stop and return the
// status; the NDJSON path abandons the remaining lines so the caller can retry
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
	// record of an NDJSON batch.
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

	// NDJSON batch path: one JSON record per line, partial-failure aware.
	if isNDJSONContentType(r.Header.Get("Content-Type")) {
		h.handleNDJSON(ctx, w, r, table, scope, schema, perms, role, now)
		return
	}

	// Single flat-JSON-object path.
	var data map[string]any
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		h.logger.ErrorContext(ctx, "invalid json payload", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	dup, reject, abort := h.processRecord(ctx, table, scope, schema, perms, role, data, now)
	if abort != nil {
		if abort.RetryAfter != "" {
			w.Header().Set("Retry-After", abort.RetryAfter)
		}
		writeJSONError(w, abort.Status, abort.Message)
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

// handleNDJSON ingests a newline-delimited JSON body, one record per line. Blank
// lines are skipped. Each record runs the same validate → authorize → dedup →
// publish pipeline as a single insert; a record that fails validation or a
// permission rule is recorded against its line number and the batch continues,
// while a whole-request condition (backpressure, publish/dedup backend failure)
// aborts the batch. Returns 200 with a per-record summary once the body is
// consumed.
func (h *IngestHandler) handleNDJSON(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	table, scope string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
	role string,
	now time.Time,
) {
	var result ndjsonResult

	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxNDJSONLineBytes)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue // skip blank lines between records
		}
		result.Total++
		lineNo := result.Total

		var data map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&data); err != nil {
			h.recordNDJSONError(&result, lineNo, "invalid json")
			continue
		}

		dup, reject, abort := h.processRecord(ctx, table, scope, schema, perms, role, data, now)
		if abort != nil {
			// Whole-request failure (backpressure / publish / dedup backend):
			// stop and surface the status so the caller retries the batch
			// instead of treating a system outage as per-record loss.
			if abort.RetryAfter != "" {
				w.Header().Set("Retry-After", abort.RetryAfter)
			}
			writeJSONError(w, abort.Status, abort.Message)
			return
		}
		if reject != nil {
			h.recordNDJSONError(&result, lineNo, reject.Message)
			continue
		}
		if dup {
			result.Duplicates++
			continue
		}
		result.Succeeded++
	}

	if err := scanner.Err(); err != nil {
		// A record exceeding maxNDJSONLineBytes (bufio.ErrTooLong) or a body
		// read error — the scanner can't reliably resume, so fail the request.
		h.logger.WarnContext(ctx, "ndjson read error", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, "invalid ndjson: "+err.Error())
		return
	}

	if result.Total == 0 {
		writeJSONError(w, http.StatusBadRequest, "empty ndjson body")
		return
	}

	h.logger.InfoContext(ctx, "ndjson batch ingested", "table", table,
		"total", result.Total, "succeeded", result.Succeeded,
		"failed", result.Failed, "duplicates", result.Duplicates)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// recordNDJSONError counts a per-line failure and appends it to the response up
// to maxReportedNDJSONErrors. Failed always reflects the true total even when
// the Errors slice is truncated.
func (h *IngestHandler) recordNDJSONError(result *ndjsonResult, line int, msg string) {
	result.Failed++
	if len(result.Errors) < maxReportedNDJSONErrors {
		result.Errors = append(result.Errors, ndjsonLineError{Line: line, Error: msg})
	}
}

// processRecord runs the per-record pipeline shared by the single-object and
// NDJSON ingest paths: schema validation → column/check permission enforcement
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

	// Optional deduplication.
	if h.Dedup != nil && h.IDField != "" {
		if idVal, ok := data[h.IDField]; ok {
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

// isNDJSONContentType reports whether the request Content-Type selects the
// NDJSON batch path. It matches application/x-ndjson and common synonyms
// (application/ndjson, application/jsonl, application/jsonlines), ignoring
// parameters such as "; charset=utf-8". Anything else — including a missing
// type or application/json — uses the single-object path.
func isNDJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	switch mediaType {
	case "application/x-ndjson", "application/ndjson", "application/jsonl", "application/jsonlines":
		return true
	default:
		return false
	}
}
