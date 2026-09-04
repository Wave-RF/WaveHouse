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
	Registry *discovery.SchemaRegistry
	Dedup    dedupe.Deduplicator // nil when no dedupe store is wired (tests)
	// DedupeSettings resolves the effective dedupe id_field/require_id for a
	// table (settings.Store.DedupeFor in production). Called once per record so
	// a settings reload lands at a record boundary — one record never mixes two
	// documents' values. Dedup is skipped when nil.
	DedupeSettings func(table string) (enabled bool, idField string, requireID bool)
	Publisher      mq.Publisher
	PolicySource   policy.Source
	logger         *slog.Logger

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

// dedupeDisabledCounter counts records published un-deduped because the
// settings snapshot said dedupe was on while the store was switched off —
// transient across a reload; a climbing rate means the store and the
// settings have come apart.
var dedupeDisabledCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_dedupe_disabled_total",
	metric.WithDescription("Ingested records published without dedupe because the store was switched off while settings said enabled (reload window)"),
)

// batchResult is the response body for any multi-record ingest (a JSON array,
// an NDJSON batch, and — later — CSV). The status is 200 whenever the body was
// readable and the records were processed; per-record rejections (malformed
// JSON, schema or permission failures) are reported in Results without failing
// the whole request, so one bad record never obscures the rest of the batch
// (issue #195). Whole-request conditions abort with a non-200 status instead —
// see requestAbort for the list, which lives there and only there, because
// stating it in three places is how two of them went stale.
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

// requestAbort is a whole-request failure: this record and every one that
// follows is refused. Both paths stop and return the status; the batch path
// abandons the remaining records rather than silently losing the tail.
//
// Most causes are TRANSIENT system conditions, where abandoning the tail is what
// makes the batch safe to retry: publish backpressure (503), a publish/marshal
// failure (500), a dedup backend error (500).
//
// One is not. An insert grant that resolved for the other operation is a 403 and
// a caller/config bug — retrying cannot help. It aborts rather than rejecting
// per record because the grant is resolved ONCE per request, so it is true for
// every record or none; as a per-record reject a 10k batch would report 10k
// independent permission failures for a single mis-wired grant.
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

	if h.PolicySource != nil {
		p := h.PolicySource()
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

	// Bound the inbound body (parity with /v1/ops/query; also caps the
	// array/stream decode vectors). See query.go for maxRequestBodyBytes.
	reqCap := int64(maxRequestBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)

	// Pick a reader from the DECLARED format; the body only chooses arity within
	// the JSON family. Resolving every header line and requiring agreement is what
	// stops a request declaring both application/json and application/x-ndjson
	// from silently taking the JSON path — an NDJSON body read as one object
	// ingests record one and drops the rest behind a 200. See resolveContentType
	// for when a comma-bearing value is refused rather than split.
	values := r.Header.Values("Content-Type")
	format, err := resolveContentType(values)
	if err != nil {
		conflicting := errors.Is(err, errConflictingContentType)
		if conflicting {
			// Logged with the whole declaration set, not just the first line:
			// this is the one refusal whose cause is usually NOT the caller's own
			// doing, and a proxy that starts duplicating the header would
			// otherwise produce a wall of client-side 415s whose server-side
			// record named only one of the two declarations involved.
			h.logger.WarnContext(ctx, "conflicting ingest content-type declarations",
				"content_types", echoSafe(values), "table", table)
		} else {
			// Logs the whole set, like the conflicting branch: the response body
			// names every header line, so logging only Header.Get would make the
			// server-side record disagree with what the caller was told — for
			// ["", "text/csv"] the client sees `Content-Type "", "text/csv"`
			// while Header.Get would have logged only content_type="".
			h.logger.WarnContext(ctx, "ingest content-type not declared or not supported",
				"content_types", echoSafe(values), "table", table)
		}
		writeJSONError(w, http.StatusUnsupportedMediaType, contentTypeMessage(values, conflicting))
		return
	}

	// The reader is built from the RESOLVED format, not from a second look at
	// the header. Re-resolving here would duplicate the rule in two places,
	// which is how the joined and repeated paths came to disagree before. The
	// only error left is an empty body.
	rr, batch, err := newRecordReader(format, r.Body)
	if err != nil {
		h.logger.ErrorContext(ctx, "empty ingest body", "table", table, "format", format.String())
		writeJSONError(w, http.StatusBadRequest, emptyBodyMessage(format))
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
// single insert. A record that fails validation or a PER-RECORD permission rule
// (a denied column, a failed check clause) — or that the reader couldn't decode
// — is recorded against its index and the batch continues; a whole-request
// condition aborts it (see requestAbort). Returns 200 with a per-record summary
// once the body is consumed.
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
			// Whole-request failure: surface the status rather than recording a
			// request-scoped condition as per-record loss (see requestAbort).
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
			if !perms.IsColumnAllowed(col, true) {
				h.logger.WarnContext(ctx, "column insertion forbidden", "column", col, "role", role)
				return false, &recordReject{
					Status:  http.StatusForbidden,
					Message: fmt.Sprintf("column %q not allowed for insert", col),
				}, nil
			}
		}
		// Through the accessor, not a bare read. The check loop iterates a side's
		// map rather than asking about a column, so IsColumnAllowed cannot cover it,
		// and a bare read presents an empty map on an unresolved side — every check
		// then passes vacuously. ok=false ABORTS the request; it must never be read
		// as "no checks to run".
		//
		// Reachable, unlike the query path's bare reads: discovery.Validate only
		// requires a column that is neither nullable nor defaulted, so a table whose
		// columns are all nullable or all defaulted accepts `{}`, and the column loop
		// above then runs zero times.
		checks, resolved := perms.CheckClauses()
		if !resolved {
			// ABORT, not a per-record reject: perms is resolved once per request,
			// so this is true for every record or none. As a reject, a 10k-record
			// batch would emit 10k ERROR lines and report 10k independent
			// permission failures for one mis-wired grant.
			h.logger.ErrorContext(ctx, "insert checks consulted on a grant resolved for another operation",
				"table", table, "role", role)
			return false, nil, &requestAbort{
				Status:  http.StatusForbidden,
				Message: "insert permissions were not resolved for this request",
			}
		}
		for col, requiredVal := range checks {
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
				// Both sides canonicalize through policy.CanonicalScalar, so a
				// numeric insert value matches a numeric claim by value, not by
				// spelling (payload 1.0 vs claim 1), and a value with no canonical
				// form (object/array/null) matches nothing. A policy.LiteralValue —
				// a placeholder-free check value, which carries no JSON type —
				// additionally matches by its numeric reading, so `_eq: "1.0"`
				// accepts an inserted 1.0 as well as an inserted "1.0". The type is
				// what scopes that second reading to author-written literals: a
				// claim-derived value arrives as a plain string and never gains a
				// reading the token's own JSON type didn't give it.
				if !checkValueMatches(actual, requiredVal) {
					h.logger.WarnContext(ctx, "check clause failed", "column", col, "expected", requiredVal, "actual", actual)
					return false, &recordReject{
						Status:  http.StatusForbidden,
						Message: fmt.Sprintf("check failed for column %q", col),
					}, nil
				}
			} else {
				// Auto-inject the required value if not provided — as a plain
				// string: a LiteralValue must not leak its named type into the
				// published payload, where downstream type switches (timestamp
				// canonicalization's `case string`) would silently miss it.
				if lit, isLit := requiredVal.(policy.LiteralValue); isLit {
					data[col] = string(lit)
				} else {
					data[col] = requiredVal
				}
			}
		}
	}

	// Canonicalize timestamps to RFC 3339 UTC (#372; fail-open — #381's row-filter
	// enforces) after the permission checks: check clauses keep pre-#372 semantics.
	discovery.CanonicalizeTimestamps(schema, data)

	// Optional deduplication. enabled/id_field/require_id resolve per record
	// from one snapshot (table override → global; the settings directory
	// always states them, so no compiled fallback is needed), so a reload
	// lands at a record boundary. A Deduplicator without a settings source is
	// a wiring bug, not a mode — main wires both or neither.
	if h.Dedup != nil && h.DedupeSettings != nil {
		if enabled, idField, requireID := h.DedupeSettings(table); enabled {
			idVal, ok := data[idField]
			if !ok {
				dedupeMissingIDCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", table)))
				if requireID {
					h.logger.WarnContext(ctx, "dedupe id_field missing; rejecting", "id_field", idField, "table", table)
					return false, &recordReject{
						Status:  http.StatusBadRequest,
						Message: fmt.Sprintf("missing dedupe id field %q", idField),
					}, nil
				}
				h.logger.WarnContext(ctx, "dedupe id_field missing; publishing without idempotency", "id_field", idField, "table", table)
			} else {
				eventID := fmt.Sprint(idVal)
				dup, err := h.Dedup.CheckAndMark(ctx, eventID)
				switch {
				case errors.Is(err, dedupe.ErrDisabled):
					// A reload flipped dedupe.enabled between the snapshot
					// read above and this call (the two transition at
					// different instants). Publish un-deduped, as a record
					// under the other setting would have been. The counter
					// carries the signal (a burst is a reload; a steady rate
					// is the store and settings out of step), so the line is
					// Debug rather than a WARN per record.
					dedupeDisabledCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", table)))
					h.logger.DebugContext(ctx, "dedupe switched off mid-reload; publishing without idempotency", "event_id", eventID, "table", table)
				case err != nil:
					h.logger.ErrorContext(ctx, "dedupe check failed", "error", err, "event_id", eventID)
					return false, nil, &requestAbort{Status: http.StatusInternalServerError, Message: "dedupe failed"}
				case dup:
					h.logger.InfoContext(ctx, "duplicate event skipped", "event_id", eventID)
					return true, nil, nil
				}
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

// checkValueMatches decides insert-check equality: the payload value must
// have a canonical scalar form (object/array/null match nothing) equal to the
// required value's canonical form. A policy.LiteralValue — and only that type,
// which Evaluate reserves for placeholder-free check values — also matches by
// its canonical numeric reading (policy.CanonicalNumericLiteral), so a static
// `_eq: "1.0"` accepts an inserted 1.0 and an inserted "1.0" alike.
func checkValueMatches(actual, required any) bool {
	actualStr, hasForm := policy.CanonicalScalar(actual)
	if !hasForm {
		return false
	}
	if lit, isLit := required.(policy.LiteralValue); isLit {
		if actualStr == string(lit) {
			return true
		}
		n, ok := policy.CanonicalNumericLiteral(string(lit))
		return ok && actualStr == n
	}
	requiredStr, ok := policy.CanonicalScalar(required)
	return ok && actualStr == requiredStr
}

// valueInSet reports whether v matches any member of set, comparing by
// canonical string form (policy.CanonicalScalar) to mirror the scalar check's
// claim-derived equality — a JSON number in the insert body matches a
// claim-derived value by value, not spelling, and a v with no canonical form
// (object/array/null) is a member of no set. _in members never take the
// LiteralValue numeric reading: an _in set is claim-derived by design, and a
// placeholder-free _in template is a degenerate one-element set that keeps
// spelling equality.
func valueInSet(v any, set []any) bool {
	vs, ok := policy.CanonicalScalar(v)
	if !ok {
		return false
	}
	for _, s := range set {
		if ss, ok := policy.CanonicalScalar(s); ok && ss == vs {
			return true
		}
	}
	return false
}
