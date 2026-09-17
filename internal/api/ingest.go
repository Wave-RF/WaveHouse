package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"

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
	// Types answers "would this record insert?" with ClickHouse's own parser.
	// Nil is never "skip validation": the handler refuses the request, because
	// nothing else on this path looks at a value.
	Types *typelayer.Engine
	Dedup dedupe.Deduplicator // nil when no dedupe store is wired (tests)
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

	// noticeMu guards noticeLast, the last time each rate-limited notice was
	// logged. A missing artifact or a policy whose injected literal will not
	// compile is a standing condition: one line per request would bury the rest
	// of the log under it.
	noticeMu   sync.Mutex
	noticeLast map[string]time.Time
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

// batchResult is the response body for any multi-record ingest: a JSON array,
// an NDJSON batch, a CSV or a TSV body. The status is 200 whenever the body was
// readable and the records were processed; per-record rejections (unparseable
// values, unknown columns, failed check clauses) are reported in Results without
// failing the whole request, so one bad record never obscures the rest of the
// batch (issue #195). Whole-request conditions abort with a non-200 status
// instead — see requestAbort for the list, which lives there and only there,
// because stating it in three places is how two of them went stale.
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
	// Code is ClickHouse's own error code when the record was refused by the
	// server's parser (117 unknown field — which now includes a column the role
	// may not write, 27 unparseable value, 6 out of range). Absent for a gateway
	// rejection — a failed check clause is our verdict, not ClickHouse's, and
	// must not be dressed as one.
	Code int `json:"code,omitempty"`
}

// recordReject is a per-record rejection: this record is bad (ClickHouse's
// parser refused it, or a policy check clause did), but the rest of the batch
// can still proceed. The single-object path maps Status to the HTTP code; the
// batch path records Message against the index and keeps going.
type recordReject struct {
	Status  int
	Message string
	Code    int // ClickHouse's code; 0 for a gateway rejection (see recordResult.Code)
}

// requestAbort is a whole-request failure: this record and every one that
// follows is refused. Both paths stop and return the status; the batch path
// abandons the remaining records rather than silently losing the tail.
//
// Most causes are TRANSIENT system conditions, where abandoning the tail is what
// makes the batch safe to retry: publish backpressure (503), a publish/marshal
// failure (500), a dedup backend error (500), a type layer that cannot answer
// (503).
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

// ingestRun is one request's state after the body has been framed and ruled on: the
// handle that produced the bytes, chtypes' verdict per record, and the check
// verdicts over the accepted ones. Both response shapes read their records out
// of it, so the single-object and batch paths cannot disagree about what a
// record's outcome is — only about how it is rendered.
type ingestRun struct {
	table, scope string
	tbl          *typelayer.Table
	batch        typelayer.Batch
	// records is how many records the request contains: the body's own framing
	// before chtypes has read it (an empty array is zero, anything else is at
	// least one), then len(batch.Rows) once it has answered.
	records int
	// verdicts/reasons are index-aligned with the ACCEPTED records, not with
	// batch.Rows, so they are read through the accepted cursor.
	verdicts []bool
	reasons  []string
	// checkColumns names the check clauses the verdicts came from, for the
	// rejection message. The filter is AND-joined over all of them, so a false
	// verdict does not say which one failed — with one clause it does.
	checkColumns []string
	// checkGuard is the rejection every otherwise-acceptable record gets when
	// the role's check clauses name columns no record can carry.
	checkGuard *recordReject
	accepted   int // cursor into verdicts/reasons
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

	if table == "" {
		h.logger.ErrorContext(ctx, "missing table parameter in request")
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

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

	// Resolve the DECLARED format before touching the body: an undeclared or
	// unreadable Content-Type is a 415, and there is no reason to read (let
	// alone buffer) bytes for a request already known to be unreadable.
	//
	// Resolving every header line and requiring agreement is what stops a request
	// declaring both application/json and application/x-ndjson from silently
	// taking the JSON path — an NDJSON body read as one object ingests record one
	// and drops the rest behind a 200. See resolveContentType for when a
	// comma-bearing value is refused rather than split.
	values := r.Header.Values("Content-Type")
	format, pin, err := resolveContentType(values)
	if err != nil {
		conflicting := errors.Is(err, errConflictingContentType)
		// ONE bounded set, read by both the log and the response — pin comes from
		// resolveContentType, so neither the declaration named nor the set it sits
		// in can differ between them. Two call sites passing matching arguments is
		// what let them diverge before.
		decls := echoSafe(values, pin)
		if conflicting {
			// Logged with the same bounded set the response shows, not just the
			// first line: this is the one refusal whose cause is usually NOT the
			// caller's own doing, and a proxy that starts duplicating the header
			// would otherwise produce a wall of client-side 415s whose
			// server-side record named only one of the declarations involved.
			h.logger.WarnContext(ctx, "conflicting ingest content-type declarations",
				"content_types", decls, "table", table)
		} else {
			// Logs the same bounded set the response shows, like the conflicting
			// branch: logging only Header.Get would make the server-side record
			// disagree with what the caller was told — for
			// ["", "text/csv"] the client sees `Content-Type "", "text/csv"`
			// while Header.Get would have logged only content_type="".
			h.logger.WarnContext(ctx, "ingest content-type not declared or not supported",
				"content_types", decls, "table", table)
		}
		writeJSONError(w, http.StatusUnsupportedMediaType, contentTypeMessage(decls, conflicting))
		return
	}

	// Bound the inbound body (parity with /v1/ops/query). See query.go for
	// maxRequestBodyBytes.
	reqCap := int64(maxRequestBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)

	// Read the whole (already-capped) body up front and hand those bytes to
	// ClickHouse's own parser. The buffer comes from a pool and goes back on the
	// way out; the exported rows point INTO the type layer's payload, not into
	// this buffer, and are copied into the envelope before it is released.
	body := getBodyBuffer()
	defer putBodyBuffer(body)
	if _, err := body.ReadFrom(r.Body); err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		h.logger.WarnContext(ctx, "ingest body read failed", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Framing: the declared format says how the body frames its records, and the
	// first non-whitespace byte answers the one question left inside the JSON
	// family — array or single object. That byte is the ONLY thing the body gets
	// to decide, and it decides the response shape (AUDIT §A.6), never the
	// format.
	first, ok := firstNonSpace(body.Bytes())
	if !ok {
		h.logger.ErrorContext(ctx, "empty ingest body", "table", table, "format", format.String())
		writeJSONError(w, http.StatusBadRequest, emptyBodyMessage(format))
		return
	}
	batchShape := format.alwaysBatch() || first == '['
	var records int
	switch {
	case format == FormatJSON && first == '[':
		n, framed := reframeArray(body.Bytes())
		if !framed {
			// Brackets that do not balance: a truncated upload or a structural
			// syntax error. Neither can be salvaged per record, and reporting the
			// records that did arrive as a complete batch is the failure this
			// refusal exists to prevent.
			h.logger.WarnContext(ctx, "ingest read error", "error", "unterminated json array", "table", table)
			writeJSONError(w, http.StatusBadRequest, "invalid json: unterminated json array")
			return
		}
		records = n
	default:
		// A single-object body is one record (concatenated objects after it are
		// ignored, as they always have been — declare NDJSON to batch them, #561);
		// a line-framed body has at least the record its first byte starts. The
		// real count is chtypes' own, taken from the batch once it has answered.
		records = 1
	}

	// The role's projection of the table answers column policy and the auto
	// -injected check values inside ClickHouse's own parser, so no Go code walks
	// the record's keys. It is taken once and held for the whole request, so
	// every record is judged by one generation and one shape: a refresh that
	// recompiles the table waits for this request rather than changing the
	// answer halfway through a batch. Resolved after the framing checks so a
	// 415/413/empty-body/unterminated request is still refused for its own
	// reason when the type layer happens to be down.
	guard := h.policyCheckGuard(ctx, table, role, schema, perms)
	shape, preds, checkColumns, abort := h.insertShape(ctx, table, role, schema, perms, guard)
	if abort != nil {
		writeAbort(w, abort)
		return
	}
	tbl, abort := h.roleTable(ctx, table, shape)
	if abort != nil {
		writeAbort(w, abort)
		return
	}
	defer tbl.Release()

	st := &ingestRun{table: table, scope: scope, tbl: tbl, records: records, checkColumns: checkColumns, checkGuard: guard}
	if records > 0 {
		if abort := h.judge(ctx, st, format.wire(), body.Bytes(), preds); abort != nil {
			writeAbort(w, abort)
			return
		}
	}

	if batchShape {
		h.writeBatch(ctx, w, st, now)
		return
	}
	h.writeSingle(ctx, w, st, now)
}

// judge asks ClickHouse's own parser for a verdict per record — ONE call for the
// whole body, no chunking (AUDIT §A.9) — and then, when the role carries insert
// check clauses, evaluates them as ONE compiled filter over the exported rows.
//
// Both are Go-level failures or data verdicts, never both: an unavailable handle
// is the type layer's outage and anything else is ours, and neither is the
// caller's record to fix.
func (h *IngestHandler) judge(ctx context.Context, st *ingestRun, wire typelayer.Format, body []byte, preds []policy.Predicate) *requestAbort {
	batch, err := st.tbl.Ingest(wire, body)
	if err != nil {
		var un *typelayer.Unavailable
		if errors.As(err, &un) {
			h.logUnavailable(ctx, st.table, un.Cause)
			return unavailableAbort(un.Cause)
		}
		h.logger.ErrorContext(ctx, "record validation failed", "error", err, "table", st.table)
		return &requestAbort{Status: http.StatusInternalServerError, Message: "validation failed"}
	}
	st.batch = batch
	// chtypes' per-record answer is the record count: a JSON array sent as
	// NDJSON is however many elements its reader took, a blank line is nothing.
	// When it gave no per-record detail the padded batch is still index-shaped,
	// so the same rule keeps every later index in range.
	st.records = len(batch.Rows)

	if len(preds) == 0 || len(batch.Payload) == 0 {
		return nil
	}
	verdicts, reasons, err := st.tbl.CheckVerdicts(batch.Payload, preds)
	if err != nil {
		var un *typelayer.Unavailable
		if errors.As(err, &un) {
			h.logUnavailable(ctx, st.table, un.Cause)
			return unavailableAbort(un.Cause)
		}
		h.logger.ErrorContext(ctx, "insert check evaluation failed", "error", err, "table", st.table)
		return &requestAbort{Status: http.StatusInternalServerError, Message: "check evaluation failed"}
	}
	st.verdicts, st.reasons = verdicts, reasons
	return nil
}

// resolveRecord decides the i-th record's outcome and, when it survives every
// rule, publishes it. Exactly one of the returns is set, or none (published).
//
// The order is the one the type layer imposes and is a documented change: a
// record that both fails to parse and violates a check clause now reports the
// PARSE error, because the check runs over rows ClickHouse has already accepted.
// Nothing is published either way, so no enforcement is lost (AUDIT §A.2).
func (h *IngestHandler) resolveRecord(ctx context.Context, st *ingestRun, i int, now time.Time) (duplicate bool, reject *recordReject, abort *requestAbort) {
	verdict := verdictAt(st.batch, i)
	if !verdict.Accepted {
		h.logVerdict(ctx, st.table, verdict)
		return false, verdictReject(verdict), nil
	}

	// The payload cursor advances for every accepted record, whatever happens
	// next: the check verdicts are index-aligned with the exported rows, so a
	// record skipped here would shift every later record onto another's answer.
	cursor := st.accepted
	st.accepted++

	if st.checkGuard != nil {
		return false, st.checkGuard, nil
	}
	if st.verdicts != nil {
		if cursor >= len(st.verdicts) {
			// Fewer verdicts than exported rows: the type layer already logged
			// the mismatch. Withhold rather than publish an unchecked row.
			return false, checkReject(typelayer.ReasonDecline, st.checkColumns), nil
		}
		if !st.verdicts[cursor] {
			h.logger.WarnContext(ctx, "check clause failed",
				"columns", st.checkColumns, "reason", st.reasons[cursor], "table", st.table)
			return false, checkReject(st.reasons[cursor], st.checkColumns), nil
		}
	}
	return h.publishAccepted(ctx, st, verdict.Line, now)
}

// writeSingle answers a lone flat JSON object and preserves the GA response
// contract: 200 {"ok":true} (or {"duplicate":true} when dedup skips it), or the
// matching non-200 on refusal / permission / whole-request failure.
func (h *IngestHandler) writeSingle(ctx context.Context, w http.ResponseWriter, st *ingestRun, now time.Time) {
	dup, reject, abort := h.resolveRecord(ctx, st, 0, now)
	switch {
	case abort != nil:
		writeAbort(w, abort)
	case reject != nil:
		writeJSONErrorCode(w, reject.Status, reject.Message, reject.Code)
	case dup:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
	default:
		h.logger.InfoContext(ctx, "event successfully ingested", "table", st.table)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}
}

// writeBatch answers a multi-record body (JSON array, NDJSON, CSV or TSV). A
// record ClickHouse's parser refused, or one a check clause denied, is recorded
// against its index and the batch continues; a whole-request condition aborts it
// (see requestAbort). Returns 200 with a per-record summary.
func (h *IngestHandler) writeBatch(ctx context.Context, w http.ResponseWriter, st *ingestRun, now time.Time) {
	result := batchResult{Total: st.records, Results: []recordResult{}}
	for i := range st.records {
		dup, reject, abort := h.resolveRecord(ctx, st, i, now)
		if abort != nil {
			// Whole-request failure: surface the status rather than recording a
			// request-scoped condition as per-record loss (see requestAbort).
			writeAbort(w, abort)
			return
		}
		entry := recordResult{Index: i + 1}
		switch {
		case reject != nil:
			entry.Error, entry.Code = reject.Message, reject.Code
			result.Failed++
		case dup:
			entry.Duplicate = true
			result.Duplicates++
		default:
			entry.Ok = true
			result.Succeeded++
		}
		// Up to maxReportedResults entries are echoed; the counts above stay
		// authoritative even when the slice is truncated.
		if len(result.Results) < maxReportedResults {
			result.Results = append(result.Results, entry)
		}
	}

	h.logger.InfoContext(ctx, "batch ingested", "table", st.table,
		"total", result.Total, "succeeded", result.Succeeded,
		"failed", result.Failed, "duplicates", result.Duplicates)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// insertShape turns the role's resolved insert grant into the projection of the
// table ClickHouse itself will enforce, plus the predicates the check clauses
// become.
//
// Columns is the allow/deny decision, answered by omitting the denied columns
// from the compiled schema: a record naming one is then refused per row with
// ClickHouse's own code 117 rather than by a Go walk over the record's keys
// (AUDIT §A.1, decision D1). nil means the role may write every column, which
// compiles to no second handle at all.
//
// Defaults is the `_eq` auto-inject: the required value becomes the column's
// DEFAULT, so a record that omits it is filled and a record that supplies one
// still wins (measured, AUDIT §A.3). An `_in` check has no single value to
// inject, so the column keeps the TABLE's own default and the filter tests that
// — a documented behaviour change (D3).
func (h *IngestHandler) insertShape(
	ctx context.Context,
	table, role string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
	guard *recordReject,
) (typelayer.RoleShape, []policy.Predicate, []string, *requestAbort) {
	if perms == nil {
		return typelayer.RoleShape{}, nil, nil, nil
	}

	// Through the accessor, not a bare read: a bare read presents an empty map
	// on an unresolved side and every check then passes vacuously. ok=false
	// ABORTS the request; it must never be read as "no checks to run".
	checks, resolved := perms.CheckClauses()
	if !resolved {
		// ABORT, not a per-record reject: perms is resolved once per request, so
		// this is true for every record or none. As a reject, a 10k-record batch
		// would emit 10k ERROR lines and report 10k independent permission
		// failures for one mis-wired grant.
		h.logger.ErrorContext(ctx, "insert checks consulted on a grant resolved for another operation",
			"table", table, "role", role)
		return typelayer.RoleShape{}, nil, nil, &requestAbort{
			Status:  http.StatusForbidden,
			Message: "insert permissions were not resolved for this request",
		}
	}

	shape := typelayer.RoleShape{Columns: allowedInsertColumns(schema, perms)}
	if guard != nil {
		// The guard's own columns cannot be injected into or filtered on — that
		// is what it refuses — so the shape stays bare and every record that
		// parses gets the guard's rejection instead.
		return shape, nil, nil, nil
	}

	cols := slices.Sorted(maps.Keys(checks))
	preds := make([]policy.Predicate, 0, len(cols))
	for _, col := range cols {
		switch v := checks[col].(type) {
		case []any:
			// An _in set. A nil/empty set is an unresolvable claim and matches
			// nothing — CheckVerdicts short-circuits it to all-false without
			// compiling anything (#224).
			vals := make([]string, 0, len(v))
			for _, e := range v {
				s, ok := scalarString(e)
				if !ok {
					vals = nil
					break
				}
				vals = append(vals, s)
			}
			preds = append(preds, policy.Predicate{Column: col, Op: "in", Values: vals})
		default:
			s, ok := scalarString(checks[col])
			if !ok {
				// A required value with no string form cannot be expressed as a
				// filter constant, and admitting the record would drop the check
				// entirely. An empty Values list matches nothing.
				preds = append(preds, policy.Predicate{Column: col, Op: "="})
				continue
			}
			if shape.Defaults == nil {
				shape.Defaults = make(map[string]string, len(cols))
			}
			shape.Defaults[col] = s
			preds = append(preds, policy.Predicate{Column: col, Op: "=", Values: []string{s}})
		}
	}
	return shape, preds, cols, nil
}

// allowedInsertColumns is the role's writable column set, or nil when it may
// write every column the table has. nil is the identity shape, which reuses the
// table's own compiled handle instead of a second one.
//
// Every column is asked through IsColumnAllowed so the allow/deny precedence
// stays in the one place that owns it; the computed kinds are included for the
// same reason, and typelayer keeps them whatever this list says (they are the
// server's to compute, and a MATERIALIZED expression over a dropped column would
// not compile at all).
func allowedInsertColumns(schema *discovery.TableSchema, perms *policy.ResolvedPermissions) []string {
	allowed := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		if perms.IsColumnAllowed(c.Name, true) {
			allowed = append(allowed, c.Name)
		}
	}
	if len(allowed) == len(schema.Columns) {
		return nil
	}
	return allowed
}

// scalarString renders a check clause's required value as the string the filter
// binds. Every filter parameter binds as {pN:String} whatever the column's
// declared type (AUDIT §C.1), so this is the only conversion the check path
// needs.
//
// The reflect.Kind test rather than a type switch is deliberate: policy marks a
// placeholder-free check value with its own string-kinded named type, a
// distinction that existed only for the Go-side numeric re-reading ClickHouse
// now answers and that policy drops with it. Naming the type here would keep it
// alive; asking for its kind works across the change.
func scalarString(v any) (string, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}

// roleTable resolves the compiled handle for this role's projection, or the 503
// every record of this request gets instead. The type layer being down is an
// outage, never a verdict about the data: a caller must be able to retry the
// same body unchanged.
//
// An injected literal the column cannot read (`count UInt64 DEFAULT 'abc'`) is a
// compile refusal, ClickHouse code 6 — measured. That must not become a 503 for
// a policy that is simply unsatisfiable, so the shape is retried without its
// defaults: the check filter then judges the record as sent, which fails closed
// (an absent column takes the table default and the filter refuses it). The
// type layer logs the refusal once per generation and shape; this adds one
// rate-limited line saying what was done about it.
func (h *IngestHandler) roleTable(ctx context.Context, table string, shape typelayer.RoleShape) (*typelayer.Table, *requestAbort) {
	if h.Types == nil {
		// Nothing else on this path inspects a value, so an unwired type layer
		// cannot mean "accept anything" — the same fail-closed direction the
		// stream's row evaluator takes.
		h.logUnavailable(ctx, table, "no type layer is wired")
		return nil, unavailableAbort("ingest validation is unavailable")
	}
	tbl, err := h.Types.RoleTable(table, shape)
	if err == nil {
		return tbl, nil
	}
	if len(shape.Defaults) > 0 {
		bare := typelayer.RoleShape{Columns: shape.Columns}
		if t, bareErr := h.Types.RoleTable(table, bare); bareErr == nil {
			if h.noticeDue("inject:" + table) {
				h.logger.WarnContext(ctx, "insert check value cannot be injected as a column default; records omitting it will fail the check",
					"table", table, "columns", slices.Sorted(maps.Keys(shape.Defaults)), "cause", err.Error())
			}
			return t, nil
		}
	}
	var un *typelayer.Unavailable
	if errors.As(err, &un) {
		h.logUnavailable(ctx, table, un.Cause)
		return nil, unavailableAbort(un.Cause)
	}
	h.logger.ErrorContext(ctx, "could not resolve the compiled schema", "error", err, "table", table)
	return nil, unavailableAbort(err.Error())
}

// unavailableAbort is the 503 for a type layer that cannot answer. Retry-After
// matches the publish-backpressure 503: both are "come back, nothing is wrong
// with your request".
func unavailableAbort(cause string) *requestAbort {
	return &requestAbort{Status: http.StatusServiceUnavailable, Message: cause, RetryAfter: "30"}
}

// logUnavailable emits at most one line per table per minute. A missing
// artifact or a timezone mismatch persists until an operator acts, so the
// per-request line says nothing the first one didn't.
func (h *IngestHandler) logUnavailable(ctx context.Context, table, cause string) {
	if h.noticeDue("unavailable:" + table) {
		h.logger.ErrorContext(ctx, "ingest type layer unavailable", "table", table, "cause", cause)
	}
}

// noticeDue rate-limits a standing-condition notice to one line per key per
// minute, so a condition that persists until an operator acts does not bury the
// rest of the log under one line per request.
func (h *IngestHandler) noticeDue(key string) bool {
	now := time.Now()
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	if last, seen := h.noticeLast[key]; seen && now.Sub(last) < time.Minute {
		return false
	}
	if h.noticeLast == nil {
		h.noticeLast = make(map[string]time.Time)
	}
	h.noticeLast[key] = now
	return true
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

// policyCheckGuard evaluates, ONCE per request, whether the role's insert
// `check` clauses name only columns a published row can actually carry, and
// returns the rejection every record should get when they do not.
//
// A check the row cannot carry can never be enforced: the row holds one slot
// per WIRE column, by position, so an injected value for anything outside that
// set is dropped on the way out and the record inserts WITHOUT the value the
// policy requires — answering 200. Policy validation cannot catch this; it never
// sees the ClickHouse schema. Three ways in, all refused.
//
// It is not redundant now that the compiled schema answers column policy: a
// Defaults entry for a column the shape cannot carry is a COMPILE refusal, so
// without this guard a mis-wired policy would be a 503 naming a ClickHouse
// internal rather than a 403 naming the column the operator has to fix.
//
// Evaluated here rather than per record because the condition is a property of
// (table, role, policy) and is identical for every record in the request. Doing
// it per record would emit one ERROR line per record for a single mis-wired
// policy, which on a 16 MiB body of small records is ~1.2M lines. The reject is
// still returned per record, so a batch reports each record's own cause: one
// that SUPPLIES the column is refused by ClickHouse first, with its own message.
func (h *IngestHandler) policyCheckGuard(
	ctx context.Context,
	table, role string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
) *recordReject {
	if perms == nil {
		return nil
	}
	checks, resolved := perms.CheckClauses()
	if !resolved {
		return nil // the !resolved abort in insertShape owns this case
	}

	// Sorted, and every offender — not the first one a map range happens to
	// yield. This message is the diagnostic an operator fixes their policy from:
	// reporting one of two broken columns, and a different one between
	// otherwise-identical requests, hides the second until the first is fixed.
	cols := make([]string, 0, len(checks))
	for col := range checks {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	var reasons []string
	for _, col := range cols {
		schemaCol, known := schema.Lookup(col)
		switch {
		case !known:
			h.logger.ErrorContext(ctx, "policy check references a column the table does not have",
				"column", col, "table", table, "role", role)
			reasons = append(reasons, fmt.Sprintf("%q, which table %q does not have", col, table))
		case !schemaCol.IsInsertable():
			h.logger.ErrorContext(ctx, "policy check references a column no record may write",
				"column", col, "table", table, "role", role, "default_kind", schemaCol.DefaultKind)
			reasons = append(reasons, fmt.Sprintf("%q of table %q, which is %s and cannot be inserted",
				col, table, strings.ToLower(schemaCol.DefaultKind)))
		case schemaCol.DefaultKind == "EPHEMERAL":
			// Insertable, so the row DOES carry a slot for it — but ClickHouse
			// never stores an ephemeral column and no query can read one back, so
			// the constraint is unverifiable the moment the insert returns.
			// Accepting a check that provably does nothing is worse than refusing
			// it. An operator wanting this should check the DEFAULT column derived
			// from the ephemeral one, which is stored and therefore enforceable.
			h.logger.ErrorContext(ctx, "policy check references an ephemeral column, which is never stored",
				"column", col, "table", table, "role", role)
			reasons = append(reasons, fmt.Sprintf("%q of table %q, which is ephemeral and is never stored",
				col, table))
		}
	}
	if len(reasons) == 0 {
		return nil
	}
	return &recordReject{
		Status:  http.StatusForbidden,
		Message: "policy check references column " + strings.Join(reasons, "; and column "),
	}
}

// verdictAt reads the verdict for the i-th record of a batch. A verdict the
// type layer did not return is a decline, never an acceptance: a missing answer
// must not publish a row nobody ruled on.
func verdictAt(batch typelayer.Batch, i int) typelayer.RowVerdict {
	if i < len(batch.Rows) {
		return batch.Rows[i]
	}
	return typelayer.RowVerdict{Declined: true, Message: "no verdict was returned for this record"}
}

// verdictReject maps a verdict that is not an acceptance to the record's
// rejection. A refusal is ClickHouse's own answer about the data — 400, with
// its code. A decline is the validation engine failing to answer at all, which
// is never the caller's fault and must never be dressed as a 400: 422 says "we
// could not judge this", so a retry is meaningful and a client cannot learn
// from it that its payload was wrong.
func verdictReject(v typelayer.RowVerdict) *recordReject {
	if v.Declined {
		return &recordReject{
			Status:  http.StatusUnprocessableEntity,
			Message: "validation engine declined: " + v.Message,
		}
	}
	return &recordReject{Status: http.StatusBadRequest, Message: v.Message, Code: v.Code}
}

// checkReject maps one check verdict that is not a definite true. "The data says
// no" is a 403; "we could not tell" is a 422, fail closed either way.
//
// The filter is AND-joined over every check clause, so a false verdict does not
// name which clause failed — with a single clause the column is unambiguous, and
// with several the message names the set that was tested rather than inventing
// an attribution.
func checkReject(reason string, cols []string) *recordReject {
	if reason == typelayer.ReasonFilter {
		return &recordReject{Status: http.StatusForbidden, Message: "check failed for " + columnList(cols)}
	}
	return &recordReject{
		Status:  http.StatusUnprocessableEntity,
		Message: "validation engine declined: the insert check for " + columnList(cols) + " could not be evaluated",
	}
}

// columnList renders a check's column set for a rejection message.
func columnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf("%q", c)
	}
	if len(cols) == 1 {
		return "column " + quoted[0]
	}
	return "columns " + strings.Join(quoted, ", ")
}

// logVerdict records a record ClickHouse would not take. A refusal carries its
// code so an operator can look it up without parsing the message; a decline is
// an ERROR because it is the engine, not the data, that failed.
func (h *IngestHandler) logVerdict(ctx context.Context, table string, v typelayer.RowVerdict) {
	if v.Declined {
		h.logger.ErrorContext(ctx, "validation engine declined a record", "reason", v.Message, "table", table)
		return
	}
	h.logger.WarnContext(ctx, "schema validation failed", "error", v.Message, "code", v.Code, "table", table)
}

// publishAccepted runs the two steps reserved for a record the server itself
// accepted: dedupe, then the publish of the row ClickHouse's own writer
// produced.
//
// Dedupe runs AFTER validation deliberately — the idempotency key must mark
// only what is actually published, or a record ClickHouse refuses would burn
// its id and a corrected retry would be swallowed as a duplicate.
//
// line is the exported JSONCompactEachRow row, a sub-slice of the type layer's
// payload, so it is copied into the envelope before the handle is released.
func (h *IngestHandler) publishAccepted(
	ctx context.Context,
	st *ingestRun,
	line []byte,
	now time.Time,
) (duplicate bool, reject *recordReject, abort *requestAbort) {
	// Optional deduplication. enabled/id_field/require_id resolve per record
	// from one snapshot (table override → global; the settings directory
	// always states them, so no compiled fallback is needed), so a reload
	// lands at a record boundary. A Deduplicator without a settings source is
	// a wiring bug, not a mode — main wires both or neither.
	if h.Dedup != nil && h.DedupeSettings != nil {
		if enabled, idField, requireID := h.DedupeSettings(st.table); enabled {
			eventID, ok := eventIDAt(line, slices.Index(st.tbl.WireColumns, idField))
			if !ok {
				dedupeMissingIDCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", st.table)))
				if requireID {
					h.logger.WarnContext(ctx, "dedupe id_field missing; rejecting", "id_field", idField, "table", st.table)
					return false, &recordReject{
						Status:  http.StatusBadRequest,
						Message: fmt.Sprintf("missing dedupe id field %q", idField),
					}, nil
				}
				h.logger.WarnContext(ctx, "dedupe id_field missing; publishing without idempotency", "id_field", idField, "table", st.table)
			} else {
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
					dedupeDisabledCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", st.table)))
					h.logger.DebugContext(ctx, "dedupe switched off mid-reload; publishing without idempotency", "event_id", eventID, "table", st.table)
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

	// The row travels POSITIONALLY as the bytes ClickHouse's own writer
	// produced, with the column names alongside rather than in the row: a batch
	// of rows for one table carries the names once, and the reader can tell a
	// schema change mid-stream from a reordering. WireColumns are the ROLE's
	// columns, so a role that may not write every column publishes a shorter row
	// with a matching name list — the worker already groups a flush by column
	// signature, so that is one more group, not a new code path.
	evt := ingest.EventMessage{
		TableName:         st.table,
		Scope:             st.scope,
		ReceivedTimestamp: now.Format(time.RFC3339Nano),
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           st.tbl.WireColumns,
		Row:               json.RawMessage(line),
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		h.logger.ErrorContext(ctx, "failed to marshal event message", "error", err)
		return false, nil, &requestAbort{Status: http.StatusInternalServerError, Message: "marshal failed"}
	}

	subject := "ingest." + query.SafeEncodeNATS(st.table)
	if st.scope != "" {
		subject += "." + query.SafeEncodeNATS(st.scope)
	}

	h.logger.DebugContext(ctx, "publishing event to NATS", "subject", subject, "table", st.table, "scope", st.scope)
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

// eventIDAt reads the dedupe id out of the exported row by POSITION — the id
// column's index in the table's wire columns — so no record is decoded for it
// (AUDIT §A.4). idx is -1 when the column is not on the wire at all.
//
// Two consequences worth knowing, both documented:
//   - the key is the STORED value, not the caller's spelling: `256` into a
//     UInt8 keys on `0`, and a DateTime keys on ClickHouse's rendering. For the
//     documented case — a string id — the two are identical.
//   - "missing" now means "the row carries no value", which for an omitted
//     column is its default. An empty id is therefore treated as absent, which
//     is what an omitted `event_id String` produces and what the WARN, the
//     counter and require_id have always been about. A numeric id column cannot
//     distinguish an omitted 0 from a supplied one.
func eventIDAt(line []byte, idx int) (string, bool) {
	if idx < 0 {
		return "", false
	}
	cell, ok := cellAt(line, idx)
	if !ok || len(cell) == 0 {
		return "", false
	}
	if cell[0] == '"' {
		// One scalar string, not the record: the cell is JSON-encoded by
		// ClickHouse's own writer (it escapes "/" as "\/"), so Go's own
		// string-literal unquoting would refuse it.
		var s string
		if err := json.Unmarshal(cell, &s); err != nil || s == "" {
			return "", false
		}
		return s, true
	}
	return string(cell), true
}
