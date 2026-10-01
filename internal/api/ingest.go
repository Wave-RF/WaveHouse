package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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

// ingestWindow is how many records a batch prepares before reserving,
// publishing and committing them together: one dedupe call per phase per
// window rather than per record, and at most one window of encoded envelopes
// held. The exported rows they are built from are held for the whole request —
// one parse per body — and are bounded by the body cap.
const ingestWindow = 256

// IngestHandler handles POST /v1/ingest?table={table}
type IngestHandler struct {
	// Registry yields the request tenant's schema registry.
	Registry RegistrySource
	// Types is the process's type layer: ClickHouse's own parser, which judges
	// every record against the request tenant's compiled schema. Nil is never
	// "skip validation" — nothing else on this path looks at a value — so a
	// handler without one refuses every request it would have judged with a 503.
	Types *typelayer.Engine
	// Dedup resolves the request tenant's deduplicator — the tenant's own
	// store, picked off the store the handler already holds (#583 story 7;
	// dedupe.Stores in production). nil when no dedupe store is wired (tests).
	Dedup func(store *settings.Store) dedupe.Deduplicator
	// DedupeSettings resolves the effective dedupe settings for a table of the
	// request's tenant ((*settings.Store).DedupeFor in production). Called
	// once per record so a settings reload lands at a record boundary — one
	// record never mixes two documents' values. Dedup is skipped when nil.
	DedupeSettings func(store *settings.Store, table string) settings.Dedupe
	// DedupeLease is how long a record's claimed id stays pending while it is
	// published; 0 means dedupe.DefaultLease.
	DedupeLease  time.Duration
	Publisher    mq.Publisher
	PolicySource PolicySource

	// maxRequestBytes optionally overrides the default inbound request body cap
	// (maxRequestBodyBytes). When 0, the default applies. Exists so same-package
	// tests can pin the cap-overflow path without allocating 16 MiB per run; not
	// a production tuning knob, hence unexported. Mirrors QueryHandler.
	maxRequestBytes int64
	// window overrides ingestWindow when > 0, for tests and benchmarks.
	window int

	// noticeMu guards noticeLast, the last time each rate-limited notice was
	// logged. A tenant or table the type layer cannot serve, or a policy whose
	// injected literal will not compile, is a standing condition: one line per
	// request would bury the rest of the log under it.
	noticeMu   sync.Mutex
	noticeLast map[string]time.Time
}

func NewIngestHandler(registry RegistrySource, pub mq.Publisher) *IngestHandler {
	return &IngestHandler{Registry: registry, Publisher: pub}
}

var dedupeMissingIDCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_dedupe_missing_id_total",
	metric.WithDescription("Ingested records missing the configured dedupe id_field (idempotency skipped)"),
)

// dedupeCommitFailedCounter counts records published whose id could not be
// committed afterwards: a retry after the lease lapses publishes them again.
var dedupeCommitFailedCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_dedupe_commit_failed_total",
	metric.WithDescription("Published records whose dedupe id failed to commit afterwards (the claim lapses with its lease)"),
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
	// ExceptionCode is ClickHouse's own error code when the record was refused
	// by the server's parser (117 unknown field — which includes a column the
	// role may not write, 27 unparseable value, 41 a bad DateTime; an
	// out-of-range integer wraps rather than refusing). Absent for a
	// gateway rejection — a failed check clause is our verdict, not
	// ClickHouse's, and must not be dressed as one.
	ExceptionCode int `json:"exception_code,omitempty"`
}

// recordReject is a per-record rejection: this record is bad (ClickHouse's
// parser refused it, or a policy check clause did), but the rest of the batch
// can still proceed. The single-object path maps Status to the HTTP code; the
// batch path records Message against the index and keeps going.
type recordReject struct {
	Status        int
	Message       string
	ExceptionCode int // ClickHouse's code; 0 for a gateway rejection (see recordResult)
}

// requestAbort is a whole-request failure: this record and every one that
// follows is refused. Both paths stop and return the status; the batch path
// abandons the remaining records rather than silently losing the tail. What
// earlier windows published stays published, and with dedupe on stays
// committed, so a whole-batch retry reports those records as duplicates.
//
// Most causes are TRANSIENT system conditions, where abandoning the tail is what
// makes the batch safe to retry: publish backpressure (503), an unreachable
// broker (503, mq.ErrUnavailable), a publish/marshal failure (500), a dedupe
// store that cannot answer (503) or fails (500), an id another request holds
// (503), a type layer that cannot judge the tenant's table (503).
//
// Three are not, and retrying any of them unchanged cannot help:
//   - An insert grant that resolved for the other operation is a 403 and a
//     caller/config bug. It aborts rather than rejecting per record because the
//     grant is resolved ONCE per request, so it is true for every record or
//     none; as a per-record reject a 10k batch would report 10k independent
//     permission failures for a single mis-wired grant.
//   - A header-format body whose header ClickHouse refuses — a name the table
//     or the role lacks, or a name given twice — is a 400 with the
//     clickhouse.rejected class and ClickHouse's own code (117). The header is
//     not a record, and no record was read past it.
//   - A role whose projection of the table does not compile is a 500 marked
//     not retryable (see roleRefusedAbort).
type requestAbort struct {
	Status        int
	Message       string
	Code          string // the error class, when one applies (codeCHRejected)
	ExceptionCode int    // ClickHouse's code, when its parser refused the body as a whole
	RetryAfter    string // non-empty → emit a Retry-After header
	// Retryable, when set, overrides what a client reads off the status: the
	// SDK retries every 5xx unless the body says otherwise.
	Retryable *bool
}

// ingestRun is one request after its body has been ruled on: chtypes' verdict
// per record, its check answer included, and the wire columns the accepted rows
// were exported with. Both response shapes read their records out of it, so
// the single-object and batch paths cannot disagree about what a record's
// outcome is — only about how it is rendered.
type ingestRun struct {
	store        *settings.Store
	table, scope string
	now          time.Time
	batch        typelayer.Batch
	// wire is the role's wire columns, copied off the handle that exported the
	// rows: the envelope's Columns, and where the dedupe id sits in a row.
	wire []string
	// records is how many records the request contains: the body's own framing
	// before chtypes has read it (an empty array is zero, anything else is at
	// least one), then len(batch.Rows) once it has answered.
	records int
	// framed is a JSON array's element count — the one body whose framing
	// gives WaveHouse an exact record count before chtypes reads it — and 0
	// for every other body. chtypes answering any other number declines the
	// whole body (typelayer.IngestOptions.Records).
	framed int
	// checkColumns names the check clauses a record's check answer came from,
	// for the rejection message. The filter is AND-joined over all of them, so
	// a false verdict does not say which one failed — with one clause it does.
	checkColumns []string
	// checkGuard is the rejection every otherwise-acceptable record gets when
	// the role's check clauses name columns no record can carry.
	checkGuard *recordReject
}

func (h *IngestHandler) Handle(w http.ResponseWriter, r *http.Request) {
	store, ok := requestStore(w, r)
	if !ok {
		return
	}
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

	if table == "" {
		slog.ErrorContext(ctx, "missing table parameter in request")
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	// TODO: prevent table-enumeration...
	schema, err := lookupSchema(w, h.Registry, store, table, "unknown table: "+table)
	if err != nil {
		if errors.Is(err, discovery.ErrUnknownTable) {
			slog.WarnContext(ctx, "unknown table requested", "table", table)
		}
		return
	}

	// FAST AUTH: Table-level policy check (before spending CPU parsing the body).
	// The insert grant depends only on role+table+action, not on record
	// contents, so it is evaluated once for the whole request — including every
	// record of a batch.
	var perms *policy.ResolvedPermissions
	var role string

	if h.PolicySource != nil {
		p := h.PolicySource(store)
		role = policy.ResolveRole(p, auth.RoleFromContext(ctx))
		claims, _ := auth.ClaimsFromContext(ctx)
		perms = policy.Evaluate(p, role, table, "insert", claims)
		if !perms.Allowed {
			writeAuthzDenied(w, r, role, nil,
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
			slog.WarnContext(ctx, "conflicting ingest content-type declarations",
				"content_types", decls, "table", table)
		} else {
			// Logs the same bounded set the response shows, like the conflicting
			// branch: logging only Header.Get would make the server-side record
			// disagree with what the caller was told — for
			// ["", "text/csv"] the client sees `Content-Type "", "text/csv"`
			// while Header.Get would have logged only content_type="".
			slog.WarnContext(ctx, "ingest content-type not declared or not supported",
				"content_types", decls, "table", table)
		}
		writeJSONError(w, http.StatusUnsupportedMediaType, contentTypeMessage(decls, conflicting))
		return
	}

	// A tenant or table the type layer cannot judge is refused before the body
	// is read, like a schema not discovered yet: nothing in the body can change
	// the answer, so there is no reason to buffer up to 16 MiB of it. The handle
	// is not held across the read — a slow upload must not hold off a rebind.
	if abort := h.typesReady(ctx, store.Tenant(), table); abort != nil {
		writeAbort(w, abort)
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
	// way out; what is published is the type layer's own export of the records,
	// a separate buffer, so nothing downstream points into this one.
	body := getBodyBuffer()
	defer putBodyBuffer(body)
	if _, err := body.ReadFrom(r.Body); err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		slog.WarnContext(ctx, "ingest body read failed", "error", err, "table", table)
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Framing: the declared format says how the body frames its records, and the
	// first non-whitespace byte answers the one question left inside the JSON
	// family — array or single object. That byte is the ONLY thing the body gets
	// to decide, and it decides the response shape, never the format.
	first, ok := firstNonSpace(body.Bytes())
	if !ok {
		slog.ErrorContext(ctx, "empty ingest body", "table", table, "format", format.String())
		writeJSONError(w, http.StatusBadRequest, emptyBodyMessage(format))
		return
	}
	batchShape := format.alwaysBatch() || first == '['
	records := 1
	if format == FormatJSON && first == '[' {
		n, err := reframeArray(body.Bytes())
		if err != nil {
			// Brackets that do not balance — a truncated upload or a structural
			// syntax error — or a tail after the array. None can be salvaged per
			// record, and reporting what did frame as a complete batch is the
			// failure this refusal exists to prevent.
			slog.WarnContext(ctx, "ingest read error", "error", err.Error(), "table", table)
			writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		records = n
	}
	framed := 0
	if format == FormatJSON && first == '[' {
		framed = records
	}
	// Otherwise a single-object body is one record, and a line-framed body has
	// at least the record its first byte starts. Concatenated objects after a
	// single object are neither answered nor published, as they always have
	// been (declare NDJSON to batch them, #561), but chtypes still parses
	// them, so one cut off mid-record can turn the answer into a decline. The
	// real count is chtypes' own once it has answered — held to a floor the
	// type layer counts in the body itself, so a short answer is a decline,
	// never a short batch.

	guard := h.policyCheckGuard(ctx, table, role, schema, perms)
	shape, preds, checkColumns, abort := h.insertShape(ctx, table, role, schema, perms, guard)
	if abort != nil {
		writeAbort(w, abort)
		return
	}

	run := &ingestRun{
		store: store, table: table, scope: scope, now: now,
		records: records, framed: framed, checkColumns: checkColumns, checkGuard: guard,
	}
	if records > 0 {
		if abort := h.judge(ctx, run, shape, format, body.Bytes(), preds); abort != nil {
			writeAbort(w, abort)
			return
		}
	}

	if batchShape {
		h.writeBatch(ctx, w, run)
		return
	}
	h.writeSingle(ctx, w, run)
}

// typesReady answers whether the type layer can judge table for tenant id now,
// with the 503 every record of the request would get when it cannot.
func (h *IngestHandler) typesReady(ctx context.Context, id tenant.ID, table string) *requestAbort {
	if h.Types == nil {
		// Nothing else on this path inspects a value, so an unwired type layer
		// cannot mean "accept anything" — the same fail-closed direction the
		// stream's row evaluator takes.
		h.logUnavailable(ctx, id, table, "no type layer is wired")
		return unavailableAbort()
	}
	tbl, err := h.Types.Table(id, table)
	if err != nil {
		return h.typesFailed(ctx, id, table, err)
	}
	tbl.Release()
	return nil
}

// judge asks ClickHouse's own parser for a verdict per record — ONE call for
// the whole body, no chunking — with the role's insert check clauses, if any,
// compiled to ONE filter and answered by that same parse.
//
// The role's projection of the table answers column policy and the injected
// check values inside the parser, so no Go code walks the record's keys. It is
// held for the parse alone: the verdicts and exported rows are the run's own
// once the call returns, so dedupe and publish latency never hold off a rebind.
//
// A Go-level failure is an unavailable handle (the type layer's outage) or
// ours, and neither is the caller's record to fix. A body ClickHouse refused as
// a whole is the caller's to fix, and is a 400 with its code.
func (h *IngestHandler) judge(ctx context.Context, run *ingestRun, shape typelayer.RoleShape, format IngestFormat, body []byte, preds []policy.Predicate) *requestAbort {
	id := run.store.Tenant()
	tbl, abort := h.roleTable(ctx, id, run.table, shape)
	if abort != nil {
		return abort
	}
	opts := format.options()
	opts.Records = run.framed
	batch, err := tbl.IngestWith(format.wire(), opts, body, preds...)
	run.wire = slices.Clone(tbl.WireColumns)
	tbl.Release()
	if err != nil {
		if typelayer.IsUnavailable(err) {
			return h.typesFailed(ctx, id, run.table, err)
		}
		slog.ErrorContext(ctx, "record validation failed", "error", err, "table", run.table)
		return &requestAbort{Status: http.StatusInternalServerError, Message: "validation failed"}
	}
	if r := batch.Refused; r != nil {
		slog.WarnContext(ctx, "ingest body refused by the parser", "error", r.Message, "exception_code", r.Code, "table", run.table)
		return &requestAbort{Status: http.StatusBadRequest, Message: r.Message, Code: codeCHRejected, ExceptionCode: r.Code}
	}
	if m := batch.Miscount; m != nil {
		// Every record is answered declined (422) and nothing is published;
		// logged once here because the cause is the batch's, not any record's.
		slog.ErrorContext(ctx, "chtypes answered a different number of records than the body holds; declining the whole batch",
			"counted", m.Counted, "exact", m.Exact, "verdicts", m.Verdicts,
			"table", run.table, "format", format.String())
	}
	run.batch = batch
	// The type layer's answer is the record count: chtypes' own verdicts when
	// they account for every record in the body, or one declined verdict per
	// counted record when they do not or chtypes gave no per-record detail.
	// A JSON array sent as NDJSON is however many elements its reader took,
	// a blank NDJSON line is nothing.
	run.records = len(batch.Rows)
	return nil
}

// writeSingle answers a lone flat JSON object and preserves the GA response
// contract: 200 {"ok":true} (or {"duplicate":true} when dedup skips it), or the
// matching non-200 on refusal / permission / whole-request failure.
func (h *IngestHandler) writeSingle(ctx context.Context, w http.ResponseWriter, run *ingestRun) {
	rec, abort := h.prepareVerdict(ctx, run, 0)
	if abort == nil && rec.reject == nil {
		window := []pendingRecord{rec}
		abort = h.ingestWindow(ctx, run.store, run.table, run.scope, window)
		rec = window[0]
	}
	if abort != nil {
		writeAbort(w, abort)
		return
	}
	if rec.reject != nil {
		writeJSONErrorBody(w, rec.reject.Status, errorBody{Error: rec.reject.Message, ExceptionCode: rec.reject.ExceptionCode})
		return
	}
	if rec.duplicate {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"duplicate": true})
		return
	}

	slog.InfoContext(ctx, "event successfully ingested", "table", run.table)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// writeBatch answers a multi-record body (JSON array, NDJSON, CSV or TSV),
// running each verdict through the same prepare → reserve → publish → commit
// pipeline as a single insert, a window at a time. A record ClickHouse's
// parser refused, or one a check clause denied, is recorded against its index
// and the batch continues; a whole-request condition aborts it (see
// requestAbort). Returns 200 with a per-record summary.
func (h *IngestHandler) writeBatch(ctx context.Context, w http.ResponseWriter, run *ingestRun) {
	result := batchResult{Total: run.records, Results: []recordResult{}}
	size := h.window
	if size <= 0 {
		size = ingestWindow
	}
	window := make([]pendingRecord, 0, min(size, 16))
	// flush runs the window's records through reserve → publish → commit and
	// reports them in order; false when it aborted the request.
	flush := func() bool {
		if abort := h.ingestWindow(ctx, run.store, run.table, run.scope, window); abort != nil {
			writeAbort(w, abort)
			return false
		}
		for i := range window {
			result.add(&window[i])
		}
		window = window[:0]
		return true
	}

	for i := range run.records {
		rec, abort := h.prepareVerdict(ctx, run, i)
		if abort != nil {
			// Whole-request failure: surface the status rather than recording a
			// request-scoped condition as per-record loss (see requestAbort).
			// Nothing in the open window has been published.
			writeAbort(w, abort)
			return
		}
		rec.index = i + 1
		window = append(window, rec)
		if len(window) == size && !flush() {
			return
		}
	}
	if len(window) > 0 && !flush() {
		return
	}

	slog.InfoContext(ctx, "batch ingested", "table", run.table,
		"total", result.Total, "succeeded", result.Succeeded,
		"failed", result.Failed, "duplicates", result.Duplicates)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// add counts rec's outcome and records it up to maxReportedResults; the counts
// stay authoritative when Results is truncated. Total is the run's record
// count.
func (r *batchResult) add(rec *pendingRecord) {
	entry := recordResult{Index: rec.index}
	switch {
	case rec.reject != nil:
		r.Failed++
		entry.Error, entry.ExceptionCode = rec.reject.Message, rec.reject.ExceptionCode
	case rec.duplicate:
		r.Duplicates++
		entry.Duplicate = true
	default:
		r.Succeeded++
		entry.Ok = true
	}
	if len(r.Results) < maxReportedResults {
		r.Results = append(r.Results, entry)
	}
}

// insertShape turns the role's resolved insert grant into the projection of the
// table ClickHouse itself will enforce, plus the predicates the check clauses
// become.
//
// Columns is the allow/deny decision, answered by the compiled schema: a
// column the role may not write is one no INSERT may name, so a record naming
// it is refused per row with ClickHouse's own code 117 rather than by a Go walk
// over the record's keys. nil means the role may write every column, which
// compiles to no second handle at all.
//
// Defaults is the `_eq` auto-inject: the required value becomes the column's
// DEFAULT, so a record that omits it is filled and a record that supplies one
// still wins, and is then tested by the filter. An `_in` check has no single
// value to inject, so the column keeps the TABLE's own default and the filter
// tests that.
//
// An `_eq` column joins Columns even when the role may not otherwise write
// it: the check is what makes a value there legitimate — an absent one takes
// the claim, a supplied one passes only if it equals the claim — and the
// published row must carry it, or the server would store the column's own
// default instead of the value the policy requires.
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
	//
	// Reachable for every table: nothing rejects an empty record before this
	// point, because ClickHouse reads `{}` as every column taking its default.
	checks, resolved := perms.CheckClauses()
	if !resolved {
		// ABORT, not a per-record reject: perms is resolved once per request, so
		// this is true for every record or none. As a reject, a 10k-record batch
		// would emit 10k ERROR lines and report 10k independent permission
		// failures for one mis-wired grant.
		slog.ErrorContext(ctx, "insert checks consulted on a grant resolved for another operation",
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
			// nothing — Ingest fails every record's check without compiling
			// anything (#224).
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
			if shape.Columns != nil && !slices.Contains(shape.Columns, col) {
				shape.Columns = append(shape.Columns, col)
			}
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
// stays in the one place that owns it, the computed kinds included. typelayer
// declares every column whatever this list says — a denied one MATERIALIZED,
// so no record may name it and every expression over it still compiles — and
// reads the list for which columns a record may supply.
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
// declared type, so this is the only conversion the check path needs.
func scalarString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// roleTable resolves the compiled handle for this role's projection, or the
// abort every record of this request gets instead. The type layer being down
// is an outage, never a verdict about the data: a caller must be able to retry
// the same body unchanged (503). A projection that does not compile is a
// standing condition of the role's policy and the table's schema, not an
// outage (roleRefusedAbort).
//
// An injected literal the column cannot read (`count UInt64 DEFAULT 'abc'`) is a
// compile refusal, ClickHouse code 6 — measured. That must not refuse every
// insert for a policy that is simply unsatisfiable, so a refused shape is
// retried without its defaults: if that compiles, the defaults were the
// problem, and the check filter judges each record as sent, which fails closed
// (an absent column takes the table default and the filter refuses it). The
// type layer logs each refusal once per generation and shape; this adds one
// rate-limited line saying what was done about it.
func (h *IngestHandler) roleTable(ctx context.Context, id tenant.ID, table string, shape typelayer.RoleShape) (*typelayer.Table, *requestAbort) {
	if h.Types == nil {
		h.logUnavailable(ctx, id, table, "no type layer is wired")
		return nil, unavailableAbort()
	}
	tbl, err := h.Types.RoleTable(id, table, shape)
	if err == nil {
		return tbl, nil
	}
	if _, refused := errors.AsType[*typelayer.RoleRefused](err); refused && len(shape.Defaults) > 0 {
		bare := typelayer.RoleShape{Columns: shape.Columns}
		t, bareErr := h.Types.RoleTable(id, table, bare)
		if bareErr == nil {
			if h.noticeDue("inject:" + id.String() + "/" + table) {
				slog.WarnContext(ctx, "the role's schema does not compile with its insert check values as column defaults; "+
					"serving it without them, so records omitting those columns fail the check",
					"tenant", id, "table", table, "columns", slices.Sorted(maps.Keys(shape.Defaults)), "cause", err.Error())
			}
			return t, nil
		}
		err = bareErr // the refusal that stands without the defaults
	}
	if refused, ok := errors.AsType[*typelayer.RoleRefused](err); ok {
		if h.noticeDue("refused:" + id.String() + "/" + table) {
			slog.ErrorContext(ctx, "ingest refused: the role's insert permissions do not compile against this table",
				"tenant", id, "table", table, "cause", refused.Cause)
		}
		return nil, roleRefusedAbort()
	}
	return nil, h.typesFailed(ctx, id, table, err)
}

// roleRefusedAbort is the answer for a role whose projection of the table does
// not compile. It is a 500 marked not retryable rather than the 503 an outage
// gets, and carries no Retry-After: the refusal follows from the role's policy
// and the table's schema and is cached for the schema generation, so the same
// request fails the same way until an operator changes one of them, and a
// retry hint would only invite a client to hammer it. The body stays generic
// like the 503's — the cause names columns and ClickHouse internals, and is
// the operator's to read in the log.
func roleRefusedAbort() *requestAbort {
	retryable := false
	return &requestAbort{
		Status:    http.StatusInternalServerError,
		Message:   "this role's insert permissions cannot be enforced on this table",
		Retryable: &retryable,
	}
}

// typesFailed maps a type layer failure to the request's 503, logging its
// cause at most once a minute per tenant and table.
func (h *IngestHandler) typesFailed(ctx context.Context, id tenant.ID, table string, err error) *requestAbort {
	cause := err.Error()
	if un, ok := errors.AsType[*typelayer.Unavailable](err); ok {
		cause = un.Cause
	}
	h.logUnavailable(ctx, id, table, cause)
	return unavailableAbort()
}

// unavailableAbort is the 503 for a type layer that cannot judge the tenant's
// table: a tenant not bound yet, a missing artifact for its ClickHouse line, a
// server zone this process cannot serve, or a table that did not compile (or,
// a wiring fault, no type layer at all). The body stays generic — the cause can
// name server paths and other tenants' zones, and is the operator's to read in
// the log. Retry-After is the schema hint's: the tenant's next schema refresh
// is what binds it again, and what compiles a failed table again.
func unavailableAbort() *requestAbort {
	return &requestAbort{
		Status:     http.StatusServiceUnavailable,
		Message:    "ingest validation is unavailable",
		RetryAfter: retryAfterSchema,
	}
}

// logUnavailable emits at most one line per tenant and table per minute. A
// missing artifact, a timezone mismatch or a table that does not compile
// persists until an operator acts, so the per-request line says nothing the
// first one didn't.
func (h *IngestHandler) logUnavailable(ctx context.Context, id tenant.ID, table, cause string) {
	if h.noticeDue("unavailable:" + id.String() + "/" + table) {
		slog.ErrorContext(ctx, "ingest type layer unavailable", "tenant", id, "table", table, "cause", cause)
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

// writeAbort emits a whole-request failure response: the status, message and
// any error class and ClickHouse code, plus a Retry-After header when one is
// set. Shared by the single-object and batch paths.
func writeAbort(w http.ResponseWriter, abort *requestAbort) {
	if abort.RetryAfter != "" {
		w.Header().Set("Retry-After", abort.RetryAfter)
	}
	writeJSONErrorBody(w, abort.Status, errorBody{
		Error: abort.Message, Code: abort.Code, ExceptionCode: abort.ExceptionCode, Retryable: abort.Retryable,
	})
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
// without this guard a mis-wired policy would be a generic 500 (logged with a
// ClickHouse internal) rather than a 403 naming the column the operator has to
// fix.
//
// Evaluated here rather than per record because the condition is a property of
// (table, role, policy) and is identical for every record in the request. Doing
// it per record would emit one ERROR line per record for a single mis-wired
// policy, which on a 16 MiB body of small records is ~1.2M lines. The reject is
// still returned per record, so a batch reports each record's own cause: one
// that SUPPLIES the column is refused by ClickHouse first, with its own code.
func (h *IngestHandler) policyCheckGuard(
	ctx context.Context,
	table, role string,
	schema *discovery.TableSchema,
	perms *policy.ResolvedPermissions,
) *recordReject {
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
			slog.ErrorContext(ctx, "policy check references a column the table does not have",
				"column", col, "table", table, "role", role)
			reasons = append(reasons, fmt.Sprintf("%q, which table %q does not have", col, table))
		case !schemaCol.IsInsertable():
			slog.ErrorContext(ctx, "policy check references a column no record may write",
				"column", col, "table", table, "role", role, "default_kind", schemaCol.DefaultKind)
			reasons = append(reasons, fmt.Sprintf("%q of table %q, which is %s and cannot be inserted",
				col, table, strings.ToLower(schemaCol.DefaultKind)))
		case schemaCol.DefaultKind == "EPHEMERAL":
			// A record may supply it — its value feeds the DEFAULT columns over
			// it — but ClickHouse never stores it, the published row carries no
			// slot for it, and no query can read it back, so the constraint is
			// unverifiable the moment the insert returns. Accepting a check that
			// provably does nothing is worse than refusing it. An operator
			// wanting this should check the DEFAULT column derived from the
			// ephemeral one, which is stored and therefore enforceable.
			slog.ErrorContext(ctx, "policy check references an ephemeral column, which is never stored",
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

// pendingRecord is one record between prepareVerdict and its outcome.
type pendingRecord struct {
	index   int           // 1-based position in a batch
	reject  *recordReject // non-nil: the record is bad and is not published
	payload []byte        // the encoded envelope to publish
	// key is the record's dedupe identity, nil when it is published
	// un-deduped; retention is how long its id stays a duplicate once
	// committed; claim is Reserve's answer for it.
	key       *dedupe.Key
	retention time.Duration
	claim     dedupe.Claim
	duplicate bool
}

// prepareVerdict turns the i-th record's verdict into its pending outcome:
// ClickHouse's refusal or decline, the check guard, the check answer, then the
// dedupe id and the encoded envelope of the row ClickHouse's own writer
// produced. Reserving, publishing and committing happen per window, in
// ingestWindow.
//
// The order is the one the type layer imposes, and is documented: a record
// that both fails to parse and violates a check clause reports the PARSE
// error — chtypes answers the check only for a record it accepted. Nothing is
// published either way, so no enforcement is lost.
//
// Dedupe runs AFTER validation deliberately — the id must be claimed only for
// what is published, or a record ClickHouse refuses would burn its id and a
// corrected retry would be swallowed as a duplicate.
//
// A record the rest of a batch may proceed past comes back with reject set;
// abort non-nil is a whole-request failure the caller stops and returns.
func (h *IngestHandler) prepareVerdict(ctx context.Context, run *ingestRun, i int) (rec pendingRecord, abort *requestAbort) {
	verdict := verdictAt(run.batch, i)
	if !verdict.Accepted {
		logVerdict(ctx, run.table, verdict)
		return pendingRecord{reject: verdictReject(verdict)}, nil
	}
	if run.checkGuard != nil {
		return pendingRecord{reject: run.checkGuard}, nil
	}
	if verdict.CheckReason != "" {
		slog.WarnContext(ctx, "check clause failed", "columns", run.checkColumns,
			"reason", verdict.CheckReason, "cause", verdict.Message, "table", run.table)
		return pendingRecord{reject: checkReject(verdict.CheckReason, run.checkColumns)}, nil
	}

	// Optional deduplication. The dedupe settings resolve per record
	// from one snapshot (table override → global; the settings directory
	// states them all but dedupe.retention, whose absence means "0"), so a
	// reload lands at a record boundary. A Deduplicator without a settings source is
	// a wiring bug, not a mode — main wires both or neither. The id is claimed
	// in ingestWindow, once every record of the window is encoded, so nothing
	// but the publish can fail while the claim is held.
	if h.Dedup != nil && h.DedupeSettings != nil {
		if dd := h.DedupeSettings(run.store, run.table); dd.Enabled {
			idField := dd.IDField
			if id, ok := eventIDAt(verdict.Line, slices.Index(run.wire, idField)); ok {
				rec.key = &dedupe.Key{Table: run.table, ID: id}
				rec.retention = dd.Retention
			} else {
				dedupeMissingIDCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", run.table)))
				if dd.RequireID {
					slog.WarnContext(ctx, "dedupe id_field missing or null; rejecting", "id_field", idField, "table", run.table)
					return pendingRecord{reject: &recordReject{
						Status:  http.StatusBadRequest,
						Message: fmt.Sprintf("missing dedupe id field %q", idField),
					}}, nil
				}
				slog.WarnContext(ctx, "dedupe id_field missing or null; publishing without idempotency", "id_field", idField, "table", run.table)
			}
		}
	}

	// The row travels POSITIONALLY as the bytes ClickHouse's own writer
	// produced, with the column names alongside rather than in the row: a batch
	// of rows for one table carries the names once, and the reader can tell a
	// schema change mid-stream from a reordering. They are the ROLE's wire
	// columns, so a role that may not write every column publishes a shorter
	// row with a matching name list — the worker already groups a flush by
	// column signature, so that is one more group, not a new code path.
	evt := ingest.EventMessage{
		TableName:         run.table,
		Scope:             run.scope,
		ReceivedTimestamp: run.now.Format(time.RFC3339Nano),
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           run.wire,
		Row:               json.RawMessage(verdict.Line),
	}

	var err error
	rec.payload, err = json.Marshal(evt)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal event message", "error", err)
		return pendingRecord{}, &requestAbort{Status: http.StatusInternalServerError, Message: "marshal failed"}
	}
	return rec, nil
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
	return &recordReject{Status: http.StatusBadRequest, Message: v.Message, ExceptionCode: v.Code}
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
func logVerdict(ctx context.Context, table string, v typelayer.RowVerdict) {
	if v.Declined {
		slog.ErrorContext(ctx, "validation engine declined a record", "reason", v.Message, "table", table)
		return
	}
	slog.WarnContext(ctx, "schema validation failed", "error", v.Message, "exception_code", v.Code, "table", table)
}

// ingestWindow reserves, publishes and commits one window of prepared
// records, in three phases: one Reserve for every keyed record, the publishes
// in record order, one Commit for every claim published. Rejected and
// duplicate records are skipped. It sets each record's outcome and returns an
// abort when the request must stop; what the window published before a failure
// is committed first, so the retry reports it as duplicates (see publishFailed).
func (h *IngestHandler) ingestWindow(ctx context.Context, store *settings.Store, table, scope string, recs []pendingRecord) *requestAbort {
	var dd dedupe.Deduplicator
	var keyed []int
	for i := range recs {
		if recs[i].reject == nil && recs[i].key != nil {
			keyed = append(keyed, i)
		}
	}
	if len(keyed) > 0 {
		dd = h.Dedup(store)
		if abort := h.reserve(ctx, dd, table, recs, keyed); abort != nil {
			return abort
		}
	}

	topic := mq.Topic{Tenant: store.Tenant(), Table: table, Scope: scope}
	for i := range recs {
		rec := &recs[i]
		if rec.reject != nil || rec.duplicate {
			continue
		}
		var opts []mq.PublishOpt
		if rec.claim.Status == dedupe.Claimed {
			// The retry of an uncertain publish carries the same id, so the
			// queue drops its copy if the first one landed.
			opts = append(opts, mq.WithIdempotencyKey(dedupe.IdempotencyKey(store.Tenant(), rec.claim.Key)))
		}
		if err := h.Publisher.Publish(ctx, topic, rec.payload, opts...); err != nil {
			return h.publishFailed(ctx, dd, topic, recs, i, err)
		}
	}
	commitClaims(ctx, dd, recs, table)
	return nil
}

// lease is the dedupe lease in effect: h.DedupeLease when set, else
// dedupe.DefaultLease. Shared by reserve (Reserve's argument) and
// publishFailed (the Retry-After of a claim left to lapse), so both name the
// same window a client is told to wait out.
func (h *IngestHandler) lease() time.Duration {
	if h.DedupeLease > 0 {
		return h.DedupeLease
	}
	return dedupe.DefaultLease
}

// reserve claims the keys of recs[keyed] in one call and records each answer.
// A duplicate is skipped. A key another request holds releases the window's
// claims and aborts with 503 and the lease as Retry-After, since that
// request's outcome decides this one's. A store that cannot answer now is a
// 503 too; nothing in the window has been published. ErrDisabled — a reload
// switched the store off after the settings snapshot was read — publishes the
// window un-deduped, as records under the other setting would have been.
func (h *IngestHandler) reserve(ctx context.Context, dd dedupe.Deduplicator, table string, recs []pendingRecord, keyed []int) *requestAbort {
	lease := h.lease()
	keys := make([]dedupe.Key, len(keyed))
	for j, i := range keyed {
		keys[j] = *recs[i].key
	}
	claims, err := dd.Reserve(ctx, keys, lease)
	switch {
	case errors.Is(err, dedupe.ErrDisabled):
		// The counter carries the signal (a burst is a reload; a steady rate
		// is the store and settings out of step), so the line is Debug rather
		// than a WARN per record.
		dedupeDisabledCounter.Add(ctx, int64(len(keys)), metric.WithAttributes(attribute.String("table", table)))
		slog.DebugContext(ctx, "dedupe switched off mid-reload; publishing without idempotency", "records", len(keys), "table", table)
		return nil
	case errors.Is(err, dedupe.ErrUnavailable):
		slog.WarnContext(ctx, "dedupe store unavailable", "error", err, "table", table)
		return &requestAbort{Status: http.StatusServiceUnavailable, Message: "dedupe store unavailable", RetryAfter: "5"}
	case err != nil:
		if ctx.Err() != nil {
			// The request's own context ended — the client is gone, or its
			// deadline passed — while Reserve was in flight. Reserve wraps
			// that as an ordinary error, but it is not a backend problem
			// worth an operator's attention, and the response status below
			// is moot: nothing is listening for it.
			slog.DebugContext(ctx, "dedupe reserve failed: request context ended", "error", err, "table", table)
		} else {
			slog.ErrorContext(ctx, "dedupe reserve failed", "error", err, "table", table)
		}
		return &requestAbort{Status: http.StatusInternalServerError, Message: "dedupe failed"}
	}
	var held *dedupe.Key
	for j, i := range keyed {
		recs[i].claim = claims[j]
		switch claims[j].Status {
		case dedupe.Duplicate:
			recs[i].duplicate = true
			slog.InfoContext(ctx, "duplicate event skipped", "event_id", keys[j].ID, "table", table)
		case dedupe.InFlight:
			if held == nil {
				held = &keys[j]
			}
		case dedupe.Claimed:
		}
	}
	if held != nil {
		releaseClaims(ctx, dd, claimedIn(recs))
		slog.InfoContext(ctx, "event id in flight in another request", "event_id", held.ID, "table", table)
		return &requestAbort{
			Status:     http.StatusServiceUnavailable,
			Message:    "a request with the same dedupe id is in flight",
			RetryAfter: strconv.Itoa(int(math.Ceil(lease.Seconds()))),
		}
	}
	return nil
}

// publishFailed settles a window whose publish failed at recs[k] and returns
// the abort. The records before k are queued, so their ids are committed. A
// definite failure — ErrQueueFull, the broker refused the event — releases k's
// id and the rest, so the client's retry publishes them (#384). Any other
// failure may have stored the event before failing, so k's claim is left to
// lapse with its lease instead: a retry before then answers in-flight, and one
// after republishes under the same idempotency key, which the queue drops if
// the first copy landed. The records after k were never sent and are
// released.
//
// mq.ErrUnavailable — a broker blip, not a refusal — is one such uncertain
// failure, but still answers 503 rather than the plain 500 below: when k held
// a Claimed claim (left to lapse, as above), Retry-After is that lease
// rounded up to whole seconds, so an obedient client waits out the in-flight
// window instead of retrying straight into it and getting the 503 reserve
// already answers for that; when k was never keyed there is no lapse to wait
// out, so Retry-After is the flat 5 seconds main's per-record path used.
func (h *IngestHandler) publishFailed(ctx context.Context, dd dedupe.Deduplicator, topic mq.Topic, recs []pendingRecord, k int, err error) *requestAbort {
	definite := errors.Is(err, mq.ErrQueueFull)
	commitClaims(ctx, dd, recs[:k], topic.Table)
	after := k + 1
	if definite {
		after = k
	}
	releaseClaims(ctx, dd, claimedIn(recs[after:]))
	switch {
	case definite:
		slog.WarnContext(ctx, "ingest queue is full", "tenant", topic.Tenant, "error", err, "table", topic.Table, "scope", topic.Scope)
		return &requestAbort{Status: http.StatusServiceUnavailable, Message: "service unavailable", RetryAfter: "30"}
	case errors.Is(err, mq.ErrUnavailable):
		retryAfter := "5"
		if recs[k].claim.Status == dedupe.Claimed {
			retryAfter = strconv.Itoa(int(math.Ceil(h.lease().Seconds())))
		}
		slog.WarnContext(ctx, "ingest queue unavailable", "tenant", topic.Tenant, "error", err, "table", topic.Table, "scope", topic.Scope)
		return &requestAbort{Status: http.StatusServiceUnavailable, Message: "service unavailable", RetryAfter: retryAfter}
	}
	slog.ErrorContext(ctx, "failed to publish to the ingest queue", "tenant", topic.Tenant, "error", err, "table", topic.Table, "scope", topic.Scope)
	return &requestAbort{Status: http.StatusInternalServerError, Message: "publish failed"}
}

// claimedIn is the Claimed claims among recs.
func claimedIn(recs []pendingRecord) []dedupe.Claim {
	var out []dedupe.Claim
	for i := range recs {
		if recs[i].claim.Status == dedupe.Claimed {
			out = append(out, recs[i].claim)
		}
	}
	return out
}

// commitClaims makes the ids of recs' Claimed claims duplicates, one Commit
// per retention — one in practice, unless a reload changed it mid-window. A
// failure does not fail the records — they are in the queue — so it is logged
// and counted, and the claims lapse after their lease.
func commitClaims(ctx context.Context, dd dedupe.Deduplicator, recs []pendingRecord, table string) {
	var retentions []time.Duration
	byRetention := map[time.Duration][]dedupe.Claim{}
	for i := range recs {
		if recs[i].claim.Status != dedupe.Claimed {
			continue
		}
		r := recs[i].retention
		if _, ok := byRetention[r]; !ok {
			retentions = append(retentions, r)
		}
		byRetention[r] = append(byRetention[r], recs[i].claim)
	}
	for _, r := range retentions {
		claims := byRetention[r]
		// The records are queued whatever the request's context does next.
		err := dd.Commit(context.WithoutCancel(ctx), claims, r)
		switch {
		case err == nil, errors.Is(err, dedupe.ErrDisabled):
		default:
			dedupeCommitFailedCounter.Add(ctx, int64(len(claims)), metric.WithAttributes(attribute.String("table", table)))
			slog.ErrorContext(ctx, "dedupe commit failed after publish; the ids lapse with their lease", "error", err, "table", table, "records", len(claims))
		}
	}
}

// releaseClaims gives back claims whose records were not published. A failure
// is only logged: the claims lapse with their lease either way.
func releaseClaims(ctx context.Context, dd dedupe.Deduplicator, claims []dedupe.Claim) {
	if len(claims) == 0 {
		return
	}
	if err := dd.Release(context.WithoutCancel(ctx), claims); err != nil && !errors.Is(err, dedupe.ErrDisabled) {
		slog.WarnContext(ctx, "dedupe release failed; the ids lapse with their lease", "error", err)
	}
}
