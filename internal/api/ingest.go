package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	// maxReportedResults caps how many per-record entries are echoed back in a
	// batch response. The four counts (total/succeeded/failed/duplicates) stay
	// authoritative; the results array is truncated so a very large batch can't
	// produce an unbounded response body. (The 16 MiB inbound body cap already
	// bounds input, but full per-record fidelity amplifies even a capped body,
	// so keep an explicit ceiling.)
	maxReportedResults = 10000

	// maxSniffBytes bounds how far the format sniffer peeks for the first
	// non-whitespace byte. Far beyond any reasonable amount of leading
	// whitespace; a body that is only whitespace within this window is treated
	// as empty.
	maxSniffBytes = 512

	// maxRequestBodyBytes (the inbound body cap) is shared with the admin query
	// handler — see internal/api/query.go.
)

// IngestHandler handles POST /v1/ingest?table={table}
type IngestHandler struct {
	Registry    *discovery.SchemaRegistry
	Dedup       dedupe.Deduplicator // nil if dedup disabled
	IDField     string              // dedup key field name (e.g. "event_id")
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

// recordReader yields ingest records one at a time from a request body. Each
// concrete reader covers one wire format (single JSON object, JSON array,
// NDJSON, and — later — CSV), so the handler stays format-agnostic and new
// formats / transports (streaming uploads) slot in behind this one interface.
//
// Next returns io.EOF at the clean end of the body. A *recordSyntaxError is a
// recoverable per-record decode failure — the framing let the reader resync, so
// the handler records it and continues. Any other non-EOF error is fatal to the
// request.
type recordReader interface {
	Next() (map[string]any, error)
}

// recordSyntaxError marks a per-record decode failure the reader recovered from
// (the framing let it skip to the next record). The batch handler turns it into
// a recordResult error; it carries no HTTP status because the decode layer sits
// below the validation/permission layer that owns status codes.
type recordSyntaxError struct{ msg string }

func (e *recordSyntaxError) Error() string { return e.msg }

// errEmptyBody is returned by newRecordReader when the body has no content (no
// non-whitespace byte within the sniff window). The handler maps it to a 400.
var errEmptyBody = errors.New("empty body")

// errUnterminatedArray marks a JSON array body that ended before its closing
// ']' (a truncated / cut-off upload). It is deliberately NOT io.EOF so the
// batch loop fails the whole request (400) instead of treating the records that
// did arrive as a complete, successful batch.
var errUnterminatedArray = errors.New("unterminated json array")

// objectReader decodes exactly one flat JSON object — the single-object ingest
// path. A second Next returns io.EOF. Trailing bytes after the first object are
// ignored (matching the historical single-object behavior), so the response
// shape never depends on what follows the object.
type objectReader struct {
	dec  *json.Decoder
	done bool
}

func (o *objectReader) Next() (map[string]any, error) {
	if o.done {
		return nil, io.EOF
	}
	o.done = true
	var m map[string]any
	if err := o.dec.Decode(&m); err != nil {
		return nil, err // fatal: handler maps MaxBytes → 413, else 400 invalid json
	}
	return m, nil
}

// arrayReader streams the elements of a top-level JSON array. A wrong-typed
// element (a scalar/array where an object was expected) yields a
// *json.UnmarshalTypeError, which leaves the decoder in sync — recoverable, so
// it becomes a per-record error and iteration continues. A *json.SyntaxError
// desyncs the decoder and is returned as fatal.
type arrayReader struct {
	dec     *json.Decoder
	started bool
	done    bool
}

func (a *arrayReader) Next() (map[string]any, error) {
	if a.done {
		return nil, io.EOF
	}
	if !a.started {
		if _, err := a.dec.Token(); err != nil { // consume the opening '['
			a.done = true
			return nil, err
		}
		a.started = true
	}
	if !a.dec.More() {
		a.done = true
		// More() reports false not only at a clean ']', but also on a read error
		// (the body cap tripping *between* elements) and on a truncated array
		// (EOF before ']'), swallowing both — which would let dropped records
		// masquerade as a complete partial-200 insert. Read the closing token to
		// tell the cases apart: only a ']' ends the batch. A read error
		// (MaxBytesError → 413, else 400) and a missing/non-']' close (the upload
		// was cut off → 400, via errUnterminatedArray which is NOT io.EOF) both
		// fail the whole request.
		tok, err := a.dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errUnterminatedArray
			}
			return nil, err
		}
		if d, ok := tok.(json.Delim); !ok || d != ']' {
			return nil, errUnterminatedArray
		}
		return nil, io.EOF
	}
	var m map[string]any
	if err := a.dec.Decode(&m); err != nil {
		if _, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
			// Decoder stayed in sync past the bad element — recoverable.
			return nil, &recordSyntaxError{"record must be a JSON object"}
		}
		// Syntax/read error: the decoder is desynced, the rest of the array is
		// unrecoverable. Don't try to read the closing ']'.
		a.done = true
		if errors.Is(err, io.EOF) {
			// More() said an element followed (e.g. after a trailing comma) but
			// the stream ended — a truncated array, not a clean close. Map to a
			// fatal error rather than the io.EOF the batch loop treats as "done".
			return nil, errUnterminatedArray
		}
		return nil, err
	}
	return m, nil
}

// lineReader yields one record per non-blank line of an NDJSON body. It recovers
// from both type and syntax errors per line (the newline reframes the next
// record), so a single malformed line never blocks the rest of the batch.
type lineReader struct {
	sc *bufio.Scanner
}

func (l *lineReader) Next() (map[string]any, error) {
	for l.sc.Scan() {
		line := bytes.TrimSpace(l.sc.Bytes())
		if len(line) == 0 {
			continue // skip blank lines between records
		}
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			return nil, &recordSyntaxError{"invalid json"}
		}
		return m, nil
	}
	if err := l.sc.Err(); err != nil {
		// A line exceeding maxNDJSONLineBytes (bufio.ErrTooLong) or a body read
		// error (possibly the MaxBytes cap) — the scanner can't resume, so fail
		// the request.
		return nil, err
	}
	return nil, io.EOF
}

// newRecordReader picks a reader from the Content-Type and a peek at the body.
// Content-Type is a hint, not a requirement: the first non-whitespace byte wins
// for the JSON family ('[' → array, else → single object), and an explicit
// application/x-ndjson type selects line-framing only when the body doesn't
// start with '[' (so a mislabeled JSON array still works). batch is false only
// for the single-object path; true for array/NDJSON. This is what makes ingest
// forgiving: a JSON array, a single object, or NDJSON all work regardless of the
// header.
func newRecordReader(contentType string, br *bufio.Reader) (rr recordReader, batch bool, err error) {
	first, perr := peekFirstNonSpace(br)
	if perr != nil {
		return nil, false, errEmptyBody
	}

	// Explicit NDJSON wins for line-framing, unless the body is actually an
	// array. (CSV plugs in here later: isCSVContentType(contentType) && first != '['.)
	if isNDJSONContentType(contentType) && first != '[' {
		sc := bufio.NewScanner(br)
		sc.Buffer(make([]byte, 0, 64*1024), maxNDJSONLineBytes)
		return &lineReader{sc: sc}, true, nil
	}

	dec := json.NewDecoder(br)
	dec.UseNumber()
	if first == '[' {
		return &arrayReader{dec: dec}, true, nil
	}
	return &objectReader{dec: dec}, false, nil
}

// peekFirstNonSpace returns the first non-whitespace byte of the body without
// consuming it (the chosen decoder still sees the whole body). It returns
// errEmptyBody when there is no such byte within the sniff window.
func peekFirstNonSpace(br *bufio.Reader) (byte, error) {
	buf, _ := br.Peek(maxSniffBytes) // short read at EOF is fine; we only need the first token
	for _, c := range buf {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c, nil
		}
	}
	return 0, errEmptyBody
}

// emptyBodyMessage tailors the empty-body 400 message to the declared format so
// an NDJSON caller still gets the familiar "empty ndjson body".
func emptyBodyMessage(contentType string) string {
	if isNDJSONContentType(contentType) {
		return "empty ndjson body"
	}
	return "empty body"
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
	br := bufio.NewReader(r.Body)

	// Pick a reader from the body shape (Content-Type is only a hint).
	rr, batch, err := newRecordReader(r.Header.Get("Content-Type"), br)
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
			if abort.RetryAfter != "" {
				w.Header().Set("Retry-After", abort.RetryAfter)
			}
			writeJSONError(w, abort.Status, abort.Message)
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

// isNDJSONContentType reports whether the request Content-Type explicitly
// selects NDJSON line-framing. It matches application/x-ndjson and common
// synonyms (application/ndjson, application/jsonl, application/jsonlines),
// ignoring parameters such as "; charset=utf-8". Anything else — including a
// missing type or application/json — leaves the format to the body sniffer.
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
