package typelayer

import (
	"bytes"
	"fmt"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// InsertSettings are the parsing settings the ingest worker pins on the real
// INSERT, and which chtypes must therefore see — otherwise it answers a
// different question than the server will be asked. Returned fresh so a caller
// may add its own non-parsing pins (async_insert=0) without mutating ours.
func InsertSettings() map[string]string {
	return map[string]string{
		"date_time_input_format":       "best_effort",
		"input_format_null_as_default": "1",
	}
}

// IngestOptions adjusts how IngestWith reads a body. The zero value is
// ClickHouse's own behaviour.
type IngestOptions struct {
	// StrictPositional switches header auto-detection off for FormatCSV and
	// FormatTSV (input_format_csv_detect_header / input_format_tsv_detect_header
	// = 0), so every line is a record. By default ClickHouse consumes a first
	// line that names the columns as a header (both settings are on by default;
	// measured on the 26.6 and 26.8 servers and artifacts). Ignored for other
	// formats.
	StrictPositional bool
}

// parseSettings is what a body is parsed under: the insert pins, plus header
// detection off when the caller asked for strict positional CSV/TSV. The worker
// inserts JSONCompactEachRow, so the detect_header settings have no real-INSERT
// twin to keep in step. ok is false for a format Ingest does not parse.
//
// The export renders DateTime as RFC 3339 in UTC (`…T…Z`, the column's scale),
// the spelling the query paths pin too, so a published row reads the same as a
// queried one and carries its instant whatever the zone; the worker's
// best_effort INSERT stores that exact instant. Measured on 26.8.15.10.
func parseSettings(format Format, opts IngestOptions) (settings map[string]string, ok bool) {
	settings = InsertSettings()
	settings["date_time_output_format"] = "iso"
	switch format {
	case FormatJSONEachRow, FormatCSVWithNames, FormatTSVWithNames:
	case FormatCSV:
		if opts.StrictPositional {
			settings["input_format_csv_detect_header"] = "0"
		}
	case FormatTSV:
		if opts.StrictPositional {
			settings["input_format_tsv_detect_header"] = "0"
		}
	default:
		return nil, false
	}
	return settings, true
}

// RowVerdict is one input record's answer, index-aligned with the records the
// caller wrote into the body.
type RowVerdict struct {
	Accepted bool
	// Code is ClickHouse's own error code when the record was refused (27, 117,
	// 6 …) and 0 otherwise. Declined verdicts carry no code: chtypes did not
	// answer, so there is nothing to attribute to the data.
	Code    int
	Message string
	// Declined marks "the validation engine could not answer", never "the data
	// is bad". A caller must not turn it into a 400.
	Declined bool
	// CheckReason is why the insert checks given to Ingest did not admit an
	// accepted record: ReasonFilter (the data says no), ReasonError or
	// ReasonDecline (we could not tell). "" when they admitted it or there were
	// none. A record with a CheckReason has no Line — only the rows the filter
	// admits are exported — and Message may carry the predicate's own error.
	CheckReason string
	// Line is the record as ClickHouse's own JSONCompactEachRow writer
	// serialized it, without the trailing newline. nil unless Accepted with no
	// CheckReason. It is a sub-slice of the batch's exported bytes, so a caller
	// that outlives the request must copy it.
	Line []byte
}

// Batch holds one verdict per input record, in input order.
type Batch struct {
	// Rows holds one verdict per record chtypes read, in input order. When
	// Answered is false chtypes gave no per-record detail (the whole batch was
	// declined) and Rows is padded to the body's line count so a caller still
	// has something index-shaped to report.
	Rows     []RowVerdict
	Answered bool
	// Refused is ClickHouse's own refusal of a WithNames body as a whole — a
	// header naming a column the schema does not have, or naming one twice —
	// before any record was read. Rows is then empty; nil otherwise.
	Refused *Refusal
}

// Refusal is ClickHouse's verdict on a body rather than on any one record.
type Refusal struct {
	Code    int
	Message string
}

// Ingest asks ClickHouse's own parser whether each record in body would
// insert, in ONE call, and exports the accepted rows as JSONCompactEachRow.
//
// format is how body is spelled:
//
//   - FormatJSONEachRow: newline-separated and name-addressed (an NDJSON body
//     is byte-identical to this);
//   - FormatCSV, FormatTSV: positional in declaration order. ClickHouse's
//     header auto-detection applies (a first line naming the columns is
//     consumed, not a record) unless IngestWith sets StrictPositional, where a
//     header line is one failed record;
//   - FormatCSVWithNames, FormatTSVWithNames: a first line naming the columns
//     in any order. It is not a record, so Rows index the data lines; a column
//     it omits takes its DEFAULT, and one it names that the schema lacks (or
//     names twice) refuses the whole body (Batch.Refused, code 117).
//
// Any other format is a programming error and returns an error without
// touching the data — a binding must never declare a format it has not asked
// the artifact about.
//
// checks are a role's insert check clauses. They compile to ONE filter,
// AND-joined, attached to the same parse (RowsExportWith; the chtypes SDK's
// filters guide, "Exporting only the rows a filter admits"), so each record's
// parse outcome and check answer come from one read of the body and only the
// admitted records are exported. The parse outcome decides first: a record
// chtypes did not accept reports its own error whatever the filter says (it
// answers such a row 'd'). A predicate with no Values matches nothing and
// never reaches the compiler; a filter that will not compile declines every
// accepted record. The compiled filter is cached per (generation, expression,
// values) on the handle it runs against.
//
// Document flags stay lean (verdicts and exported bytes only). The per-value
// provenance DocValues would give costs 2.65× on this path and nothing here
// reads it.
//
// The returned error is for a Go-level failure only — every data verdict is in
// the batch.
func (t *Table) Ingest(format Format, body []byte, checks ...Predicate) (Batch, error) {
	return t.IngestWith(format, IngestOptions{}, body, checks...)
}

// IngestWith is Ingest with options; see IngestOptions.
func (t *Table) IngestWith(format Format, opts IngestOptions, body []byte, checks ...Predicate) (Batch, error) {
	settings, ok := parseSettings(format, opts)
	if !ok {
		return Batch{}, fmt.Errorf(
			"typelayer: Ingest cannot parse format %d; use FormatJSONEachRow, FormatCSV, FormatTSV, FormatCSVWithNames or FormatTSVWithNames",
			int(format))
	}
	if t.pool == nil {
		return Batch{}, &Unavailable{Tenant: t.tenant, Table: t.Name, Cause: t.cause}
	}
	// The filter must be compiled on the handle the parse runs on: one from
	// another handle rejects the whole call.
	s := t.pool.acquire()
	defer t.pool.release(s)
	filter, uniform := t.checkFilter(s, checks)
	res, err := export(s, format, body, settings, filter)
	if err != nil && filter != nil {
		// The cached filter was evicted and closed between lookup and use. Fail
		// the checks closed, as an evaluation error would, not the request.
		filter, uniform = nil, ReasonDecline
		res, err = export(s, format, body, settings, nil)
	}
	if err != nil {
		return Batch{}, err
	}

	// Only a fully accepted batch exports bytes. Gate on the outcome, never on
	// RowsPassed or ExportDeclined: older artifact builds (every 26.6 build)
	// count the admitted rows of a rejected batch and leave ExportDeclined
	// empty when it rejects with no rows, and the outcome is right on both.
	if res.Outcome != chtypes.Accepted {
		if len(res.Rows) == 0 && res.Outcome == chtypes.Rejected && res.ErrCode != 0 &&
			(format == FormatCSVWithNames || format == FormatTSVWithNames) {
			return Batch{Answered: true, Refused: &Refusal{Code: res.ErrCode, Message: res.ErrMsg}}, nil
		}
		return declineAll(countRecords(body, len(res.Rows)), firstNonEmpty(res.ExportDeclined, res.ErrMsg, res.Outcome.String())), nil
	}
	// Accepted but withheld (the full-arity guard, a serialization failure):
	// nothing can be forwarded, whatever the per-row detail says.
	if res.ExportDeclined != "" {
		return declineAll(countRecords(body, len(res.Rows)), res.ExportDeclined), nil
	}

	// chtypes answered per record, so its count is the record count: the
	// verdicts are index-aligned with the records it read, and padding to the
	// body's newline count would invent declined records out of blank lines
	// and pretty-printed framing.
	out := Batch{Rows: make([]RowVerdict, len(res.Rows)), Answered: true}
	for i := range out.Rows {
		v := rowVerdict(res.Rows[i], span(res, i), filter != nil)
		if v.Accepted && uniform != "" {
			v.Line, v.CheckReason = nil, uniform
		}
		out.Rows[i] = v
	}
	return out, nil
}

// checkFilter resolves the check clauses to the filter attached to the parse,
// or to the one answer every accepted record gets when no filter runs:
// ReasonFilter for a predicate render cannot express (it matches nothing, like
// the SQL path's `1 = 0`), ReasonDecline for one that will not compile.
func (t *Table) checkFilter(s *schemaSlot, checks []Predicate) (*chtypes.LoadedFilter, string) {
	if len(checks) == 0 {
		return nil, ""
	}
	expr, params, ok := t.render(checks)
	if !ok {
		return nil, ReasonFilter
	}
	if f := t.filterOn(s, expr, params); f != nil {
		return f, ""
	}
	return nil, ReasonDecline
}

// export is the one parse. With no filter RowsExportWith is RowsExport.
func export(s *schemaSlot, format Format, body []byte, settings map[string]string, f *chtypes.LoadedFilter) (chtypes.BatchResult, error) {
	if f == nil {
		return s.schema.RowsExportWith(format, body, settings, chtypes.JSONCompactEachRow)
	}
	return s.schema.RowsExportWith(format, body, settings, chtypes.JSONCompactEachRow, chtypes.WithRowFilter(f))
}

// rowVerdict maps one chtypes RowResult. An unsupported setting is the engine
// declining even when the row itself parsed, so it is checked before the
// outcome, and the outcome before the filter's verdict.
func rowVerdict(r chtypes.RowResult, line []byte, filtered bool) RowVerdict {
	if len(r.UnsupportedSettings) > 0 {
		return RowVerdict{Declined: true, Message: "chtypes does not support setting(s) " + joinQuoted(r.UnsupportedSettings)}
	}
	switch r.Outcome {
	case chtypes.Accepted:
		if filtered {
			// A nil verdict is a row the filter never answered: it must not pass.
			if r.Verdict == nil {
				return RowVerdict{Accepted: true, CheckReason: ReasonDecline}
			}
			if ok, reason := verdictBool(*r.Verdict); !ok {
				return RowVerdict{Accepted: true, CheckReason: reason, Message: r.VerdictErr}
			}
		}
		if line == nil {
			return RowVerdict{Declined: true, Message: "accepted but no bytes were exported for this row"}
		}
		return RowVerdict{Accepted: true, Line: line}
	case chtypes.Skipped, chtypes.Rejected:
		// ErrCode/ErrMsg, not VerdictCode/VerdictErr: chtypes answers such a row
		// 'd', and older artifact builds (every 26.6 build) leave the verdict's
		// own code and message empty, where ErrCode/ErrMsg are set on every build.
		return RowVerdict{Code: r.ErrCode, Message: r.ErrMsg}
	case chtypes.Unsupported, chtypes.AcceptedPoisoned:
		// AcceptedPoisoned holds a value no writer can honestly serialize, so
		// like Unsupported it yields no bytes and is not a data verdict.
		return declinedVerdict(r)
	default:
		return declinedVerdict(r)
	}
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

// declinedVerdict reports an outcome that is not a verdict about the data,
// keeping the engine's own wording when it supplied any.
func declinedVerdict(r chtypes.RowResult) RowVerdict {
	msg := r.ErrMsg
	if msg == "" {
		msg = r.Outcome.String()
	}
	return RowVerdict{Declined: true, Message: msg}
}

// span slices row i's exported line out of the payload, dropping the trailing
// newline the writer emits. Returns nil when the row contributed no bytes.
func span(res chtypes.BatchResult, i int) []byte {
	if i >= len(res.Spans) {
		return nil
	}
	s := res.Spans[i]
	if s.Len <= 0 || s.Off < 0 || s.Off+s.Len > len(res.Payload) {
		return nil
	}
	return bytes.TrimSuffix(res.Payload[s.Off:s.Off+s.Len], []byte("\n"))
}

func declineAll(n int, msg string) Batch {
	b := Batch{Rows: make([]RowVerdict, n)}
	for i := range b.Rows {
		b.Rows[i] = RowVerdict{Declined: true, Message: msg}
	}
	return b
}

// countRecords recovers the input record count when chtypes returned no
// per-row detail, so the caller still gets an index-aligned answer. JSONEachRow
// records are newline-separated and json.Marshal escapes any newline inside a
// value, so counting lines is exact for the bodies this package is handed. A
// CSV field may legally contain a raw newline, and a WithNames header is a
// line but not a record, so for those formats the fallback can OVER-count,
// which produces extra declined verdicts — never an extra acceptance.
func countRecords(body []byte, known int) int {
	if known > 0 {
		return known
	}
	n := bytes.Count(body, []byte("\n"))
	if len(body) > 0 && !bytes.HasSuffix(body, []byte("\n")) {
		n++
	}
	return n
}

func joinQuoted(names []string) string {
	var b bytes.Buffer
	for i, n := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('"')
		b.WriteString(n)
		b.WriteByte('"')
	}
	return b.String()
}
