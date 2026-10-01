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
	// Records is the caller's own exact count of the records in body, when its
	// framing gives one (a JSON array's elements), and 0 otherwise. chtypes
	// answering any other number declines the whole body; without it the type
	// layer counts a floor itself (see recordFloor).
	Records int
}

// parseSettings is what a body is parsed under: the insert pins, plus header
// detection off when the caller asked for strict positional CSV/TSV. The worker
// inserts JSONCompactEachRow, so the detect_header settings have no real-INSERT
// twin to keep in step. ok is false for a format Ingest does not parse.
//
// The export renders DateTime as RFC 3339 in UTC (`…T…Z`, the column's scale)
// and leaves `/` unescaped (`"/home"`, where the writer's default is
// `"\/home"`), the spellings the query paths pin too, so a published row spells
// a timestamp and a `/` as a queried one does, and carries its instant whatever
// the zone; the worker's best_effort INSERT stores that exact instant. Measured
// on 26.8.15.10.
func parseSettings(format Format, opts IngestOptions) (settings map[string]string, ok bool) {
	settings = InsertSettings()
	settings["date_time_output_format"] = "iso"
	settings["output_format_json_escape_forward_slashes"] = "0"
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
	// 41 …) and 0 otherwise. Declined verdicts carry no code: chtypes did not
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
	// Answered is false the whole batch was declined — chtypes gave no
	// per-record detail, or answered fewer records than the body holds
	// (Miscount) — and Rows holds one declined verdict per record counted in
	// the body, so a caller still has something index-shaped to report.
	Rows     []RowVerdict
	Answered bool
	// Refused is ClickHouse's own refusal of a WithNames body as a whole — a
	// header naming a column the schema does not have, or naming one twice —
	// before any record was read. Rows is then empty; nil otherwise.
	Refused *Refusal
	// Miscount is set when chtypes answered a different number of records than
	// the body holds: fewer than the type layer's own floor, or other than the
	// caller's exact count (IngestOptions.Records). The batch is then declined
	// whole — Answered false, one declined verdict per counted record — so a
	// record chtypes never read cannot go unreported. nil otherwise.
	Miscount *Miscount
}

// Miscount is how far chtypes' answer fell from the body's own count.
type Miscount struct {
	Counted  int  // records in the body: the caller's exact count, or a floor
	Verdicts int  // verdicts chtypes returned
	Exact    bool // Counted is IngestOptions.Records rather than a floor
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
// The verdicts account for every record or for none: when chtypes answers
// fewer records than the body holds (recordFloor), the whole body is declined
// (Batch.Miscount) rather than returned short.
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
	var columns []string
	if format != FormatCSV && format != FormatTSV {
		columns = t.inputs // see inputColumns; positional formats map to WireColumns
	}
	filter, uniform := t.checkFilter(s, checks)
	res, err := export(s, format, body, settings, columns, filter)
	if err != nil && filter != nil {
		// The cached filter was evicted and closed between lookup and use. Fail
		// the checks closed, as an evaluation error would, not the request.
		filter, uniform = nil, ReasonDecline
		res, err = export(s, format, body, settings, columns, nil)
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
		n := declineCount(opts.Records, len(res.Rows), t.floor(s, format, opts, body), body)
		return declineAll(n, firstNonEmpty(res.ExportDeclined, res.ErrMsg, res.Outcome.String())), nil
	}
	// Accepted but withheld (the full-arity guard, a serialization failure):
	// nothing can be forwarded, whatever the per-row detail says.
	if res.ExportDeclined != "" {
		n := declineCount(opts.Records, len(res.Rows), t.floor(s, format, opts, body), body)
		return declineAll(n, res.ExportDeclined), nil
	}

	// chtypes answered per record, and its verdicts are index-aligned with the
	// records it read — which is every record only if it read as many as the
	// body holds. Fewer means its reader took some records with another (see
	// recordFloor), and reporting the short batch would drop them without a
	// word: decline the body whole instead. More is checked only against an
	// exact count, because a floor is no ceiling.
	if m := miscount(opts.Records, len(res.Rows), t.floor(s, format, opts, body)); m != nil {
		b := declineAll(m.Counted, m.message(res.Rows))
		b.Miscount = m
		return b, nil
	}
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

// export is the one parse, with columns as the INSERT column list (nil: none).
// With no filter RowsExportWith is RowsExport.
func export(s *schemaSlot, format Format, body []byte, settings map[string]string, columns []string, f *chtypes.LoadedFilter) (chtypes.BatchResult, error) {
	opts := make([]chtypes.RowsOption, 0, 2)
	if columns != nil {
		opts = append(opts, chtypes.WithColumns(columns))
	}
	if f != nil {
		opts = append(opts, chtypes.WithRowFilter(f))
	}
	return s.schema.RowsExportWith(format, body, settings, chtypes.JSONCompactEachRow, opts...)
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

// floor is recordFloor for body against this handle's columns: header
// auto-detection compares a first line with the wire columns and a second with
// their compiled types, and the first column's type decides whether a leading
// byte order mark is read as framing. Skipped when the caller has an exact
// count.
func (t *Table) floor(s *schemaSlot, format Format, opts IngestOptions, body []byte) int {
	if opts.Records > 0 {
		return 0
	}
	var types map[string]string
	if format == FormatCSV || format == FormatTSV {
		types = make(map[string]string, len(s.schema.Columns))
		for _, c := range s.schema.Columns {
			types[c.Name] = c.Type
		}
	}
	return recordFloor(format, opts, body, t.WireColumns, types)
}

// miscount compares chtypes' verdict count with the body's: exact (the
// caller's count, any difference) or a floor (fewer only). nil when they agree.
func miscount(exact, verdicts, floor int) *Miscount {
	switch {
	case exact > 0 && verdicts != exact:
		return &Miscount{Counted: exact, Verdicts: verdicts, Exact: true}
	case exact == 0 && verdicts < floor:
		return &Miscount{Counted: floor, Verdicts: verdicts}
	}
	return nil
}

// message is every declined record's error: the two counts, and the first
// record chtypes refused, which is where the records went missing — the
// verdicts before it are aligned with the body, so its index is the caller's.
func (m *Miscount) message(rows []chtypes.RowResult) string {
	holds := "at least "
	if m.Exact {
		holds = ""
	}
	msg := fmt.Sprintf("the body holds %s%d records but chtypes answered %d, so the batch is declined whole rather than reported short",
		holds, m.Counted, m.Verdicts)
	for i, r := range rows {
		if r.Outcome == chtypes.Rejected || r.Outcome == chtypes.Skipped {
			return msg + fmt.Sprintf("; record %d was refused (code %d: %s) and its reader may have taken the records after it", i+1, r.ErrCode, r.ErrMsg)
		}
	}
	return msg
}

// declineCount is how many records a whole-batch decline answers: the caller's
// exact count when it has one, else the larger of chtypes' own count and the
// floor — chtypes may have stopped short of the body's end — and the body's
// line count when neither saw a record.
func declineCount(exact, verdicts, floor int, body []byte) int {
	if exact > 0 {
		return exact
	}
	return countRecords(body, max(verdicts, floor))
}

func declineAll(n int, msg string) Batch {
	b := Batch{Rows: make([]RowVerdict, n)}
	for i := range b.Rows {
		b.Rows[i] = RowVerdict{Declined: true, Message: msg}
	}
	return b
}

// countRecords is the record count of a declined batch: known when anything
// counted a record, else the body's line count, so the caller still gets an
// index-shaped answer. A blank line, a pretty-printed object's inner lines, a
// CSV field holding a raw newline and a WithNames header each add a line that
// is no record, so the fallback can OVER-count, which produces extra declined
// verdicts — never an extra acceptance.
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
