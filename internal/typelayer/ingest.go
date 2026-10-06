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
// The export renders DateTime as RFC 3339 in UTC (`…T…Z`, the column's scale)
// and leaves `/` unescaped (`"/home"`, where the writer's default is
// `"\/home"`), the spellings the query paths pin too, so a published row spells
// a timestamp and a `/` as a queried one does, and carries its instant whatever
// the zone; the worker's best_effort INSERT stores that exact instant. A Float
// NaN or infinity is a string ("nan", "inf", "-inf"), which the INSERT stores
// as that value: the export's default renders null, which would store the
// column's default. Measured on 26.8.15.10.
func parseSettings(format Format, opts IngestOptions) (settings map[string]string, ok bool) {
	settings = InsertSettings()
	settings["date_time_output_format"] = "iso"
	settings["output_format_json_escape_forward_slashes"] = "0"
	settings["output_format_json_quote_denormals"] = "1"
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
// caller wrote into the body. A record ClickHouse refuses is never one: it
// refuses the whole body (Batch.Refused).
type RowVerdict struct {
	Accepted bool
	Message  string
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

// Batch is a body's answer: a verdict per record, ClickHouse's refusal of the
// body, or chtypes declining to answer for it. At most one of Refused and
// Declined is set, and Rows is empty when either is.
type Batch struct {
	// Rows holds one verdict per record, in input order.
	Rows []RowVerdict
	// Refused is ClickHouse's own refusal of the body, as its INSERT refuses
	// one: a value a column cannot read, a field the table (or the role's
	// projection of it) lacks, a WithNames header naming such a column or one
	// twice, or framing the format does not allow. nil otherwise.
	Refused *Refusal
	// Declined is why chtypes could not answer for the body at all — a shape
	// it does not support, a session zone it cannot load, an export it
	// withheld — which is no verdict on the data. "" otherwise.
	Declined string
}

// Refusal is ClickHouse's verdict on a body as a whole.
type Refusal struct {
	Code    int
	Message string
	// Record is the 1-based record ClickHouse's reader failed on, header lines
	// not counted, and 0 when the failure is in no record (a WithNames header,
	// the framing after the last record).
	Record int
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
//     names twice) refuses the whole body (code 117).
//
// Any other format is a programming error and returns an error without
// touching the data — a binding must never declare a format it has not asked
// the artifact about.
//
// checks are a role's insert check clauses. They compile to ONE filter,
// AND-joined, attached to the same parse (Rows with WithRowFilter; the chtypes
// SDK's filters guide, "Exporting only the rows a filter admits"), so each record's
// parse outcome and check answer come from one read of the body and only the
// admitted records are exported. The checks answer only a body that parses:
// a record chtypes cannot read refuses the body whatever the filter says. A
// predicate with no Values matches nothing and never reaches the compiler; a
// filter that will not compile declines every accepted record. The compiled
// filter is cached per (generation, expression, values) on the table.
//
// The body is read as a ClickHouse INSERT reads it, with no error recovery:
// the first record ClickHouse cannot read refuses the whole body
// (Batch.Refused), and nothing is accepted. Recovery would hand back a verdict
// per record, but its reader resumes at the next line, which can be inside the
// same record or past the next one, so the verdicts stop matching the records.
// Per-record parse verdicts may return through Wave-RF/chtypes#497.
//
// Document flags stay lean (verdicts and exported bytes only), passed
// explicitly: chtypes' default, every group, costs 5.6× on this path and
// nothing here reads the per-value provenance it adds.
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
	if t.c == nil {
		return Batch{}, &Unavailable{Tenant: t.tenant, Table: t.Name, Cause: t.cause}
	}
	var columns []string
	if format != FormatCSV && format != FormatTSV {
		columns = t.inputs // see inputColumns; positional formats map to WireColumns
	}
	filter, uniform := t.checkFilter(checks)
	res, err := t.export(format, body, settings, columns, filter)
	if err != nil && filter != nil {
		// The cached filter was evicted and closed between lookup and use. Fail
		// the checks closed, as an evaluation error would, not the request.
		filter, uniform = nil, ReasonDecline
		res, err = t.export(format, body, settings, columns, nil)
	}
	if err != nil {
		return Batch{}, err
	}

	// Only a fully accepted batch exports bytes. Gate on the outcome, never on
	// RowsPassed or ExportDeclined: older artifact builds (every 26.6 build)
	// count the admitted rows of a rejected batch and leave ExportDeclined
	// empty when it rejects with no rows, and the outcome is right on both.
	if res.Outcome != chtypes.Accepted {
		// A refusal is about the data once the reader has started on the body
		// (a record, its framing) or a WithNames header. Otherwise the call
		// failed before reading any data (a session zone chtypes cannot load,
		// code 36), which is declined rather than blamed on the body.
		if res.Outcome == chtypes.Rejected && res.ErrCode != 0 &&
			(len(res.Rows) > 0 || res.Framing != nil || format == FormatCSVWithNames || format == FormatTSVWithNames) {
			return Batch{Refused: refusal(res)}, nil
		}
		// The reader stops at the first record it cannot answer for, so the
		// rows it returned are not the body's: decline the body, not them.
		return Batch{Declined: firstNonEmpty(res.ExportDeclined, res.ErrMsg, string(res.Outcome))}, nil
	}
	// Accepted but withheld (the full-arity guard, a serialization failure):
	// nothing can be forwarded, whatever the per-row detail says.
	if res.ExportDeclined != "" {
		return Batch{Declined: res.ExportDeclined}, nil
	}
	// The compile profile turns error recovery off, so an accepted batch never
	// skips bytes. If one does, its verdicts may not be the body's records.
	if len(res.Unconsumed) > 0 {
		return Batch{Declined: "chtypes skipped part of the body recovering from an error, so its verdicts may not match the records"}, nil
	}
	out := Batch{Rows: make([]RowVerdict, len(res.Rows))}
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
func (t *Table) checkFilter(checks []Predicate) (*chtypes.Filter, string) {
	if len(checks) == 0 {
		return nil, ""
	}
	expr, params, ok := t.render(checks)
	if !ok {
		return nil, ReasonFilter
	}
	if f := t.filterFor(expr, params); f != nil {
		return f, ""
	}
	return nil, ReasonDecline
}

// leanDocs asks a batch for its verdicts and exported bytes only.
const leanDocs = chtypes.DocFlags(0)

// export is the one parse, with columns as the INSERT column list (nil: none),
// in the table's zone (zoneOpts) — the zone f was compiled in.
func (t *Table) export(format Format, body []byte, settings map[string]string, columns []string, f *chtypes.Filter) (chtypes.BatchResult, error) {
	opts := []chtypes.RowsOption{
		chtypes.WithSettings(settings),
		chtypes.WithExport(chtypes.JSONCompactEachRow),
		chtypes.WithDocFlags(leanDocs),
	}
	for _, z := range t.zoneOpts() {
		opts = append(opts, z)
	}
	if columns != nil {
		opts = append(opts, chtypes.WithColumns(columns))
	}
	if f != nil {
		opts = append(opts, chtypes.WithRowFilter(f))
	}
	return t.c.schema.Rows(format, body, opts...)
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
	case chtypes.Unsupported, chtypes.AcceptedPoisoned:
		// AcceptedPoisoned holds a value no writer can honestly serialize, so
		// like Unsupported it yields no bytes and is not a data verdict.
		return declinedVerdict(r)
	case chtypes.Rejected, chtypes.Skipped:
		// Not in an accepted batch with recovery off: a refused row refuses the
		// batch. Should one appear, it is declined, never accepted.
		return declinedVerdict(r)
	default:
		return declinedVerdict(r)
	}
}

// refusal is a rejected batch's verdict, naming the first record the reader
// did not accept: the records before it parsed, so its index is the caller's.
func refusal(res chtypes.BatchResult) *Refusal {
	r := &Refusal{Code: int(res.ErrCode), Message: res.ErrMsg}
	for i, row := range res.Rows {
		if row.Outcome != chtypes.Accepted {
			r.Record = i + 1
			break
		}
	}
	return r
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
		msg = string(r.Outcome)
	}
	return RowVerdict{Declined: true, Message: msg}
}

// span slices row i's exported line out of the payload, dropping the trailing
// newline the writer emits. Returns nil when the row contributed no bytes.
func span(res chtypes.BatchResult, i int) []byte {
	if i >= len(res.Spans) {
		return nil
	}
	s, n := res.Spans[i], uint64(len(res.Payload))
	if s.Len == 0 || s.Off > n || s.Len > n-s.Off {
		return nil
	}
	return bytes.TrimSuffix(res.Payload[s.Off:s.Off+s.Len], []byte("\n"))
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
