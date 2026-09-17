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
	// Line is the record as ClickHouse's own JSONCompactEachRow writer
	// serialized it, without the trailing newline. nil unless Accepted.
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
	// Payload is the JSONCompactEachRow bytes chtypes exported for the
	// ACCEPTED records — one line each, in input order, and every accepted
	// RowVerdict.Line is a sub-slice of it. It is what CheckVerdicts is handed:
	// that call's verdicts are index-aligned with these lines, which is to say
	// with the accepted records, NOT with Rows. nil when nothing was exported.
	//
	// The bytes belong to chtypes and stay valid for as long as the Table is
	// held; a caller that outlives the request must copy them.
	Payload []byte
}

// Ingest asks ClickHouse's own parser whether each record in body would
// insert, in ONE call, and exports the accepted rows as JSONCompactEachRow.
//
// format is how body is spelled and must be FormatJSONEachRow (newline-
// separated, name-addressed — an NDJSON body is byte-identical to this),
// FormatCSV or FormatTSV (positional, declaration order, and with NO header
// line: a header is one skipped record with code 27). Any other format is a
// programming error and returns an error without touching the data — a binding
// must never declare a format it has not asked the artifact about.
//
// Document flags stay lean (verdicts and exported bytes only). The per-value
// provenance DocValues would give costs 2.65× on this path (AUDIT §A.1) and
// nothing here reads it.
//
// The returned error is for a Go-level failure only — every data verdict is in
// the batch.
func (t *Table) Ingest(format Format, body []byte) (Batch, error) {
	switch format {
	case FormatJSONEachRow, FormatCSV, FormatTSV:
	default:
		return Batch{}, fmt.Errorf(
			"typelayer: Ingest cannot parse format %d; use FormatJSONEachRow, FormatCSV or FormatTSV", int(format))
	}
	if len(t.slots) == 0 {
		return Batch{}, &Unavailable{Table: t.Name, Cause: t.cause}
	}
	res, err := t.slot().schema.RowsExport(format, body, InsertSettings(), chtypes.JSONCompactEachRow)
	if err != nil {
		return Batch{}, err
	}

	// No bytes means nothing can be published, whatever the per-row detail
	// says: decline the whole batch rather than accept rows we cannot forward.
	if res.ExportDeclined != "" {
		return declineAll(countRecords(body, len(res.Rows)), res.ExportDeclined), nil
	}
	if res.Outcome != chtypes.Accepted && len(res.Rows) == 0 {
		msg := res.ErrMsg
		if msg == "" {
			msg = res.Outcome.String()
		}
		return declineAll(countRecords(body, 0), msg), nil
	}

	// chtypes answered per record, so its count is the record count: the
	// verdicts are index-aligned with the records it read, and padding to the
	// body's newline count would invent declined records out of blank lines
	// and pretty-printed framing.
	out := Batch{Rows: make([]RowVerdict, len(res.Rows)), Payload: res.Payload, Answered: true}
	for i := range out.Rows {
		out.Rows[i] = rowVerdict(res.Rows[i], span(res, i))
	}
	return out, nil
}

// rowVerdict maps one chtypes RowResult. An unsupported setting is the engine
// declining even when the row itself parsed, so it is checked before the
// outcome.
func rowVerdict(r chtypes.RowResult, line []byte) RowVerdict {
	if len(r.UnsupportedSettings) > 0 {
		return RowVerdict{Declined: true, Message: "chtypes does not support setting(s) " + joinQuoted(r.UnsupportedSettings)}
	}
	switch r.Outcome {
	case chtypes.Accepted:
		if line == nil {
			return RowVerdict{Declined: true, Message: "accepted but no bytes were exported for this row"}
		}
		return RowVerdict{Accepted: true, Line: line}
	case chtypes.Skipped, chtypes.Rejected:
		return RowVerdict{Code: r.ErrCode, Message: r.ErrMsg}
	case chtypes.Unsupported, chtypes.AcceptedPoisoned:
		// AcceptedPoisoned holds a value no writer can honestly serialize, so
		// like Unsupported it yields no bytes and is not a data verdict.
		return declinedVerdict(r)
	default:
		return declinedVerdict(r)
	}
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
// value, so counting lines is exact for the bodies this package is handed; the
// same holds for the JSONCompactEachRow payload chtypes exports. A CSV field
// may legally contain a raw newline, so for that format the fallback can
// OVER-count, which produces extra declined verdicts — never an extra
// acceptance.
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
