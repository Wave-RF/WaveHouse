package typelayer

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/wave-rf/chtypes/go/v2/chtypes"
)

// InsertSettings are the parsing settings the ingest worker pins on the real
// INSERT, and which chtypes must therefore see — otherwise it answers a
// different question than the server will be asked. Returned fresh so a caller
// may add its own non-parsing pins (wait_for_async_insert=1) without mutating ours.
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
	// Now pins the clock (chtypes_now_epoch_nanos) for every window of the
	// body, so a now() DEFAULT is one instant across the request. The zero
	// time leaves each call its own clock.
	Now time.Time
	// Window bounds each parse call; the zero value is defaultLimits.
	Window Limits
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
	// Error recovery off at the call, the highest settings layer: a reader
	// that skips a bad record can resume inside the next one (see Ingest).
	settings["input_format_allow_errors_num"] = "0"
	settings["input_format_allow_errors_ratio"] = "0"
	// The table's own partition key is compiled, but the worker forms the
	// INSERT blocks and splits one the cap refuses (code 252), so a body
	// spanning many partitions is stored; a window must not refuse it.
	settings["max_partitions_per_insert_block"] = "0"
	settings["date_time_output_format"] = "iso"
	settings["output_format_json_escape_forward_slashes"] = "0"
	settings["output_format_json_quote_denormals"] = "1"
	// Every other chtypes format, including any it adds later, is refused.
	//exhaustive:ignore
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
	// Columns is the column list every exported Line carries its fields in:
	// chtypes' export_columns, which IngestWith checks equal to the table's
	// WireColumns. nil when Refused or Declined is set.
	Columns []string
	// Refused is ClickHouse's own refusal of the body, as its INSERT refuses
	// one: a value a column cannot read, a field the table lacks or the role
	// may not write, a WithNames header naming such a column or one twice, or
	// framing the format does not allow. nil otherwise.
	Refused *Refusal
	// Declined is why chtypes could not answer for the body at all — a shape
	// it does not support, an export it withheld — which is no verdict on the
	// data. "" otherwise.
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
// insert, and exports the accepted rows as JSONCompactEachRow. The body is
// read a window at a time, each window one call cut at a record the reader
// itself reported (see cursor), so a record's verdict, its number in a
// refusal and what is exported are the same as from one call over the body.
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
// the first record ClickHouse cannot read, in any window, refuses the whole
// body (Batch.Refused), and nothing is accepted. Recovery would hand back a verdict
// per record, but its reader resumes at the next line, which can be inside the
// same record or past the next one, so the verdicts stop matching the records.
// Per-record parse verdicts may return through Wave-RF/chtypes#497.
//
// Document flags stay lean (verdicts and exported bytes only), passed
// explicitly: chtypes' default, every group, costs 5.6× on this path and
// nothing here reads the per-value provenance it adds.
//
// On a role's view (Engine.RoleTable) the parse is the role's INSERT: only the
// columns it may write are listed, and its Defaults fill what a record omits.
//
// The returned error is for a Go-level failure only — every data verdict is in
// the batch — or, on a role's view, *RoleRefused for a Defaults value its
// column cannot read.
func (t *Table) Ingest(format Format, body []byte, checks ...Predicate) (Batch, error) {
	return t.IngestWith(format, IngestOptions{}, body, checks...)
}

// IngestWith is Ingest with options; see IngestOptions. The body is parsed a
// window at a time (see cursor) on this one held table; Engine.IngestRole is
// the same walk, releasing the table between windows and bounding how many
// parses run at once.
func (t *Table) IngestWith(format Format, opts IngestOptions, body []byte, checks ...Predicate) (Batch, error) {
	c, err := newCursor(format, opts, body)
	if err != nil {
		return Batch{}, err
	}
	var out Batch
	for !c.done {
		w, err := t.ingestNext(c, checks)
		if err != nil {
			return Batch{}, err
		}
		if out.merge(w) {
			break
		}
	}
	return out, nil
}

// merge appends window w's verdicts to b and reports whether the body is
// answered: a window ClickHouse refused or chtypes declined answers the whole
// body, with no verdict from any window.
func (b *Batch) merge(w Batch) bool {
	if w.Refused != nil || w.Declined != "" {
		*b = w
		return true
	}
	b.Rows = append(b.Rows, w.Rows...)
	if w.Columns != nil {
		b.Columns = w.Columns
	}
	return false
}

// parse is one chtypes call over win, with the role's column list and insert
// options and the check filter, which retry may drop (see retry).
func (t *Table) parse(c *cursor, win []byte, settings map[string]string, chk *checkState) (chtypes.BatchResult, error) {
	insert := t.insertOptions(c.format)
	res, err := t.export(c.format, win, settings, insert, chk.filter)
	if _, misuse := errors.AsType[*chtypes.UsageError](err); misuse && chk.filter != nil {
		// The cached filter was evicted and closed between lookup and use,
		// which chtypes refuses before reading the body. Fail the checks
		// closed, as an evaluation error would, not the request. Any other
		// error would come back the same from a second parse of the body.
		chk.filter, chk.uniform = nil, ReasonDecline
		res, err = t.export(c.format, win, settings, insert, nil)
	}
	if err != nil {
		// A view's other options were checked when it was built, so a call
		// chtypes refuses as misuse is its Defaults: a value its column cannot
		// read, refused before any byte of the body. That is the role's
		// standing condition, not this body's.
		if _, usage := errors.AsType[*chtypes.UsageError](err); usage && t.role != nil && len(t.role.defaults) > 0 {
			return res, &RoleRefused{Tenant: t.tenant, Table: t.Name, Cause: err.Error()}
		}
		return res, err
	}
	return res, nil
}

// checkState is the check filter one window parses with.
type checkState struct {
	filter  *chtypes.Filter
	uniform string
}

// batch maps one window's chtypes answer to its verdicts: the first keep rows
// of res, numbered after the base records earlier windows answered. A refusal
// or a decline is the whole body's answer, whichever window it came from.
func (t *Table) batch(c *cursor, res chtypes.BatchResult, chk *checkState, keep int) Batch {
	// Only a fully accepted batch exports bytes. Gate on the outcome, never on
	// RowsPassed or ExportDeclined: older artifact builds (every 26.6 build)
	// count the admitted rows of a rejected batch and leave ExportDeclined
	// empty when it rejects with no rows, and the outcome is right on both.
	if res.Outcome != chtypes.Accepted {
		// A refusal is about the data once the reader has started on the body
		// (a record, its framing) or a WithNames header. Otherwise the call
		// failed before reading any data, which is declined rather than blamed
		// on the body.
		if res.Outcome == chtypes.Rejected && res.ErrCode != 0 &&
			(len(res.Rows) > 0 || res.Framing != nil || c.format == FormatCSVWithNames || c.format == FormatTSVWithNames) {
			r := refusal(res)
			if r.Code == nestedSizesCode {
				r.Message = t.ownNested(r.Message)
			}
			if r.Record > 0 {
				r.Record += c.rows
			}
			r.Message = renumber(r.Message, c.rows)
			return Batch{Refused: r}
		}
		// The reader stops at the first record it cannot answer for, so the
		// rows it returned are not the body's: decline the body, not them.
		return Batch{Declined: batchCause(res)}
	}
	// Accepted but withheld (the full-arity guard, a serialization failure):
	// nothing can be forwarded, whatever the per-row detail says.
	if res.ExportDeclined != "" {
		return Batch{Declined: res.ExportDeclined}
	}
	// A setting the call asked for that the library declined, named at the
	// batch level, declines the body even when no row carries it.
	if msg := batchSettingsDecline(res); msg != "" {
		return Batch{Declined: msg}
	}
	if msg := skippedDecline(res); msg != "" {
		return Batch{Declined: msg}
	}
	// Every exported line carries the export's one column list, which the
	// envelope names: the table's wire columns, on a role's view too, where a
	// column the role may not write carries what the server would store.
	if len(res.Rows) > 0 && !slices.Equal(res.ExportColumns, t.WireColumns) {
		return Batch{Declined: fmt.Sprintf("chtypes exported the columns %q, not the table's wire columns %q",
			res.ExportColumns, t.WireColumns)}
	}
	out := Batch{Rows: make([]RowVerdict, keep), Columns: res.ExportColumns}
	for i := range out.Rows {
		v := rowVerdict(res.Rows[i], span(res, i), chk.filter != nil)
		if v.Accepted && chk.uniform != "" {
			v.Line, v.CheckReason = nil, chk.uniform
		}
		out.Rows[i] = v
	}
	return out
}

// batchCause is a batch's own cause when it was not accepted: its message, the
// reason's catalog id for a decline, its code and where it was refused.
// ExportDeclined is no cause here: for such a batch it only says that nothing
// was exported.
func batchCause(res chtypes.BatchResult) string {
	msg := res.ErrMsg
	if msg == "" {
		msg = "batch " + string(res.Outcome)
	}
	var detail []string
	if res.Reason != "" {
		detail = append(detail, "reason "+res.Reason)
	}
	if res.ErrCode != 0 {
		detail = append(detail, fmt.Sprintf("code %d", res.ErrCode))
	}
	switch {
	case res.RefusedAt == chtypes.RefusedAtRow && res.RefusedRow != nil:
		detail = append(detail, fmt.Sprintf("at record %d", *res.RefusedRow+1))
	case res.RefusedAt != "":
		detail = append(detail, "at "+string(res.RefusedAt))
	}
	if len(detail) > 0 {
		msg += " (" + strings.Join(detail, ", ") + ")"
	}
	return msg
}

// skippedDecline declines a batch whose reader skipped any row or byte while
// recovering from an error: its verdicts may then not be the body's records,
// and a skipped row's span can cover several of them. parseSettings pins
// recovery off on every call, so it is a guard, not a path.
func skippedDecline(res chtypes.BatchResult) string {
	if res.RowsSkipped > 0 || len(res.Unconsumed) > 0 {
		return "chtypes skipped part of the body recovering from an error, so its verdicts may not match the records"
	}
	return ""
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

// export is the one parse, with insert as its column list and insert options
// (insertOptions). It names no zone, so the body parses in the zone of the
// server the schema compiled on: a session_timezone that changed timezone()'s
// answer would be declined per row.
func (t *Table) export(format Format, body []byte, settings map[string]string, insert []chtypes.RowsOption, f *chtypes.Filter) (chtypes.BatchResult, error) {
	opts := append([]chtypes.RowsOption{
		chtypes.WithSettings(settings),
		chtypes.WithExport(chtypes.JSONCompactEachRow),
		chtypes.WithDocFlags(leanDocs),
	}, insert...)
	if f != nil {
		opts = append(opts, chtypes.WithRowFilter(f))
	}
	return t.c.schema.Rows(format, body, opts...)
}

// rowVerdict maps one chtypes RowResult: the outcome first, then the filter's
// verdict. A setting the library does not support declines the row through its
// Outcome (chtypes applies every promotion), and the batch's own list through
// batchSettingsDecline.
func rowVerdict(r chtypes.RowResult, line []byte, filtered bool) RowVerdict {
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

// batchSettingsDecline is the whole-body decline for settings the library
// refused at the batch level, "" when it names none.
func batchSettingsDecline(res chtypes.BatchResult) string {
	if len(res.UnsupportedSettings) == 0 {
		return ""
	}
	return "chtypes does not support setting(s) " + joinQuoted(res.UnsupportedSettings)
}

// refusal is a rejected batch's verdict, naming the first record the reader
// did not accept: the records before it parsed, so its index is the caller's.
func refusal(res chtypes.BatchResult) *Refusal {
	r := &Refusal{Code: int(res.ErrCode), Message: withoutDefinitionText(int(res.ErrCode), res.ErrMsg)}
	for i, row := range res.Rows {
		if row.Outcome != chtypes.Accepted {
			r.Record = i + 1
			break
		}
	}
	return r
}

// definitionMarker is how ClickHouse says an expression of the table's own
// definition (a DEFAULT, a key, partition or TTL expression, a CHECK over a
// conversion) was being evaluated or analyzed when it refused the record. Such
// a message can quote the definition anywhere, its head included (the value a
// conversion failed on, a constant, every DEFAULT of the table), so it is
// withheld whole and the client keeps its exception code and record. A
// record's own parse error carries no marker, unless the bytes it echoes do:
// that message is withheld too, which loses its locator and leaks nothing.
var definitionMarker = regexp.MustCompile(`(?i)while executing '|\bin scope\b`)

// readerCodes are the exception codes ClickHouse's input reader raises for a
// record's own bytes, measured over every column type and format on build
// 20261104.224808 and held, a case per code, by TestRefusal_KeepsTheReaderMessage
// on the build in use (26.8 build 20261105.095805). A refusal with any other code has its message withheld
// however it is worded: that is how the definition's evaluation refuses a
// record. 36, which the reader raises only for an AggregateFunction state, is
// withheld as the broad BAD_ARGUMENTS it is; 117 and 190 are kept only in the
// shapes readerShapes lists.
var readerCodes = map[int]bool{
	25: true, 26: true, 27: true, 32: true, 33: true, 38: true, 41: true, 69: true, 72: true,
	117: true, 130: true, 131: true, 190: true, 376: true, 407: true, 467: true, 563: true,
	632: true, 675: true, 676: true, 691: true,
}

// readerShapes narrows a reader code that a definition raises too, with no
// marker, to the messages measured to quote only the record's own bytes and
// the columns it named: a constant arrayMap of unequal arrays in a DEFAULT is
// 190 as a Nested group's unequal arrays are.
var readerShapes = map[int]*regexp.Regexp{
	incorrectDataCode: incorrectData,
	nestedSizesCode:   nestedSizes,
}

// nestedSizesCode is ClickHouse's SIZES_OF_ARRAYS_DONT_MATCH.
const nestedSizesCode = 190

// nestedSizes is 190 for a Nested group whose arrays differ in length. It
// names two of the group's columns, one of which can be a sibling the record
// did not write (a DEFAULT or MATERIALIZED one), so Table.ownNested keeps it
// only when the record could have written both.
var nestedSizes = regexp.MustCompile(`^Elements '(.+?)' and '(.+?)' of Nested data structure '.+?' \(Array columns\) have different array sizes\.$`)

// ownNested is a refusal's message, withheld when it is a Nested group's
// sizes naming a column a record of t may not write.
func (t *Table) ownNested(msg string) string {
	m := nestedSizes.FindStringSubmatch(msg)
	if m == nil {
		return msg
	}
	cols := t.inputs
	if cols == nil {
		cols = t.WireColumns
	}
	if slices.Contains(cols, m[1]) && slices.Contains(cols, m[2]) {
		return msg
	}
	return definitionWithheld
}

// checkHead is a CHECK violation: the sentence naming the constraint, its
// table (with the tenant's database) and the row, unless the expression came
// out NULL, then its quote of the expression and of the row's values. The
// constraint and the row are kept.
var checkHead = regexp.MustCompile(`(?s)^Constraint (.+?) for table .+? is violated( at row \d+)?\. Expression: `)

// incorrectData is the shapes of 117 (INCORRECT_DATA) the reader raises for
// a record's own bytes, quoting only those bytes and the names the record
// used; another shape, a Variant's whole type among them, is withheld.
var incorrectData = regexp.MustCompile(`^(?:Unknown field found |Duplicate field found |Expected end of line|JSON object should start with|JSON object/array should have equal number of|Unexpected symbol for key|Cannot insert data into JSON column|Cannot parse JSON object here|Cannot read JSON field here: |\s*You have carriage return|Incorrect data: )`)

// typeText is a reader quoting a column's type whole ("Cannot parse JSON
// value of type Variant(...) here"), which holds the strings the definition
// declares: Enum labels, Tuple element names.
var typeText = regexp.MustCompile(`value of type `)

// enumHint is the near-miss labels ClickHouse suggests for an unknown Enum
// element, which are the definition's. It runs to the last "]", so a label
// holding one is cut whole; at worst it takes the client's key name with it.
var enumHint = regexp.MustCompile(`(?s), maybe you meant: \[.*\]`)

// violatedConstraint is ClickHouse's VIOLATED_CONSTRAINT: a CHECK refused the
// record. A message of that code in a shape checkHead does not know is withheld.
const violatedConstraint = 469

// incorrectDataCode is ClickHouse's INCORRECT_DATA.
const incorrectDataCode = 117

// definitionWithheld replaces a refusal's message that can quote the definition,
// in the server's log line too: such a message can list every DEFAULT of the
// table, a keyed hash's key among them, and a WARN exports at 100%.
const definitionWithheld = "ClickHouse's message is withheld, since it can quote the table's definition"

// withoutDefinitionText is a refusal's message less what it quotes of the
// table's definition (Eric's choice, 2026-11-05): a reader's message is kept,
// a CHECK's is cut to the sentence naming it, and any other is withheld.
func withoutDefinitionText(code int, msg string) string {
	switch {
	case definitionMarker.MatchString(msg):
		return definitionWithheld
	case code == violatedConstraint:
		if m := checkHead.FindStringSubmatch(msg); m != nil {
			return "Constraint " + m[1] + " is violated" + m[2] + "."
		}
		return definitionWithheld
	case !readerCodes[code], typeText.MatchString(msg), readerShapes[code] != nil && !readerShapes[code].MatchString(msg):
		return definitionWithheld
	default:
		return enumHint.ReplaceAllString(msg, "")
	}
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
// newline the writer emits. Returns nil when the row contributed no bytes,
// including a row chtypes did not export (a nil span).
func span(res chtypes.BatchResult, i int) []byte {
	if i >= len(res.Spans) || res.Spans[i] == nil {
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
