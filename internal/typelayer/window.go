package typelayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wave-rf/chtypes/go/v2/chtypes"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// Limits bounds one parse call: the bytes it reads and the records a window
// is cut to hold. chtypes' memory during a call grows with the records it
// reads (about 18 KB each on a 20-column table, freed before it returns), and
// a call over 10 MiB on a table with a Map, Dynamic, JSON or Object column is
// declined (ClickHouse's parallel-parsing cut, min_chunk_bytes_for_parallel_parsing).
type Limits struct {
	Bytes   int
	Records int
}

// defaultLimits bound a call: roughly 80 ms and 80 MB on a 20-column table
// (measured on darwin-arm64); any byte bound far under 10 MiB would do.
var defaultLimits = Limits{Bytes: 4 << 20, Records: 4096}

// tailMargin is how far before a window's end a refusal must sit to be taken
// as the body's: a failure nearer the cut may be the cut's own, so the window
// is closed or grown instead. It also keeps the bytes ClickHouse quotes after
// the failure (up to 160) inside the window, so the message is the body's.
const tailMargin = 4 << 10

// atRow is a record number ClickHouse puts in its message, counted from the
// window's first record: its TSV reader's "(at row N)" and a CHECK's
// "violated at row N".
var atRow = regexp.MustCompile(`(\(at row |violated at row )(\d+)`)

// renumber rewrites msg's record numbers to count from the body's first record.
func renumber(msg string, base int) string {
	if base == 0 {
		return msg
	}
	return atRow.ReplaceAllStringFunc(msg, func(m string) string {
		sub := atRow.FindStringSubmatch(m)
		n, err := strconv.Atoi(sub[2])
		if err != nil {
			return m
		}
		return sub[1] + strconv.Itoa(n+base)
	})
}

// cursor walks one body through ingestNext a window at a time.
//
// A window's end is only a guess, made by counting records in Go: top-level
// JSON objects (string-aware), or line ends for CSV and TSV, which overcount a
// CSV record holding a quoted newline and so only shrink the window. Every
// window after the first starts where the reader itself said a record starts
// (its InputSpan), never at a byte the guess chose, and the records it keeps
// are only those the reader finished before that start:
//
//   - accepted, not the last window: the last record may be the cut's
//     (a TSV line cut short still parses), so it is dropped and read again;
//   - refused at a record past the second, near the cut: the window is read
//     again up to the start of the record before it, and the next window
//     begins there, so the refused record is read mid-window again;
//   - refused well before the cut: that refusal is the body's, numbered
//     across windows;
//   - fewer than two records, or refused at the first or second one near the
//     cut: the window grows until it is the rest of the body, which is
//     authoritative.
//
// Later windows repeat what the reader consumed before the first record: a
// WithNames header, a header bare CSV or TSV detected (with the first
// window's settings, so its names map the fields again), or the `[` of a JSON
// array, closed by a `]` of our own on every window but the last. A bare body
// with no header reads later windows with detection off, since only a body's
// first line can be its header.
//
// The table's own definition brings its CHECKs, judged per record, and its
// engine's insert steps over a block, judged per window: a window they refuse
// with every record read is read again without its last record, which the cut
// may have shortened, and a refusal of complete records refuses the body,
// never growing (see ingestNext).
// The partition cap is lifted at the parse (parseSettings), since the worker
// forms the INSERT blocks. A body chtypes reports no InputSpan for, or whose
// container is not the one guessed, is read in one call from where the
// cursor stands.
type cursor struct {
	format Format
	opts   IngestOptions
	body   []byte
	lim    Limits
	first  map[string]string // the first window's settings
	later  map[string]string // every later window's
	off    int               // where the next window starts in body
	rows   int               // records earlier windows answered
	array  bool              // a JSONEachRow body that is one JSON array
	prefix []byte            // repeated before every later window
	grow   int
	whole  bool
	done   bool
}

// newCursor validates format and opts and builds the walk over body.
func newCursor(format Format, opts IngestOptions, body []byte) (*cursor, error) {
	settings, ok := parseSettings(format, opts)
	if !ok {
		return nil, fmt.Errorf(
			"typelayer: Ingest cannot parse format %d; use FormatJSONEachRow, FormatCSV, FormatTSV, FormatCSVWithNames or FormatTSVWithNames",
			int(format))
	}
	c := &cursor{format: format, opts: opts, body: body, lim: defaultLimits, grow: 1}
	if opts.Window != (Limits{}) {
		c.lim = opts.Window
	}
	c.lim.Records = max(c.lim.Records, 2)
	c.lim.Bytes = max(c.lim.Bytes, 1)
	if !opts.Now.IsZero() {
		// A 19-digit nanosecond epoch, as a string: a double cannot hold it.
		settings["chtypes_now_epoch_nanos"] = strconv.FormatInt(opts.Now.UnixNano(), 10)
	}
	c.first = settings
	c.later = settings
	// Only these two read a first line as a header, and only a JSON body can be an array.
	//exhaustive:ignore
	switch format {
	case FormatCSV, FormatTSV:
		c.later = make(map[string]string, len(settings)+1)
		for k, v := range settings {
			c.later[k] = v
		}
		key := "input_format_csv_detect_header"
		if format == FormatTSV {
			key = "input_format_tsv_detect_header"
		}
		c.later[key] = "0"
	case FormatJSONEachRow:
		c.array = startsArray(body)
	}
	return c, nil
}

// startsArray reports whether the first byte past a UTF-8 BOM and whitespace
// is `[`; chtypes' own Framing confirms it after the first window.
func startsArray(b []byte) bool {
	b = bytes.TrimPrefix(b, []byte("\xEF\xBB\xBF"))
	for _, x := range b {
		switch x {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			continue
		}
		return x == '['
	}
	return false
}

// next is the end of the next window in body, and whether it is the last.
func (c *cursor) next() (int, bool) {
	if c.whole {
		return len(c.body), true
	}
	limit := min(len(c.body), c.off+max(c.lim.Bytes*c.grow-len(c.prefix), 1))
	end := c.scan(limit, c.lim.Records*c.grow)
	return end, end >= len(c.body)
}

// scan counts records from c.off and returns the offset just past the n-th,
// or, at limit, past the last record end it saw when it saw two, else limit.
func (c *cursor) scan(limit, n int) int {
	b, seen, last := c.body, 0, -1
	if c.format == FormatJSONEachRow {
		base, depth := 0, 0
		if c.array {
			base = 1
			if c.off > 0 {
				depth = 1
			}
		}
		inStr, esc := false, false
		for i := c.off; i < limit; i++ {
			x := b[i]
			switch {
			case esc:
				esc = false
			case inStr && x == '\\':
				esc = true
			case x == '"':
				inStr = !inStr
			case inStr:
			case x == '{' || x == '[':
				depth++
			case x == '}' || x == ']':
				depth--
				if x == '}' && depth == base {
					seen, last = seen+1, i+1
					if seen == n {
						return last
					}
				}
			}
		}
	} else {
		for i := c.off; i < limit; i++ {
			if b[i] == '\n' || b[i] == '\r' {
				seen, last = seen+1, i+1
				if seen == n {
					return last
				}
			}
		}
	}
	if limit < len(b) && seen >= 2 {
		return last
	}
	return limit
}

// window is the bytes one call reads for body[c.off:end], with how many
// bytes of our own lead them.
func (c *cursor) window(end int, final bool) (win []byte, lead int) {
	var suffix []byte
	if c.array && !final {
		suffix = []byte("]")
	}
	if c.off == 0 && len(suffix) == 0 {
		return c.body[:end], 0
	}
	win = make([]byte, 0, len(c.prefix)+end-c.off+len(suffix))
	win = append(append(append(win, c.prefix...), c.body[c.off:end]...), suffix...)
	return win, len(c.prefix)
}

// at is the body offset of offset x in a window led by lead bytes of ours.
func (c *cursor) at(x uint64, lead int) int { return c.off + offset(x) - lead }

// offset is a span's field as an int. A window is at most the 16 MiB body
// plus a few bytes, so a larger value is no offset in it: it saturates, and
// the cut that reads it lands past the window and is refused.
func offset(x uint64) int {
	if x > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(x)
}

// advance moves the cursor past kept records of res, to body offset next.
// desc is the compiled table's columns, which type a detected header.
func (c *cursor) advance(res chtypes.BatchResult, lead, kept, next int, desc []chtypes.Column) {
	if c.off == 0 {
		head := c.body[:c.at(res.Rows[0].InputSpan.Off, lead)]
		switch {
		case c.array:
			c.prefix = []byte("[")
		case c.format == FormatCSVWithNames || c.format == FormatTSVWithNames:
			c.prefix = head
		case c.format == FormatCSV || c.format == FormatTSV:
			if p := detectedHeader(c.format, head, desc); p != nil {
				c.prefix, c.later = p, c.first
			}
		}
	}
	c.off, c.rows, c.grow = next, c.rows+kept, 1
}

// detectedHeader is the prefix that makes a later window read the header the
// reader detected in head, the bytes before a bare body's first record; nil
// when head holds no header. A names-only header gets a types row after it:
// detection reads the row after a names row as types when every field is a
// type name, which a later window's first record may be (a String column
// holding "Date"), but never a third row.
func detectedHeader(format Format, head []byte, desc []chtypes.Column) []byte {
	rows, eol := headerRows(format, bytes.TrimPrefix(head, []byte("\xEF\xBB\xBF")))
	switch len(rows) {
	case 0:
		return nil
	case 1:
	default:
		return head
	}
	types := make(map[string]string, len(desc))
	for _, col := range desc {
		types[col.Name] = col.Type
	}
	sep, out := ",", slices.Clone(head)
	if format == FormatTSV {
		sep = "\t"
	}
	for i, name := range rows[0] {
		typ, ok := types[name]
		if !ok {
			// Not a name we can type: the header alone, as the first window read it.
			return head
		}
		if i > 0 {
			out = append(out, sep...)
		}
		if format == FormatTSV {
			out = append(out, tsvField.Replace(typ)...)
		} else {
			out = append(out, `"`+strings.ReplaceAll(typ, `"`, `""`)+`"`...)
		}
	}
	return append(out, eol...)
}

// headerRows splits a detected header into its non-empty rows of fields, as
// the reader reads them (CSV: `"` quoting with `""`, unquoted fields trimmed
// of spaces and tabs, a line ending at LF, CRLF or CR; TSV: backslash
// escapes, a line ending at LF), and the line end its last row had.
func headerRows(format Format, b []byte) (rows [][]string, eol string) {
	var row []string
	var field []byte
	quoted, inQuote := false, false
	end := func(term string) {
		f := string(field)
		if format == FormatCSV && !quoted {
			f = strings.Trim(f, " \t")
		}
		row = append(row, f)
		if term != "" {
			if len(row) > 1 || row[0] != "" || quoted {
				rows, eol = append(rows, row), term
			}
			row = nil
		}
		field, quoted = field[:0], false
	}
	for i := 0; i < len(b); i++ {
		x := b[i]
		switch {
		case inQuote:
			switch {
			case x == '"' && i+1 < len(b) && b[i+1] == '"':
				field = append(field, '"')
				i++
			case x == '"':
				inQuote = false
			default:
				field = append(field, x)
			}
		case format == FormatCSV && x == '"' && len(bytes.Trim(field, " \t")) == 0:
			field, inQuote, quoted = field[:0], true, true
		case format == FormatTSV && x == '\\' && i+1 < len(b):
			i++
			field = append(field, tsvUnescape(b[i])...)
		case x == ',' && format == FormatCSV, x == '\t' && format == FormatTSV:
			end("")
		case format == FormatCSV && x == '\r' && i+1 < len(b) && b[i+1] == '\n':
			i++
			end("\r\n")
		case x == '\n', format == FormatCSV && x == '\r':
			end(string(x))
		default:
			field = append(field, x)
		}
	}
	if len(field) > 0 || len(row) > 0 {
		end("\n")
	}
	return rows, eol
}

// tsvUnescape is the byte a TSV `\x` escape stands for.
func tsvUnescape(x byte) string {
	switch x {
	case 't':
		return "\t"
	case 'n':
		return "\n"
	case 'r':
		return "\r"
	case '0':
		return "\x00"
	case 'b':
		return "\b"
	case 'f':
		return "\f"
	}
	return string(x)
}

// spansKnown reports whether every row of res carries its InputSpan.
func spansKnown(res chtypes.BatchResult) bool {
	for _, r := range res.Rows {
		if r.InputSpan == nil {
			return false
		}
	}
	return true
}

// firstOpen is the index of the first row res did not accept, len(res.Rows)
// when it accepted every one.
func firstOpen(res chtypes.BatchResult) int {
	for i, r := range res.Rows {
		if r.Outcome != chtypes.Accepted {
			return i
		}
	}
	return len(res.Rows)
}

// ingestNext parses the next window of c on t and returns its verdicts,
// advancing c. A refusal or decline is the body's answer and ends the walk.
func (t *Table) ingestNext(c *cursor, checks []Predicate) (Batch, error) {
	if t.c == nil {
		return Batch{}, &Unavailable{Tenant: t.tenant, Table: t.Name, Cause: t.cause}
	}
	chk := &checkState{}
	chk.filter, chk.uniform = t.checkFilter(checks)
	for {
		end, final := c.next()
		win, lead := c.window(end, final)
		settings := c.first
		if c.off > 0 {
			settings = c.later
		}
		res, err := t.parse(c, win, settings, chk)
		if err != nil {
			return Batch{}, err
		}
		if !final && (!spansKnown(res) || (c.off == 0 && c.format == FormatJSONEachRow && res.Framing != nil &&
			(res.Framing.Container == "array") != c.array)) {
			c.whole = true
			continue
		}
		if final {
			c.done = true
			return t.batch(c, res, chk, len(res.Rows)), nil
		}
		content := len(win)
		if c.array {
			content--
		}
		accepted, n, j := res.Outcome == chtypes.Accepted, len(res.Rows), firstOpen(res)
		switch {
		case accepted && n >= 2:
			next := c.at(res.Rows[n-1].InputSpan.Off, lead)
			if next <= c.off || next > end {
				c.whole = true
				continue
			}
			b := t.batch(c, res, chk, n-1)
			if b.Refused != nil || b.Declined != "" {
				c.done = true
				return b, nil
			}
			c.advance(res, lead, n-1, next, t.c.desc)
			return b, nil
		case !accepted && j < n && offset(res.Rows[j].InputSpan.Off)+offset(res.Rows[j].InputSpan.Len)+tailMargin <= content:
			// Refused well before the cut: the cut cannot be why.
			c.done = true
			return t.batch(c, res, chk, n), nil
		case !accepted && j >= 2 && j < n:
			// The next window starts at the last record accepted, never at the
			// refused one: a window's start reads a UTF-8 BOM or a `[` that
			// one call refuses mid-body.
			return t.closeAt(c, res, lead, j-1, chk)
		case !accepted && j == n && n >= 2:
			// Refused with every record read: an insert step over the block,
			// such as a merge engine's check (a CollapsingMergeTree sign) or a
			// partition key over a value, not a record. The last record may be
			// the cut's, shortened (a CSV or TSV line cut at the byte limit
			// still parses), so the window is read again without it; a refusal
			// of complete records is the body's. Never grown: a bigger window
			// holds the same rows and more, up to the whole body in one call.
			return t.closeAt(c, res, lead, n-1, chk)
		}
		// Too few records to cut at: grow the window until it holds two, or
		// is the rest of the body. Each step doubles both bounds.
		c.grow *= 2
	}
}

// closeAt reads the window again up to record j's start, every record in it
// finished, and starts the next window at record j, one the window accepted.
func (t *Table) closeAt(c *cursor, res chtypes.BatchResult, lead, j int, chk *checkState) (Batch, error) {
	next := c.at(res.Rows[j].InputSpan.Off, lead)
	end := next
	var suffix []byte
	if c.array {
		// The `,` before record j cannot end an array; close it after j-1.
		prev := res.Rows[j-1].InputSpan
		end, suffix = c.at(prev.Off, lead)+offset(prev.Len), []byte("]")
	}
	if next <= c.off || end <= c.off || next > len(c.body) || end > next {
		c.done = true
		return Batch{Declined: fmt.Sprintf("chtypes placed record %d outside the window it read", c.rows+j+1)}, nil
	}
	win := make([]byte, 0, len(c.prefix)+end-c.off+len(suffix))
	if c.off > 0 {
		win = append(win, c.prefix...)
	}
	win = append(append(win, c.body[c.off:end]...), suffix...)
	settings := c.first
	if c.off > 0 {
		settings = c.later
	}
	closed, err := t.parse(c, win, settings, chk)
	if err != nil {
		return Batch{}, err
	}
	b := t.batch(c, closed, chk, len(closed.Rows))
	c.done = b.Refused != nil || b.Declined != ""
	if c.done {
		return b, nil
	}
	if len(closed.Rows) != j || !spansKnown(closed) {
		// The same bytes read twice must give the same records.
		c.done = true
		return Batch{Declined: fmt.Sprintf("chtypes read %d records up to record %d's start, where it had read %d",
			len(closed.Rows), c.rows+j+1, j)}, nil
	}
	c.advance(closed, lead, j, next, t.c.desc)
	return b, nil
}

// ErrBusy is Engine.IngestRole's answer when every parse slot is taken: the
// request should be retried shortly, as backpressure, not as an outage.
var ErrBusy = errors.New("typelayer: every ingest parse slot is in use")

// parseSlots is how many bodies Engine.IngestRole parses at once when
// Config.ParseSlots is 0: one per processor, at most eight. A parse is CPU in
// C, so more than one per processor only queues, and eight default windows
// in flight come to about 0.7 GB on a 20-column table.
func parseSlots() int { return min(runtime.GOMAXPROCS(0), 8) }

// slotWait is how long IngestRole waits for a parse slot before ErrBusy:
// about one default window's parse (some 80 ms measured), so a request that
// arrives while every slot is busy with a small body or a window's worth is
// served rather than refused, and one behind long bodies is refused quickly.
const slotWait = 100 * time.Millisecond

// TakeParseSlot takes one of the slots IngestRole holds for a body's walk,
// waiting up to slotWait for one to free: ErrBusy when none does, the
// context's error when it ends first. release frees it.
func (e *Engine) TakeParseSlot(ctx context.Context) (release func(), err error) {
	release = func() { <-e.slots }
	select {
	case e.slots <- struct{}{}:
		return release, nil
	default:
	}
	timer := time.NewTimer(slotWait)
	defer timer.Stop()
	select {
	case e.slots <- struct{}{}:
		return release, nil
	case <-timer.C:
		return nil, ErrBusy
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IngestRole parses body as the role's view of table (RoleTable), a window
// at a time, re-acquiring the view for each window and releasing it between
// them, so a rebind waits on one window rather than the whole body. Every
// window must come from the one compiled handle the first did: a rebind
// between two is the table's *Unavailable, retried by the caller.
//
// The body holds one of the engine's parse slots for its whole walk, so the
// memory all ingest parses hold together is bounded; with none free within
// slotWait it returns ErrBusy. A context that ends stops the wait for a slot
// and the walk between windows; with a slot free at once, the first window
// runs whatever its state, as one call did.
func (e *Engine) IngestRole(ctx context.Context, id tenant.ID, table string, shape RoleShape, format Format, opts IngestOptions, body []byte, checks ...Predicate) (Batch, error) {
	c, err := newCursor(format, opts, body)
	if err != nil {
		return Batch{}, err
	}
	release, err := e.TakeParseSlot(ctx)
	if err != nil {
		return Batch{}, err
	}
	defer release()

	var out Batch
	var handle *compiled
	for !c.done {
		if err := ctx.Err(); err != nil && handle != nil {
			return Batch{}, err
		}
		tbl, err := e.RoleTable(id, table, shape)
		if err != nil {
			return Batch{}, err
		}
		if handle == nil {
			handle = tbl.c
		} else if tbl.c != handle {
			tbl.Release()
			return Batch{}, &Unavailable{Tenant: id, Table: table, Cause: "the table was recompiled while the request's body was being read"}
		}
		w, err := tbl.ingestNext(c, checks)
		tbl.Release()
		if err != nil {
			return Batch{}, err
		}
		if out.merge(w) {
			break
		}
	}
	return out, nil
}
