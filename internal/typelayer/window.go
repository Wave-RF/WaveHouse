package typelayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"strconv"

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

// defaultLimits bound a lean call; valuesLimits one that asks for the
// per-value document, which costs about seven times as much per record. Both
// come to roughly 80 ms and 80 MB per call on a 20-column table (measured on
// darwin-arm64), and any byte bound far under 10 MiB would do.
var (
	defaultLimits = Limits{Bytes: 4 << 20, Records: 4096}
	valuesLimits  = Limits{Bytes: 4 << 20, Records: 512}
)

// tailMargin is how far before a window's end a refusal must sit to be taken
// as the body's: a failure nearer the cut may be the cut's own, so the window
// is closed or grown instead. It also keeps the bytes ClickHouse quotes after
// the failure (up to 160) inside the window, so the message is the body's.
const tailMargin = 4 << 10

// atRow is the record number ClickHouse's TSV reader puts in its message,
// counted from the window's first record.
var atRow = regexp.MustCompile(`\(at row (\d+)\)`)

// renumber rewrites msg's "(at row N)" to count from the body's first record.
func renumber(msg string, base int) string {
	if base == 0 {
		return msg
	}
	return atRow.ReplaceAllStringFunc(msg, func(m string) string {
		n, err := strconv.Atoi(atRow.FindStringSubmatch(m)[1])
		if err != nil {
			return m
		}
		return "(at row " + strconv.Itoa(n+base) + ")"
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
//   - refused at a record past the first, near the cut: the window is read
//     again up to that record's start, and the next window begins there;
//   - refused well before the cut: that refusal is the body's, numbered
//     across windows;
//   - fewer than two records, or refused at the first one near the cut: the
//     window grows until it is the rest of the body, which is authoritative.
//
// Later windows repeat what the reader consumed before the first record: a
// WithNames header, or the `[` of a JSON array, closed by a `]` of our own on
// every window but the last. They read bare CSV and TSV with header detection
// off, since only a body's first line can be its header.
//
// Whole-body steps of an INSERT (the partition cap, CHECKs over the merged
// block) then run once per window; the statement chtypes compiles has neither
// (#727). A body chtypes reports no InputSpan for, or whose container is not
// the one guessed, is read in one call from where the cursor stands.
type cursor struct {
	format Format
	opts   IngestOptions
	body   []byte
	lim    Limits
	docs   chtypes.DocFlags
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
	c := &cursor{format: format, opts: opts, body: body, docs: leanDocs, lim: defaultLimits, grow: 1}
	if opts.Supplied != "" {
		c.docs, c.lim = sourceDocs, valuesLimits
	}
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
func (c *cursor) advance(res chtypes.BatchResult, lead, kept, next int) {
	if c.off == 0 {
		switch {
		case c.array:
			c.prefix = []byte("[")
		case c.format == FormatCSVWithNames || c.format == FormatTSVWithNames:
			c.prefix = c.body[:c.at(res.Rows[0].InputSpan.Off, lead)]
		}
	}
	c.off, c.rows, c.grow = next, c.rows+kept, 1
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
			c.advance(res, lead, n-1, next)
			return b, nil
		case !accepted && j < n && offset(res.Rows[j].InputSpan.Off)+offset(res.Rows[j].InputSpan.Len)+tailMargin <= content:
			// Refused well before the cut: the cut cannot be why.
			c.done = true
			return t.batch(c, res, chk, n), nil
		case !accepted && j >= 1 && j < n:
			return t.closeAt(c, res, lead, j, chk)
		}
		// Too few records to cut at: grow the window until it holds two, or
		// is the rest of the body. Each step doubles both bounds.
		c.grow *= 2
	}
}

// closeAt reads the window again up to record j's start, every record in it
// finished, and starts the next window at record j.
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
	c.advance(closed, lead, j, next)
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

// TakeParseSlot takes one of the slots IngestRole holds for a body's walk,
// without waiting: ok is false when every slot is in use. release frees it.
func (e *Engine) TakeParseSlot() (release func(), ok bool) {
	select {
	case e.slots <- struct{}{}:
		return func() { <-e.slots }, true
	default:
		return nil, false
	}
}

// IngestRole parses body as the role's view of table (RoleTable), a window
// at a time, re-acquiring the view for each window and releasing it between
// them, so a rebind waits on one window rather than the whole body. Every
// window must come from the one compiled handle the first did: a rebind
// between two is the table's *Unavailable, retried by the caller.
//
// The body holds one of the engine's parse slots for its whole walk, so the
// memory all ingest parses hold together is bounded; with none free it
// returns ErrBusy at once. A context that ends stops the walk between
// windows; the first window runs whatever its state, as one call did.
func (e *Engine) IngestRole(ctx context.Context, id tenant.ID, table string, shape RoleShape, format Format, opts IngestOptions, body []byte, checks ...Predicate) (Batch, error) {
	c, err := newCursor(format, opts, body)
	if err != nil {
		return Batch{}, err
	}
	release, ok := e.TakeParseSlot()
	if !ok {
		return Batch{}, ErrBusy
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
