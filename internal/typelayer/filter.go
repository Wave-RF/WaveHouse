package typelayer

import (
	"container/list"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// filterCacheSize bounds the compiled filters held per table. A filter handle
// is identified by (expression, bound values), and the values come from tenant
// claims, so an unbounded cache is a memory/CPU denial of service. The budget
// is split across the handle pool (see compileDDL), so this is the table's
// total, not each slot's.
const filterCacheSize = 4096

// Predicate is one resolved row-filter clause. Values are the canonical strings
// policy already resolved for the SQL path, so the stream and the query answer
// off one resolution; len(Values)==0 means "matches nothing" and never widens.
type Predicate = policy.Predicate

// Reason names why a row was not visible, for the withheld-reason metric.
const (
	ReasonFilter  = "filter"  // the predicate answered false
	ReasonError   = "error"   // the predicate threw on this row's values
	ReasonDecline = "decline" // chtypes would not answer
)

// Row is one parsed event, reusable across every subscriber's filter. Parsing
// is the expensive half, so the hub parses once per event and evaluates K
// filters against the result.
//
// The Table it came from must stay held (not Released) until Close.
type Row struct {
	table *Table
	slot  *schemaSlot
	block *chtypes.LoadedBlock
}

// ParseRow parses one JSONCompactEachRow line, with or without its trailing
// newline. columns must be the generation's wire columns exactly: a positional
// row is uninterpretable against any other order, so a mismatch is
// ErrColumnsDrift rather than a guess.
func (t *Table) ParseRow(columns []string, row []byte) (*Row, error) {
	if len(t.slots) == 0 {
		return nil, &Unavailable{Table: t.Name, Cause: t.cause}
	}
	if !slices.Equal(columns, t.WireColumns) {
		return nil, fmt.Errorf("%w: event carries %v, generation %d exports %v",
			ErrColumnsDrift, columns, t.Generation, t.WireColumns)
	}
	body := row
	if n := len(body); n == 0 || body[n-1] != '\n' {
		body = append(append(make([]byte, 0, n+1), body...), '\n')
	}
	// The block and every filter evaluated against it stay on ONE handle: a
	// cross-handle Eval takes both handles' locks and hands back exactly the
	// serialization the pool exists to avoid.
	s := t.slot()
	block, err := s.schema.ParseBlock(chtypes.JSONCompactEachRow, body, InsertSettings())
	if err != nil {
		return nil, err
	}
	return &Row{table: t, slot: s, block: block}, nil
}

// Close frees the parsed block. Required: the C layer does not refcount.
func (r *Row) Close() {
	if r.block != nil {
		r.block.Close()
		r.block = nil
	}
}

// Visible reports whether the row satisfies every predicate. Only a definite
// true is visible — false, a predicate that threw, and a decline all withhold.
func (r *Row) Visible(preds []Predicate) bool {
	ok, _ := r.VisibleWithReason(preds)
	return ok
}

// VisibleWithReason is Visible plus the label the withheld-reason metric wants.
// The reason is "" when the row is visible.
func (r *Row) VisibleWithReason(preds []Predicate) (bool, string) {
	if len(preds) == 0 {
		return true, ""
	}
	if r.block == nil {
		return false, ReasonDecline
	}
	expr, params, ok := r.table.render(preds)
	if !ok {
		// An unresolvable predicate matches nothing, exactly as the SQL path's
		// `1 = 0` does — the two surfaces must not disagree (#457).
		return false, ReasonFilter
	}

	filter := r.table.filterOn(r.slot, expr, params)
	if filter == nil {
		return false, ReasonDecline
	}
	res, err := filter.Eval(r.block)
	if err != nil || res.Outcome != chtypes.FilterOK || len(res.Verdicts) == 0 {
		return false, ReasonDecline
	}
	return verdictBool(res.Verdicts[0])
}

// verdictBool maps one chtypes verdict onto (visible, reason). Only
// VerdictTrue is visible, and the zero value is Decline, so an answer nobody
// set withholds.
func verdictBool(v chtypes.Verdict) (bool, string) {
	switch v {
	case chtypes.VerdictTrue:
		return true, ""
	case chtypes.VerdictFalse:
		return false, ReasonFilter
	case chtypes.VerdictError:
		return false, ReasonError
	case chtypes.VerdictDecline:
		return false, ReasonDecline
	default:
		return false, ReasonDecline
	}
}

// CheckVerdicts evaluates a role's insert check clauses against rows that have
// already been exported. It is the ingest-side twin of Row.Visible: ONE
// filter, AND-joined over every predicate, evaluated against ONE parse of the
// payload.
//
// payload is a JSONCompactEachRow body — in practice Batch.Payload — and both
// returned slices are index-aligned with its rows, which are index-aligned
// with the batch's ACCEPTED records (NOT with Batch.Rows, which also holds the
// records that did not parse). Call it on the SAME Table that produced the
// payload: a positional body is only interpretable against the column shape it
// was exported from, so a role's payload must be checked by the role's handle.
//
// Only chtypes.VerdictTrue is true; reasons carries "" for a passing row and
// ReasonFilter / ReasonError / ReasonDecline otherwise, so a caller can keep
// "the data says no" (403) apart from "we could not tell" (422, fail closed).
//
// No predicates means no check applies and every row passes. A predicate with
// no Values matches nothing and short-circuits to all-false WITHOUT compiling
// — an unresolvable claim must never reach the compiler. A Go-level failure
// (an unavailable handle, a payload chtypes will not parse) is an error and
// never a verdict.
func (t *Table) CheckVerdicts(payload []byte, preds []Predicate) ([]bool, []string, error) {
	if len(t.slots) == 0 {
		return nil, nil, &Unavailable{Table: t.Name, Cause: t.cause}
	}
	n := countRecords(payload, 0)
	if n == 0 {
		return nil, nil, nil
	}
	if len(preds) == 0 {
		verdicts := make([]bool, n)
		for i := range verdicts {
			verdicts[i] = true
		}
		return verdicts, make([]string, n), nil
	}

	expr, params, ok := t.render(preds)
	if !ok {
		v, r := uniformVerdict(n, ReasonFilter)
		return v, r, nil
	}

	s := t.slot()
	block, err := s.schema.ParseBlock(chtypes.JSONCompactEachRow, payload, InsertSettings())
	if err != nil {
		return nil, nil, err
	}
	defer block.Close()

	filter := t.filterOn(s, expr, params)
	if filter == nil {
		v, r := uniformVerdict(n, ReasonDecline)
		return v, r, nil
	}
	res, err := filter.Eval(block)
	if err != nil || res.Outcome != chtypes.FilterOK {
		v, r := uniformVerdict(n, ReasonDecline)
		return v, r, nil
	}
	if len(res.Verdicts) != n {
		// Index alignment is the whole contract here: a verdict attributed to
		// the wrong row would approve one record on another record's answer.
		t.log.Error("chtypes returned a verdict count that does not match the exported rows; withholding the batch",
			"table", t.Name, "generation", t.Generation, "rows", n, "verdicts", len(res.Verdicts))
		v, r := uniformVerdict(n, ReasonDecline)
		return v, r, nil
	}

	verdicts := make([]bool, n)
	reasons := make([]string, n)
	for i, v := range res.Verdicts {
		verdicts[i], reasons[i] = verdictBool(v)
	}
	return verdicts, reasons, nil
}

// uniformVerdict is the fail-closed answer: every row withheld for one reason.
func uniformVerdict(n int, reason string) ([]bool, []string) {
	reasons := make([]string, n)
	for i := range reasons {
		reasons[i] = reason
	}
	return make([]bool, n), reasons
}

// render builds the AND-joined expression and the parameter map.
//
// EVERY value binds as {pN:String}, whatever the column's declared type.
// Measured (AUDIT §C.1, chtypes 26.6 cross-checked against a live 26.3
// server): a String parameter reproduces the SQL path's answer on every
// operator, every type and every hostile spelling — `u8 < '256'` is false
// where a UInt64 binding over-admitted it, `u8 != '256'` is true, and a
// spelling the column cannot read (`-1`, `1.5`, `007`) comes back as the
// server's own code 53 at EVALUATION time, which withholds the row. Binding in
// the column's own type wraps an out-of-domain integer; binding in the widest
// integer type compares mathematically rather than in the column's domain.
// String does neither, and needs no per-type table.
//
// Every value is encoded with chsql.EscapeStringParam, the SAME encoding the
// SQL path uses for its `{p:String}` parameters. The artifact reads a filter
// parameter with ClickHouse's escaped-text reader, exactly as the server reads
// one off the HTTP interface: measured on the 26.6 artifact, a stored `a\b`
// compared false against the raw value (the `\b` read as a backspace), and a
// stored tab, newline or trailing backslash would not compile at all (a
// decline — every row withheld). With the encoding applied all of them compare
// equal, on `=` and on `in` alike. Before this, a claim carrying any of those
// bytes silently withheld rows the SQL path returned.
//
// Values are never interpolated, so a hostile claim is inert by construction.
// Reports false when a predicate cannot be expressed, which fails closed
// without compiling anything.
func (t *Table) render(preds []Predicate) (string, map[string]string, bool) {
	var b strings.Builder
	params := make(map[string]string, len(preds))
	n := 0
	for i, p := range preds {
		if _, known := t.cols[p.Column]; !known || len(p.Values) == 0 {
			return "", nil, false
		}
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(chsql.QuoteIdent(p.Column))
		switch p.Op {
		case "=", "!=", ">", "<":
			if len(p.Values) != 1 {
				return "", nil, false
			}
			name := fmt.Sprintf("p%d", n)
			n++
			params[name] = chsql.EscapeStringParam(p.Values[0])
			fmt.Fprintf(&b, " %s {%s:String}", p.Op, name)
		case "in":
			b.WriteString(" IN (")
			for j, v := range p.Values {
				if j > 0 {
					b.WriteString(", ")
				}
				name := fmt.Sprintf("p%d", n)
				n++
				params[name] = chsql.EscapeStringParam(v)
				fmt.Fprintf(&b, "{%s:String}", name)
			}
			b.WriteByte(')')
		default:
			return "", nil, false
		}
	}
	return b.String(), params, true
}

// filterOn returns the compiled handle for this expression and parameter set
// on ONE slot, compiling it at most once per (slot, generation). A compile
// failure is cached as a negative entry so a broken policy costs one compile
// and one log line, not one per event.
func (t *Table) filterOn(s *schemaSlot, expr string, params map[string]string) *chtypes.LoadedFilter {
	key := cacheKey(t.Generation, expr, params)
	c := s.filters

	c.mu.Lock()
	defer c.mu.Unlock()
	if el, hit := c.index[key]; hit {
		c.order.MoveToFront(el)
		return el.Value.(*filterEntry).filter
	}

	f, err := s.schema.CompileFilter(expr, chtypes.WithFilterParams(params))
	if err != nil {
		f = nil
		t.log.Error("row filter will not compile; withholding every row for it",
			"table", t.Name, "generation", t.Generation, "expr", expr, "error", err)
	}
	el := c.order.PushFront(&filterEntry{key: key, filter: f})
	c.index[key] = el
	if c.order.Len() > c.cap {
		c.evictOldestLocked()
	}
	return f
}

// cacheKey identifies a compiled handle. Values are baked into the handle at
// compile time, so they belong in the key alongside the generation that owns
// the schema.
func cacheKey(generation uint64, expr string, params map[string]string) string {
	// The parameter names are positional (p0, p1 …) and generated from the same
	// expression, so a JSON object over them is stable without sorting.
	enc, _ := json.Marshal(params)
	return fmt.Sprintf("%d\x00%s\x00%s", generation, expr, enc)
}

type filterEntry struct {
	key    string
	filter *chtypes.LoadedFilter // nil: this expression does not compile
}

type filterCache struct {
	mu    sync.Mutex
	cap   int
	order *list.List // front = most recently used
	index map[string]*list.Element
}

func newFilterCache(capacity int) *filterCache {
	return &filterCache{cap: capacity, order: list.New(), index: make(map[string]*list.Element)}
}

func (c *filterCache) evictOldestLocked() {
	el := c.order.Back()
	if el == nil {
		return
	}
	c.order.Remove(el)
	e := el.Value.(*filterEntry)
	delete(c.index, e.key)
	if e.filter != nil {
		e.filter.Close()
	}
}

// closeAll drops every handle. Called before the schema is closed so no freed
// pointer survives in the index; the schema would close them anyway, but not
// the map holding them.
func (c *filterCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.order.Front(); el != nil; el = el.Next() {
		if f := el.Value.(*filterEntry).filter; f != nil {
			f.Close()
		}
	}
	c.order.Init()
	c.index = make(map[string]*list.Element)
}
