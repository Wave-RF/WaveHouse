package policy

import (
	"encoding/json"
	"strings"
	"time"
)

// HasRowFilter reports whether this role/table entry carries a row-level-security
// predicate. The stream fan-out uses it to decide whether an event can be projected
// once for a whole role bucket (no filter) or must be checked per subscriber against
// that subscriber's claims (filter present). A nil receiver (no policy applies) has
// no filter.
//
// It answers YES for a denied grant and for one whose read side was never
// resolved, neither of which has a predicate to speak of. That is deliberate:
// this is the GATE in front of RowVisible, and a "no filter" answer sends the
// caller down the deliver-to-the-whole-bucket fast path where RowVisible is
// never consulted. Saying yes forces the per-subscriber path, where RowVisible
// denies. Same shape and same reason as RestrictsColumns, which guards the
// builder's SELECT * expansion.
func (p *ResolvedPermissions) HasRowFilter() bool {
	if p == nil {
		return false
	}
	if !p.Allowed || p.Select.unresolved {
		return true
	}
	return len(p.Select.rowFilter) > 0
}

// maxTimeOperandChars is the same O(1) pre-gate for timestamp operands: the
// ingest grammar's longest accepted spelling (RFC 3339 with nanoseconds and a
// numeric offset) is 35 bytes, so 64 is generous slack — and the parser scans
// its input, which without the gate a megabyte "timestamp" would make a
// per-subscriber-per-event cost.
const maxTimeOperandChars = 64

// ColumnKind classifies a column's ClickHouse type for the in-memory row-filter
// comparison. The zero value is ColumnOpaque, so a nil map, a column absent from
// the map, and a column the schema doesn't know all land on the most conservative
// class — the three "no type knowledge" states are indistinguishable and equally
// closed, never a silent downgrade to a laxer comparison.
type ColumnKind uint8

const (
	// ColumnOpaque: no usable type knowledge (no schema, unknown column) or a type
	// whose text rendering is not canonical — UUID (case), Enum (name vs number),
	// Bool (true vs 1), Date/Date32 (producer spelling), IPv4/IPv6, … For these only
	// byte-equality is trustworthy: identical strings parse to identical ClickHouse
	// values, but differing strings prove nothing. So = and in admit exactly the
	// event's own rendering, while !=, > and < fail closed (the row is withheld).
	ColumnOpaque ColumnKind = iota
	// ColumnNumeric (Int*/UInt*/Float*/Decimal*): both operands render to exact
	// canonical decimal form through the claim side's #457 machinery, then
	// compare in the column's STORAGE domain (ColumnSpec.Numeric): integers at
	// any width exactly, floats after IEEE narrowing to the column's bit width,
	// decimals after truncation to the column's scale — the same narrowing
	// ClickHouse applies to the stored value and the bound constant, so stream
	// and query verdicts agree even on narrowing columns.
	ColumnNumeric
	// ColumnText (String, incl. Nullable/LowCardinality): byte comparison is
	// ClickHouse comparison — equality AND lexicographic order — so every operator
	// is exact. FixedString is NOT ColumnText (zero-padded storage).
	ColumnText
	// ColumnTime (DateTime/DateTime64): operands parse as instants through the
	// caller-supplied ColumnSpec.ParseTime — the same grammar, zone rule, and
	// range guard ingest canonicalization applies — and compare chronologically,
	// so every operator is exact across spellings: a zone-less filter constant
	// matches the canonicalized RFC 3339 payload denoting the same instant. A
	// side that can't be read as a provable instant fails closed.
	ColumnTime
)

// ColumnSpec is one column's comparison contract for the in-memory row filter:
// the ColumnKind classification plus the kind's parameters — ColumnTime's
// instant parser, ColumnNumeric's storage model. The zero value is ColumnOpaque
// with neither, so a nil map, an absent column, and an unknown type all land on
// the most conservative class — never a silent downgrade to a laxer comparison.
type ColumnSpec struct {
	Kind ColumnKind
	// ParseTime converts one rendering of this timestamp column's value — an
	// ingested payload value (string / json.Number / float64) or a resolved
	// filter constant (always a string) — to the instant ClickHouse would store,
	// truncated to the column's precision. ok=false (unparseable, or outside the
	// column type's range, which insert-time saturation would move) fails the
	// comparison closed. Set iff Kind is ColumnTime; the stream supplies it from
	// the schema registry (discovery's Column.TimeParser) so the filter and
	// ingest canonicalization can never disagree on the grammar.
	ParseTime func(v any) (t time.Time, ok bool)
	// Numeric is the column's storage model, set iff Kind is ColumnNumeric
	// (from discovery.NumericStorageOf via the stream's columnSpecs). Its zero
	// value refuses every comparison, so a ColumnNumeric spec built without a
	// model fails closed rather than comparing under the wrong semantics.
	Numeric NumericSpec
}

// RowVisible reports whether row satisfies every resolved row-filter predicate — the
// in-memory twin of the query path's WHERE clause, evaluated against a decoded event
// so the stream applies the same row-level security the query path does. Predicates
// are ANDed; the query path joins them with AND too.
//
// cols maps column name → ColumnSpec, supplied by the caller from the table
// schema (see stream.Hub's columnSpecs). Numeric columns compare numerically in
// the column's storage domain (9 < 100, as ClickHouse would; Float/Decimal
// operands narrowed the way insert and constant binding narrow them), String
// columns compare bytewise (exactly ClickHouse's String collation),
// DateTime/DateTime64 columns compare as instants (both operands parsed through
// the spec's ParseTime, the same grammar ingest canonicalizes with), and
// everything else — including every column when no schema is available — admits
// only byte-equality (= / in) and fails !=, > and < closed: the evaluator cannot
// mirror ClickHouse's per-type coercion, and text comparison there could admit rows
// the query path excludes ("9" > "100" as text, an uppercase UUID under !=). Every
// ambiguous or uncomparable case fails closed — the row is hidden, never leaked —
// so the boundary costs availability, not confidentiality.
//
// That guarantee is about the INGESTED PAYLOAD value, which is what the stream
// evaluates; the query path evaluates the stored row. Storage-domain narrowing
// keeps the two verdicts aligned for values ClickHouse stores; the residual
// asymmetry is an event whose INSERT later fails entirely (out-of-range value,
// batch error → DLQ): it was already streamed to whoever the filter admitted,
// and the row never becomes queryable. Documented in access-control.mdx's
// enforcement caution.
//
// A nil receiver (no policy applies) makes every row visible.
func (p *ResolvedPermissions) RowVisible(row map[string]any, cols map[string]ColumnSpec) bool {
	if p == nil {
		return true
	}
	// A denied role sees no rows — fail closed, mirroring the !Allowed guard on
	// IsColumnAllowed, so a denied receiver never reads as "no filter ⇒ all visible".
	// A grant resolved for INSERT is refused here for the same reason: its empty
	// read side would otherwise read as "no filter", admitting every row.
	if !p.Allowed || p.Select.unresolved {
		return false
	}
	for _, pred := range p.Select.rowFilter {
		if !pred.matches(row, cols[pred.Column]) {
			return false
		}
	}
	return true
}

// matches evaluates one predicate against the row, failing closed (false) whenever
// the value is absent or can't be compared as required.
func (pred ResolvedPredicate) matches(row map[string]any, spec ColumnSpec) bool {
	// No values ⇒ matches nothing: an empty/unresolvable "in" set, or a scalar
	// whose constant was unrenderable — the in-memory twin of the `1 = 0`
	// predicatesToSQL emits for the same cases.
	if len(pred.Values) == 0 {
		return false
	}
	raw, ok := row[pred.Column]
	if !ok {
		return false // column not in the event ⇒ can't prove the row is allowed
	}
	switch pred.Op {
	case "=":
		c, ok := compareScalar(raw, pred.Values[0], spec)
		return ok && c == 0
	case "!=":
		c, ok := compareScalar(raw, pred.Values[0], spec)
		return ok && c != 0
	case ">":
		c, ok := compareScalar(raw, pred.Values[0], spec)
		return ok && c > 0
	case "<":
		c, ok := compareScalar(raw, pred.Values[0], spec)
		return ok && c < 0
	case "in":
		for _, v := range pred.Values {
			if c, ok := compareScalar(raw, v, spec); ok && c == 0 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// compareScalar compares an event value against a resolved filter value, returning
// -1/0/+1 and ok=false when the comparison can't be made: a non-scalar event value,
// a numeric comparison whose operands don't parse as numbers, or two unequal values
// of a ColumnOpaque column (where inequality and order are unprovable). Every
// ok=false fails the enclosing predicate closed — for != that is what keeps a mere
// representation difference (an uppercase UUID, 1 for a Bool true) from being
// mistaken for a real inequality and admitting a row the query path excludes.
// filterVal is the raw resolved spelling — what byte-comparison arms compare
// and predicatesToSQL binds; the numeric arm derives its canonical reading per
// comparison, behind an O(1) length gate (see maxNumericOperandChars — folding
// the re-derivation into a memoized resolution is part of #435's scope).
func compareScalar(rowVal any, filterVal string, spec ColumnSpec) (int, bool) {
	switch spec.Kind {
	case ColumnTime:
		// The RAW value goes to the parser — a payload timestamp may legitimately
		// be a number (Unix seconds/ticks), which the spec's parser reads the same
		// way ingest does; scalarString's rendering would be a detour. Both sides
		// must parse; either failing (or a spec missing its parser) refuses the
		// comparison, fail closed.
		if spec.ParseTime == nil {
			return 0, false
		}
		// O(1) length gate before the parser scans either operand (see
		// maxTimeOperandChars). The payload arrives as a string OR a
		// json.Number (a Unix-epoch timestamp) — both are client-controlled and
		// must be gated; a bare-number payload that skipped this would leave the
		// parser's digit scan (and its %q error rendering) unbounded on the
		// fan-out path. Verdict-preserving for numbers (>19 digits never parsed)
		// and, for strings, refuses only past 64 bytes, where Go's parser would
		// have truncated sub-second digits rather than matched a real instant.
		switch v := rowVal.(type) {
		case string:
			if len(v) > maxTimeOperandChars {
				return 0, false
			}
		case json.Number:
			if len(v) > maxTimeOperandChars {
				return 0, false
			}
		}
		if len(filterVal) > maxTimeOperandChars {
			return 0, false
		}
		a, ok := spec.ParseTime(rowVal)
		if !ok {
			return 0, false
		}
		b, ok := spec.ParseTime(filterVal)
		if !ok {
			return 0, false
		}
		return a.Compare(b), true
	case ColumnNumeric:
		// Both operands route through the ONE canonical numeric gate the claim
		// side already uses (#457's CanonicalScalar machinery): exact decimal
		// form at any width, digit-bounded (the superlinear-parse guard lives
		// there — CWE-400, the row operand is client-controlled), with "NaN",
		// any Inf spelling, and every non-JSON-number rendering refused by the
		// grammar rather than by ad-hoc checks. Then the comparison itself runs
		// in the column's storage domain (NumericSpec.compare), narrowing both
		// sides the way ClickHouse narrows the stored value and the constant.
		if len(filterVal) > maxNumericOperandChars {
			return 0, false
		}
		a, ok := numericCanonical(rowVal)
		if !ok {
			return 0, false
		}
		b, ok := CanonicalNumericLiteral(filterVal)
		if !ok {
			return 0, false
		}
		// Spelling fidelity, integer family only: predicatesToSQL binds the
		// constant AS WRITTEN, and ClickHouse's integer cast in a WHERE rejects
		// non-plain spellings ("1e3", "1.5", "007") with a per-query type
		// error — the role reads no rows there, so comparing the canonical
		// reading here would ADMIT rows SQL never returns. A constant that
		// isn't its own canonical form refuses the comparison instead
		// (withhold — matching SQL's nothing, just quietly; the one measured
		// over-refusal is "-0", which ClickHouse accepts in a WHERE but the
		// canonical fold rewrites to "0" — availability, never exposure).
		// Claim-derived constants are canonical by construction and
		// unaffected. Float AND Decimal casts accept every JSON-number
		// spelling ('1e3' casts to Decimal as 1000 — verified), so neither
		// family gets a gate.
		if spec.Numeric.Family == NumericInteger && b != filterVal {
			return 0, false
		}
		return spec.Numeric.compare(a, b)
	case ColumnText:
		s, ok := scalarString(rowVal)
		if !ok {
			return 0, false
		}
		return strings.Compare(s, filterVal), true
	case ColumnOpaque:
		// Byte-equality is the only relation provable without type knowledge
		// (identical strings always parse to the same ClickHouse value); unequal
		// bytes prove nothing, so the comparison is refused and the predicate
		// fails closed.
		s, ok := scalarString(rowVal)
		if !ok {
			return 0, false
		}
		if s == filterVal {
			return 0, true
		}
		return 0, false
	default:
		return 0, false // unknown future kind: refuse to compare, fail closed
	}
}
