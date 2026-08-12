package policy

import (
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// HasRowFilter reports whether this role/table entry carries a row-level-security
// predicate. The stream fan-out uses it to decide whether an event can be projected
// once for a whole role bucket (no filter) or must be checked per subscriber against
// that subscriber's claims (filter present). A nil receiver (no policy applies) has
// no filter.
func (p *ResolvedPermissions) HasRowFilter() bool {
	return p != nil && len(p.rowFilter) > 0
}

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
	// ColumnNumeric (Int*/UInt*/Float*/Decimal*): operands parse as numbers and
	// compare numerically, with float64-equal ties resolved at full precision.
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
// the ColumnKind classification plus, for ColumnTime, the parser that maps a
// value rendering to an instant. The zero value is ColumnOpaque with no parser,
// so a nil map, an absent column, and an unknown type all land on the most
// conservative class — never a silent downgrade to a laxer comparison.
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
}

// RowVisible reports whether row satisfies every resolved row-filter predicate — the
// in-memory twin of the query path's WHERE clause, evaluated against a decoded event
// so the stream applies the same row-level security the query path does. Predicates
// are ANDed; the query path joins them with AND too.
//
// cols maps column name → ColumnSpec, supplied by the caller from the table
// schema (see stream.Hub's columnSpecs). Numeric columns compare numerically (9 <
// 100, as ClickHouse would), String columns compare bytewise (exactly ClickHouse's
// String collation), DateTime/DateTime64 columns compare as instants (both operands
// parsed through the spec's ParseTime, the same grammar ingest canonicalizes with),
// and everything else — including every column when no schema is available — admits
// only byte-equality (= / in) and fails !=, > and < closed: the evaluator cannot
// mirror ClickHouse's per-type coercion, and text comparison there could admit rows
// the query path excludes ("9" > "100" as text, an uppercase UUID under !=). Every
// ambiguous or uncomparable case fails closed — the row is hidden, never leaked —
// so the boundary costs availability, not confidentiality.
//
// That guarantee is about the INGESTED PAYLOAD value, which is what the stream
// evaluates; the query path evaluates the stored row. The one residual asymmetry is
// insert-time numeric narrowing — a payload carrying more precision than the
// column's declared type (a Decimal's scale, Float32 width) is rounded on insert,
// so a numeric threshold filter can admit an event whose stored row lands on the
// other side of the boundary. Documented in access-control.mdx's enforcement
// caution.
//
// A nil receiver (no policy applies) makes every row visible.
func (p *ResolvedPermissions) RowVisible(row map[string]any, cols map[string]ColumnSpec) bool {
	if p == nil {
		return true
	}
	// A denied role sees no rows — fail closed, mirroring the !Allowed guard on
	// IsColumnAllowed, so a denied receiver never reads as "no filter ⇒ all visible".
	if !p.Allowed {
		return false
	}
	for _, pred := range p.rowFilter {
		if !pred.matches(row, cols[pred.Column]) {
			return false
		}
	}
	return true
}

// matches evaluates one predicate against the row, failing closed (false) whenever
// the value is absent or can't be compared as required.
func (pred resolvedPredicate) matches(row map[string]any, spec ColumnSpec) bool {
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
		s, ok := scalarString(rowVal)
		if !ok {
			return 0, false
		}
		a, err1 := strconv.ParseFloat(s, 64)
		b, err2 := strconv.ParseFloat(filterVal, 64)
		// NaN must be rejected explicitly: ParseFloat accepts "NaN", and NaN's
		// three-way comparison reads as "equal to everything" below — a fail-open.
		if err1 != nil || err2 != nil || math.IsNaN(a) || math.IsNaN(b) {
			return 0, false
		}
		switch {
		case a < b:
			return -1, true
		case a > b:
			return 1, true
		default:
			// Float equality is not proof of equality: distinct integers beyond
			// 2^53 collapse to one float64 — whether they arrived string-encoded
			// (ingest accepts that exactly to survive JS precision loss) or as bare
			// JSON numbers (the stream decodes with UseNumber so s still carries
			// the exact digits). Rounding is monotonic so only the equal case is
			// in doubt. Resolve the tie at full precision.
			return compareExact(s, filterVal)
		}
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

// compareExact compares two numeric strings at arbitrary precision — the tie-break
// for operands float64 cannot tell apart. ok=false when either side isn't an exact
// rational (±Inf, malformed), failing the predicate closed.
func compareExact(a, b string) (int, bool) {
	ra, ok := new(big.Rat).SetString(a)
	if !ok {
		return 0, false
	}
	rb, ok := new(big.Rat).SetString(b)
	if !ok {
		return 0, false
	}
	return ra.Cmp(rb), true
}

// scalarString renders a JSON-decoded scalar as the canonical string compared
// against a (string-valued) filter. Non-scalars (arrays, objects, null) return
// ok=false so the predicate fails closed rather than guessing. The stream decodes
// events with UseNumber, so JSON numbers arrive as json.Number — the exact digit
// string, which is what lets compareExact distinguish 64-bit IDs that would
// collapse into one float64. The float64 case remains for callers that decoded
// without UseNumber (tests, future paths); -1 precision emits the shortest
// round-trip form without an exponent, so integer IDs read back as "123", not
// "1.23e+02".
func scalarString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return string(x), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(x), true
	default:
		return "", false
	}
}
