package policy

import (
	"strconv"
	"strings"
)

// HasRowFilter reports whether this role/table entry carries a row-level-security
// predicate. The stream fan-out uses it to decide whether an event can be projected
// once for a whole role bucket (no filter) or must be checked per subscriber against
// that subscriber's claims (filter present). A nil receiver (no policy applies) has
// no filter.
func (p *ResolvedPermissions) HasRowFilter() bool {
	return p != nil && len(p.rowFilter) > 0
}

// RowVisible reports whether row satisfies every resolved row-filter predicate — the
// in-memory twin of the query path's WHERE clause, evaluated against a decoded event
// so the stream applies the same row-level security the query path does. Predicates
// are ANDed; the query path joins them with AND too.
//
// numericCols maps column name → true when that column's ClickHouse type is numeric
// (the caller supplies it from the table schema; see discovery.IsNumericType).
// Numeric columns compare numerically, so 9 < 100 as ClickHouse would; every other
// column — and any column absent from the map, e.g. when no schema is available —
// compares as text. This is exact for the equality/set operators row-level security
// actually uses (=, !=, in) and best-effort for ordering (>, <): it cannot perfectly
// mirror ClickHouse's per-type coercion for exotic types (Decimal / Int128 beyond
// float64 precision, Date/DateTime stored as Unix numbers). Every ambiguous or
// uncomparable case fails closed — the row is hidden, never leaked — so the boundary
// costs availability, not confidentiality.
//
// A nil receiver (no policy applies) makes every row visible.
func (p *ResolvedPermissions) RowVisible(row map[string]any, numericCols map[string]bool) bool {
	if p == nil {
		return true
	}
	for _, pred := range p.rowFilter {
		if !pred.matches(row, numericCols[pred.Column]) {
			return false
		}
	}
	return true
}

// matches evaluates one predicate against the row, failing closed (false) whenever
// the value is absent or can't be compared as required.
func (pred resolvedPredicate) matches(row map[string]any, numeric bool) bool {
	raw, ok := row[pred.Column]
	if !ok {
		return false // column not in the event ⇒ can't prove the row is allowed
	}
	switch pred.Op {
	case "=":
		c, ok := compareScalar(raw, pred.Values[0], numeric)
		return ok && c == 0
	case "!=":
		c, ok := compareScalar(raw, pred.Values[0], numeric)
		return ok && c != 0
	case ">":
		c, ok := compareScalar(raw, pred.Values[0], numeric)
		return ok && c > 0
	case "<":
		c, ok := compareScalar(raw, pred.Values[0], numeric)
		return ok && c < 0
	case "in":
		for _, v := range pred.Values {
			if c, ok := compareScalar(raw, v, numeric); ok && c == 0 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// compareScalar compares an event value against a resolved filter value, returning
// -1/0/+1 and ok=false when the comparison can't be made (a non-scalar event value,
// or a numeric comparison whose operands don't parse as numbers). numeric selects
// numeric vs lexicographic ordering.
func compareScalar(rowVal any, filterVal string, numeric bool) (int, bool) {
	s, ok := scalarString(rowVal)
	if !ok {
		return 0, false
	}
	if numeric {
		a, err1 := strconv.ParseFloat(s, 64)
		b, err2 := strconv.ParseFloat(filterVal, 64)
		if err1 != nil || err2 != nil {
			return 0, false
		}
		switch {
		case a < b:
			return -1, true
		case a > b:
			return 1, true
		default:
			return 0, true
		}
	}
	return strings.Compare(s, filterVal), true
}

// scalarString renders a JSON-decoded scalar as the canonical string compared
// against a (string-valued) filter. Non-scalars (arrays, objects, null) return
// ok=false so the predicate fails closed rather than guessing. JSON numbers arrive
// as float64 via encoding/json; -1 precision emits the shortest round-trip form
// without an exponent, so integer IDs read back as "123", not "1.23e+02".
func scalarString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(x), true
	default:
		return "", false
	}
}
