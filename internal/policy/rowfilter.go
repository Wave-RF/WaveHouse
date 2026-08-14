package policy

import (
	"encoding/json"
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

// maxNumericOperandChars is the O(1) length pre-gate on numeric comparison
// operands, checked before ANY scan of the value. It is verdict-preserving: a
// JSON number literal carries at most four non-digit bytes (a sign, a decimal
// point, an exponent marker and its sign), so anything longer already fails
// the canonical digit bound (maxCanonicalDigits) — but proving that inside the
// canonical gate costs a full json.Valid pass plus a digit count over a
// client-controlled value, per subscriber per event on the fan-out goroutine.
// The gate refuses first, without reading the bytes.
const maxNumericOperandChars = maxCanonicalDigits + 4

// maxTimeOperandChars is the same O(1) pre-gate for timestamp operands: the
// ingest grammar's longest accepted spelling (RFC 3339 with nanoseconds and a
// numeric offset) is 35 bytes, so 64 is generous slack — and the parser scans
// its input, which without the gate a megabyte "timestamp" would make a
// per-subscriber-per-event cost.
const maxTimeOperandChars = 64

// NumericFamily classifies how a ClickHouse numeric column stores a value —
// the narrowing the row-filter comparison must apply to BOTH operands so its
// verdict matches the query path, where ClickHouse narrows the stored value at
// insert AND the filter constant at compare. The zero value is NumericNone: no
// storage model, every comparison refused — the same fail-closed zero-value
// contract as ColumnOpaque, so a future numeric type nobody classified can
// never be compared under the wrong model.
type NumericFamily uint8

const (
	NumericNone    NumericFamily = iota // unclassified: refuse, fail closed
	NumericInteger                      // Int*/UInt*: exact at any width
	NumericFloat                        // Float32/Float64: IEEE rounding at Bits
	NumericDecimal                      // Decimal*: truncation at Scale
)

// NumericSpec is a numeric column's storage model. Bits is the bit width
// (float width for NumericFloat, integer width for NumericInteger); Unsigned
// marks UInt* (NumericInteger only); Precision and Scale are the stored total
// and fractional digit counts (NumericDecimal only, 1 ≤ Precision ≤ 76).
type NumericSpec struct {
	Family    NumericFamily
	Bits      int
	Unsigned  bool
	Precision int
	Scale     int
}

// intBounds holds each ClickHouse integer width's inclusive decimal bounds as
// canonical-form strings, computed once. A constant outside the width is not
// reliably modelable — on one and the same release, ClickHouse was measured to
// ERROR the comparison (a negative literal against an unsigned column: the
// role reads no rows), to PROMOTE and compare mathematically ('256' against a
// UInt8), and to WRAP at a width boundary ('9223372036854775808' against an
// Int64 compares as −2^63, where exact-precision comparison would ADMIT the
// −2^63 rows SQL hides under !=). Refusing out-of-range operands is the one
// rule safe under all three behaviors; the cost is availability on bounds no
// in-range data could ever satisfy differently.
var intBounds = func() map[int]struct{ sMin, sMax, uMax string } {
	m := make(map[int]struct{ sMin, sMax, uMax string }, 6)
	one := big.NewInt(1)
	for _, bits := range []int{8, 16, 32, 64, 128, 256} {
		uMax := new(big.Int).Sub(new(big.Int).Lsh(one, uint(bits)), one)
		sMax := new(big.Int).Sub(new(big.Int).Lsh(one, uint(bits-1)), one)
		sMin := new(big.Int).Neg(new(big.Int).Lsh(one, uint(bits-1)))
		m[bits] = struct{ sMin, sMax, uMax string }{sMin.String(), sMax.String(), uMax.String()}
	}
	return m
}()

// integerInRange reports whether a canonical integer form lies within the
// column's width. An unknown width refuses — fail closed, never a guessed
// range.
func (n NumericSpec) integerInRange(c string) bool {
	b, ok := intBounds[n.Bits]
	if !ok {
		return false
	}
	if n.Unsigned {
		return compareCanonicalDecimals(c, "0") >= 0 && compareCanonicalDecimals(c, b.uMax) <= 0
	}
	return compareCanonicalDecimals(c, b.sMin) >= 0 && compareCanonicalDecimals(c, b.sMax) <= 0
}

// decimalInPrecision reports whether a canonical form's integer digits fit the
// column's Precision−Scale budget. A payload past it is never storable
// (DECIMAL_OVERFLOW rejects the insert); a constant past it was measured to
// promote and compare mathematically on the query path — but the integer
// widths' wrap behavior (intBounds) shows the same class is not reliably
// modelable across pairs, so the refusal keeps one rule for every family at an
// availability-only cost. A lone "0" integer part spends no digits
// (Decimal(2,2) legally stores 0.99).
func (n NumericSpec) decimalInPrecision(c string) bool {
	if n.Precision < 1 || n.Scale < 0 || n.Scale > n.Precision {
		// No coherent model: refuse, fail closed. The Scale bounds also protect
		// truncateScale's slicing — a hand-built spec must degrade to refusal,
		// never a panic on the fan-out goroutine.
		return false
	}
	intPart, _, _ := strings.Cut(strings.TrimPrefix(c, "-"), ".")
	digits := len(intPart)
	if intPart == "0" {
		digits = 0
	}
	return digits <= n.Precision-n.Scale
}

// compare orders two canonical decimal operands (numericCanonical /
// CanonicalNumericLiteral output) in the column's storage domain, ok=false when
// the model refuses the pair. Narrowing BOTH sides is what ClickHouse itself
// does — it narrows the payload at insert and the bound constant at compare
// (verified: Float32 stores 16777217 as 16777216 and `= '16777217'` still
// matches; Decimal(10,2) stores 1.005 as 1.00 and `= '1.005'` still matches) —
// so a threshold filter can no longer admit an event whose stored row lands on
// the other side of the comparison (the ordering fail-open raised on #381).
func (n NumericSpec) compare(a, b string) (int, bool) {
	switch n.Family {
	case NumericInteger:
		// A fractional CONSTANT against an integer column is a per-query type
		// error on the SQL path (the role reads no rows, loudly); a fractional
		// PAYLOAD was never storable in the column. Refuse both — fail closed.
		if strings.Contains(a, ".") || strings.Contains(b, ".") {
			return 0, false
		}
		// Same rule for the column's range: ClickHouse's reading of an
		// out-of-range constant varies by pair (error, mathematical promotion,
		// or a width-boundary wrap that compares against a DIFFERENT value
		// than written — see intBounds), and an out-of-range payload was never
		// storable. Refuse both sides rather than model any one behavior.
		if !n.integerInRange(a) || !n.integerInRange(b) {
			return 0, false
		}
		return compareCanonicalDecimals(a, b), true
	case NumericFloat:
		fa, ok := narrowFloat(a, n.Bits)
		if !ok {
			return 0, false
		}
		fb, ok := narrowFloat(b, n.Bits)
		if !ok {
			return 0, false
		}
		// Both operands are now exact values of the column's float domain, so
		// direct comparison IS the domain comparison — no ties left to break.
		switch {
		case fa < fb:
			return -1, true
		case fa > fb:
			return 1, true
		default:
			return 0, true
		}
	case NumericDecimal:
		// Precision is the range gate of the decimal family: a payload with
		// integer digits past Precision−Scale is never storable (the insert is
		// rejected), while a constant past it was measured to PROMOTE on the
		// query path and compare mathematically — the refusal there is an
		// accepted availability cost, taken because the integer widths' wrap
		// behavior proves this class has no reliable single model (see
		// decimalInPrecision — one story across the three sites). Scale
		// truncation below cannot change integer digits, so gating
		// pre-truncation is exact.
		if !n.decimalInPrecision(a) || !n.decimalInPrecision(b) {
			return 0, false
		}
		return compareCanonicalDecimals(truncateScale(a, n.Scale), truncateScale(b, n.Scale)), true
	case NumericNone:
		return 0, false // no storage model: refuse, fail closed
	default:
		return 0, false // future family nobody taught this switch: same refusal
	}
}

// narrowFloat converts a canonical decimal form to the column's float domain
// with a single correct rounding (ParseFloat at the exact bit width — never a
// float64 detour, whose double rounding can land Float32 values one ULP off).
// A magnitude the domain can't hold refuses the comparison: ClickHouse would
// store ±Inf there, and matching infinities is a verdict this evaluator can't
// prove cheaply, so the row is withheld — availability, never exposure. An
// unknown width refuses too: ParseFloat silently treats any other bitSize as
// 64, which would compare a Float32 column in the wrong (wider) domain — the
// fail-open direction — instead of the zero-value-refuses contract the
// integer and decimal families keep.
func narrowFloat(canonical string, bits int) (float64, bool) {
	if bits != 32 && bits != 64 {
		return 0, false
	}
	f, err := strconv.ParseFloat(canonical, bits)
	if err != nil {
		return 0, false
	}
	return f, true
}

// truncateScale narrows a canonical decimal form to scale fractional digits,
// truncating toward zero — ClickHouse's Decimal cast (1.005, 1.006 and 1.009
// all store as 1.00 in a Decimal(10,2); rounding would predict 1.01). The
// result is re-canonicalized (trailing zeros trimmed, bare "-0" folded) so it
// stays valid compareCanonicalDecimals input.
func truncateScale(canonical string, scale int) string {
	intPart, frac, hasFrac := strings.Cut(canonical, ".")
	if !hasFrac {
		return canonical
	}
	if len(frac) > scale {
		frac = frac[:scale]
	}
	for len(frac) > 0 && frac[len(frac)-1] == '0' {
		frac = frac[:len(frac)-1]
	}
	if len(frac) > 0 {
		return intPart + "." + frac
	}
	if intPart == "-0" {
		return "0"
	}
	return intPart
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

// numericCanonical renders a payload value as the canonical decimal form the
// numeric comparison consumes, ok=false for anything that is not a number a
// ClickHouse numeric column could have stored: booleans, structured values and
// null, spellings outside the JSON number grammar ("Inf", "NaN", "0x1f",
// "007"), values whose exact digits were lost upstream (float64 at/past 2^53),
// and anything past the canonical digit bound. String and json.Number inputs
// take the claim side's own gates (CanonicalNumericLiteral / CanonicalScalar),
// so the payload and constant sides can never disagree on what counts as a
// number or how it is spelled.
func numericCanonical(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		if len(x) > maxNumericOperandChars {
			return "", false
		}
		return CanonicalNumericLiteral(x)
	case json.Number:
		if len(x) > maxNumericOperandChars {
			return "", false
		}
		return CanonicalScalar(x)
	case float64:
		// CanonicalScalar applies the 2^53 exactness guard and renders
		// positionally; the literal gate then re-canonicalizes the one
		// rendering FormatFloat emits that canonical form forbids ("-0").
		s, ok := CanonicalScalar(x)
		if !ok {
			return "", false
		}
		return CanonicalNumericLiteral(s)
	default:
		return "", false
	}
}

// scalarString renders a JSON-decoded scalar as the exact BYTES compared under
// ColumnText and ColumnOpaque — deliberately the payload's raw spelling, never
// a canonical form: a String column stores the payload text verbatim, so byte
// comparison against it must use that spelling (canonicalizing "1.0" to "1"
// here would move equality away from what ClickHouse stores). Numeric coercion
// deliberately does NOT live here — the ColumnNumeric arm routes both operands
// through the claim side's canonical machinery (numericCanonical). Non-scalars
// (arrays, objects, null) return ok=false so the predicate fails closed rather
// than guessing. The float64 case serves callers that decoded without
// UseNumber (the stream itself always does); -1 precision emits the shortest
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
