package policy

import (
	"math/big"
	"strconv"
	"strings"
)

// This file compares canonical decimal forms (canonical.go's output) the way
// the column that stores them would: compareCanonicalDecimals is the exact
// digit-string ordering, and NumericSpec narrows both operands into the
// column's STORAGE domain first — the same narrowing ClickHouse applies to the
// stored value at insert and to the filter constant at compare — so the
// stream's in-memory verdict can't drift from the query path's SQL verdict.

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
	for _, bits := range []int{8, 16, 32, 64, 128, 256} {
		// big.Int because Int128/Int256 exceed every native width; rendered
		// once to decimal strings so range checks are compareCanonicalDecimals
		// calls. For bits=8 the three bounds are −128, 127, and 255.
		pow := new(big.Int).Lsh(big.NewInt(1), uint(bits-1))              // 2^(bits−1)
		sMin := new(big.Int).Neg(pow)                                     // signed min: −2^(bits−1)
		sMax := new(big.Int).Sub(pow, big.NewInt(1))                      // signed max: 2^(bits−1) − 1
		uMax := new(big.Int).Sub(new(big.Int).Lsh(pow, 1), big.NewInt(1)) // unsigned max: 2^bits − 1
		m[bits] = struct{ sMin, sMax, uMax string }{sMin.String(), sMax.String(), uMax.String()}
	}
	return m
}()

// integerInRange reports whether a canonical integer form lies within the
// column's width. An unknown width refuses — fail closed, never a guessed
// range. Integers alone need this explicit bounds table because their
// comparison is digit-string arithmetic with no inherent width; the float
// family's range gate is narrowFloat's ParseFloat-overflow refusal, and the
// decimal family's is decimalInPrecision.
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
// A magnitude the domain can't hold refuses the comparison (ParseFloat reports
// the overflow as an error — the float family's range gate): ClickHouse would
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

// compareCanonicalDecimals orders two canonical decimal forms (CanonicalScalar
// output) as numbers, by digit-string arithmetic alone — the comparison twin of
// canonicalDecimal, sharing its invariants: an optional leading '-' (never on
// zero), no leading integer zeros except a lone "0", no trailing fraction
// zeros, no exponent. Those invariants are what make the string operations
// sound: with no leading zeros a longer integer part IS the larger magnitude,
// and with no trailing zeros a fraction that is a proper prefix of another IS
// the smaller. Never a float round-trip, so 64-bit-plus IDs order exactly.
func compareCanonicalDecimals(a, b string) int {
	if a == b {
		return 0
	}
	na, nb := strings.HasPrefix(a, "-"), strings.HasPrefix(b, "-")
	switch {
	case na && !nb:
		return -1
	case !na && nb:
		return 1
	case na && nb:
		return -compareCanonicalMagnitudes(a[1:], b[1:])
	}
	return compareCanonicalMagnitudes(a, b)
}

// compareCanonicalMagnitudes orders two unsigned canonical forms.
func compareCanonicalMagnitudes(a, b string) int {
	ai, af, _ := strings.Cut(a, ".")
	bi, bf, _ := strings.Cut(b, ".")
	if len(ai) != len(bi) {
		if len(ai) < len(bi) {
			return -1
		}
		return 1
	}
	if c := strings.Compare(ai, bi); c != 0 {
		return c
	}
	return strings.Compare(af, bf)
}
