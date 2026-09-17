package policy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// This file is the policy engine's ONE rendering layer for comparison operands:
// every claim value a row-filter or insert-check compares is rendered here
// before any comparison or SQL bind, so the two read surfaces can't disagree on
// what a value "is". One entry point:
//
//	CanonicalScalar — decoded claim values (Evaluate's template resolution)
//
// It converges on one canonical decimal form (canonicalDecimal, bounded by
// maxCanonicalDigits): exact at any width, positional (never an exponent —
// "1e-3" renders as "0.001"), no leading/trailing zeros, "-0" folded to "0".
// Comparing a rendered value against a STORED row is not done here at all: both
// surfaces hand the predicate to ClickHouse's own engine — the type layer for
// the stream and the insert check, the server for /v1/query — rather than to a
// Go re-derivation of its coercion rules.

// maxCanonicalDigits bounds both a numeric literal's digit count and its
// exact decimal expansion. Big-integer parsing is superlinear in digit count
// and a claim is attacker-influenced input (CWE-400), and a short exponent
// literal can hide a wide expansion ("1e-150" is six characters with a
// 152-character exact form). 100 digits is far past any real id — uint256
// is 78.
const maxCanonicalDigits = 100

// CanonicalScalar renders a decoded JSON value as the canonical string the
// policy layer binds and compares, reporting ok=false for values with no such
// form: null, objects, and arrays. A structured value is never a sensible
// scalar comparison value — it usually means a dropped path segment
// ({{ jwt.app_metadata }} for {{ jwt.app_metadata.tenant_id }}), and binding
// fmt.Sprint's "map[…]"/"[…]" rendering would let _neq/_lt match essentially
// every row; the one legitimate structured shape, a bare-claim _in array, is
// unpacked by resolveInValues before its elements reach here. A json.Number
// (jwt.WithJSONNumber on claims, UseNumber on ingest payloads) binds in
// canonical decimal form, not the token's spelling: "1", "1.0", and "1e3" are
// one JSON value, and a numeric ClickHouse column rejects '1.0'/'1e3' as a
// per-query TYPE_MISMATCH error. The canonical form is exact at every width
// and precision — integer literals via big.Int, fractions and exponents via
// canonicalDecimal, never a float64 round-trip that could bind a value the
// token doesn't carry ("1e-400" fails closed rather than collapsing to "0").
// A literal, or an exact form, past maxCanonicalDigits likewise has no
// canonical form and fails closed (1e400, 1e-400). Read-filter claim
// resolution and insert-check claim resolution both route through this one
// function, so what a read filter binds and what a write check requires can't
// drift.
func CanonicalScalar(v any) (string, bool) {
	switch val := v.(type) {
	case nil, map[string]any, []any:
		return "", false
	case json.Number:
		// The digit bound guards the superlinear big.Int parse on the
		// client-controlled ingest path. It counts digits, not bytes — a sign,
		// point, or exponent marker doesn't feed the big parse — so "-1e99"
		// binds exactly like its written-out form, matching the "one JSON
		// value" contract below (only at the bound's very edge can the exact-form
		// gate's slack separate two spellings). canonicalDecimal re-checks its
		// own exact-form length, where a short literal can hide a wide expansion.
		digits := 0
		for i := 0; i < len(val); i++ {
			if val[i] >= '0' && val[i] <= '9' {
				digits++
			}
		}
		if digits > maxCanonicalDigits {
			return "", false
		}
		if i, ok := new(big.Int).SetString(val.String(), 10); ok {
			return i.String(), true
		}
		return canonicalDecimal(val.String())
	case float64:
		// Production claims arrive as json.Number (jwt.WithJSONNumber), but
		// Evaluate is an exported API and float64 is what a plain json.Unmarshal
		// hands any other caller. At or past 2^53 the decode has already
		// collapsed neighboring JSON integers onto one float — ANY digits
		// rendered for it could be another principal's ID — so refuse, in depth
		// (#381 review). Below that, render positionally: fmt.Sprint's exponent
		// form ("1e+06") is a spelling numeric ClickHouse columns reject.
		if math.Abs(val) >= 1<<53 {
			return "", false
		}
		return strconv.FormatFloat(val, 'f', -1, 64), true
	default:
		return fmt.Sprint(val), true
	}
}

// canonicalDecimal renders a non-integer JSON number literal (one carrying a
// fraction or exponent) as its exact canonical decimal string: "1.0" → "1",
// "2.50" → "2.5", "1e3" → "1000", "25e-4" → "0.0025", every digit preserved
// at any precision. ok=false for anything that is not a JSON number and for
// any literal whose exact decimal form would exceed maxCanonicalDigits.
// Exactness is the point: rounding through float64 would collapse "1e-400"
// to "0" and land wide decimals on their neighbors, either way binding a
// value the token doesn't carry.
func canonicalDecimal(lit string) (string, bool) {
	mant, expStr, hasExp := strings.Cut(lit, "e")
	if !hasExp {
		mant, expStr, hasExp = strings.Cut(lit, "E")
	}
	exp := 0
	if hasExp {
		var err error
		// Atoi rejects an empty or non-numeric exponent. The magnitude bound is
		// the allocation guard, not a redundancy: the exact-form length check at
		// the bottom runs only after strings.Repeat has already built the
		// string, so without this bound a 12-byte "1e1000000000" would allocate
		// a ~1 GiB expansion before being rejected.
		if exp, err = strconv.Atoi(expStr); err != nil || exp > 2*maxCanonicalDigits || exp < -2*maxCanonicalDigits {
			return "", false
		}
	}
	sign := ""
	if strings.HasPrefix(mant, "-") {
		sign, mant = "-", mant[1:]
	}
	intPart, fracPart, _ := strings.Cut(mant, ".")
	digits := intPart + fracPart
	if digits == "" {
		return "", false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return "", false
		}
	}
	// point is where the decimal point sits in digits once the exponent is
	// applied: digits[:point] to its left, digits[point:] to its right.
	point := len(intPart) + exp
	for len(digits) > 0 && digits[0] == '0' {
		digits = digits[1:]
		point--
	}
	for len(digits) > 0 && len(digits) > point && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
	}
	if digits == "" {
		// Every spelling of zero ("0.0", "-0e9") is the one value 0 —
		// signless, since a numeric column can't parse "-0".
		return "0", true
	}
	var out string
	switch {
	case point <= 0:
		out = "0." + strings.Repeat("0", -point) + digits
	case point >= len(digits):
		out = digits + strings.Repeat("0", point-len(digits))
	default:
		out = digits[:point] + "." + digits[point:]
	}
	// The exact form is what must stay small: "1e-150" is a six-character
	// literal with a 152-character expansion. The +2 slack exists for a "0."
	// prefix, though any form may spend it (a 102-digit integer expansion
	// passes). This runs after the Repeat above, so it bounds what callers
	// see — the exponent bound is what keeps the allocation itself small.
	if len(out) > maxCanonicalDigits+2 {
		return "", false
	}
	return sign + out, true
}
