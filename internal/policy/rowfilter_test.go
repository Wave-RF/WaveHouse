package policy

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// intNumeric is the Int64 numeric spec most tests compare under — exact within
// the width's range, the storage model of the default integer id column. Float
// and Decimal narrowing, other widths, and range gating have dedicated tests.
func intNumeric() ColumnSpec {
	return ColumnSpec{Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericInteger, Bits: 64}}
}

// evalRowFilter builds a one-role/one-table policy carrying filter and returns the
// permissions resolved against claims, so tests exercise the full
// resolvePredicates → RowVisible path the stream fan-out uses.
func evalRowFilter(t *testing.T, filter map[string]Filter, claims map[string]any) *ResolvedPermissions {
	t.Helper()
	p := &Policy{Tables: map[string]TablePolicy{
		"t": {Select: map[string]RolePermissions{"r": {Filter: filter}}},
	}}
	return Evaluate(p, "r", "t", "select", claims)
}

// TestRowVisible_EqualityScoping covers the canonical row-level-security shape —
// tenant_id = {{ jwt.tenant }} — including the fail-closed on a missing column that
// stops an event without the filtered field from slipping through.
func TestRowVisible_EqualityScoping(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}, map[string]any{"tenant": "acme"})
	is := assert.New(t)
	is.True(perms.HasRowFilter())
	is.True(perms.RowVisible(map[string]any{"tenant_id": "acme"}, nil))
	is.False(perms.RowVisible(map[string]any{"tenant_id": "globex"}, nil))
	is.False(perms.RowVisible(map[string]any{"other": "x"}, nil), "missing filtered column ⇒ fail closed")
}

// TestRowVisible_InSet covers _in against an array claim (multi-tenant scoping) and
// the fail-closed empty-set case (absent claim never widens to all rows).
func TestRowVisible_InSet(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"tenant_id": {In: new("{{ jwt.tenants }}")}}, map[string]any{"tenants": []any{"a", "b"}})
	assert.True(t, perms.RowVisible(map[string]any{"tenant_id": "b"}, nil))
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "c"}, nil))

	empty := evalRowFilter(t, map[string]Filter{"tenant_id": {In: new("{{ jwt.tenants }}")}}, nil)
	assert.True(t, empty.HasRowFilter())
	assert.False(t, empty.RowVisible(map[string]any{"tenant_id": "a"}, nil), "absent claim ⇒ empty set ⇒ no row matches")
}

// TestRowVisible_Neq: != admits a row only when inequality is PROVABLE — a numeric
// column comparing numerically or a String column comparing bytewise. On a column
// with no usable type (no schema, or a type like UUID/Bool whose text rendering
// isn't canonical) a byte difference may be pure representation — 'ABC-def' vs
// 'abc-def' for a UUID ClickHouse would treat as equal — so != fails closed rather
// than admit a row the query path excludes.
func TestRowVisible_Neq(t *testing.T) {
	t.Parallel()
	text := map[string]ColumnSpec{"status": {Kind: ColumnText}}
	perms := evalRowFilter(t, map[string]Filter{"status": {Neq: new("deleted")}}, nil)
	assert.True(t, perms.RowVisible(map[string]any{"status": "active"}, text), "String column: byte inequality is real inequality")
	assert.False(t, perms.RowVisible(map[string]any{"status": "deleted"}, text))

	assert.False(t, perms.RowVisible(map[string]any{"status": "active"}, nil),
		"no schema: byte inequality proves nothing, fail closed")

	uuid := evalRowFilter(t, map[string]Filter{"device": {Neq: new("ABC-DEF")}}, nil)
	assert.False(t, uuid.RowVisible(map[string]any{"device": "abc-def"}, map[string]ColumnSpec{"device": {Kind: ColumnOpaque}}),
		"opaque column (e.g. UUID): a case difference is not proof of inequality — fail closed")

	num := evalRowFilter(t, map[string]Filter{"amount": {Neq: new("100")}}, nil)
	kinds := map[string]ColumnSpec{"amount": intNumeric()}
	assert.True(t, num.RowVisible(map[string]any{"amount": float64(250)}, kinds), "numeric column: 250 ≠ 100 is provable")
	assert.False(t, num.RowVisible(map[string]any{"amount": float64(100)}, kinds))
}

// TestRowVisible_Ordering_SchemaInformed: ordering needs type knowledge. An event
// value of 9 is numerically LESS than 100 but lexicographically GREATER ("9" >
// "100") — so a numeric column compares numerically (matching ClickHouse), a String
// column compares bytewise (which IS ClickHouse's String order), and a column with
// no usable type fails closed: no schema, a column the schema doesn't know, and a
// non-numeric non-String type (Enum order follows enum values, not names; Date
// formats vary) must all withhold rather than fall back to a text comparison that
// can admit rows the query path excludes.
func TestRowVisible_Ordering_SchemaInformed(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"amount": {Gt: new("100")}}, nil)
	small := map[string]any{"amount": float64(9)}
	big := map[string]any{"amount": float64(250)}
	numeric := map[string]ColumnSpec{"amount": intNumeric()}

	assert.False(t, perms.RowVisible(small, numeric), "numeric: 9 is not > 100")
	assert.True(t, perms.RowVisible(big, numeric))

	assert.False(t, perms.RowVisible(small, nil), `no schema: fail closed — never the "9" > "100" text leak`)
	assert.False(t, perms.RowVisible(big, nil), "no schema: fail closed even when the numbers would pass")
	assert.False(t, perms.RowVisible(small, map[string]ColumnSpec{"other": intNumeric()}),
		"column absent from a known schema: fail closed")
	assert.False(t, perms.RowVisible(small, map[string]ColumnSpec{"amount": {Kind: ColumnOpaque}}),
		"opaque type (Enum/Date/UUID/…): order is unprovable, fail closed")

	// String columns order bytewise in ClickHouse, so ordering there is exact.
	page := evalRowFilter(t, map[string]Filter{"page": {Gt: new("/m")}}, nil)
	text := map[string]ColumnSpec{"page": {Kind: ColumnText}}
	assert.True(t, page.RowVisible(map[string]any{"page": "/z"}, text))
	assert.False(t, page.RowVisible(map[string]any{"page": "/a"}, text))
}

// TestRowVisible_NumericEquality_FloatFormatting: a JSON float64(100) equals the
// string filter value "100" under numeric comparison, so integer-valued numeric
// columns aren't tripped up by float formatting.
func TestRowVisible_NumericEquality_FloatFormatting(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("100")}}, nil)
	num := map[string]ColumnSpec{"amount": intNumeric()}
	assert.True(t, perms.RowVisible(map[string]any{"amount": float64(100)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"amount": float64(101)}, num))
}

// TestRowVisible_NaN_FailsClosed: strconv.ParseFloat accepts "NaN", and NaN's
// three-way comparison would otherwise read as "equal to everything" — a fail-open
// that delivers a row the query path's WHERE excludes. Either operand parsing to
// NaN must withhold the row instead.
func TestRowVisible_NaN_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"amount": intNumeric()}

	byRow := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("100")}}, nil)
	assert.False(t, byRow.RowVisible(map[string]any{"amount": "NaN"}, num), "NaN row value must not equal 100")

	byClaim := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("{{ jwt.cap }}")}}, map[string]any{"cap": "NaN"})
	assert.False(t, byClaim.RowVisible(map[string]any{"amount": float64(100)}, num), "NaN claim must not match any row")

	neq := evalRowFilter(t, map[string]Filter{"amount": {Neq: new("NaN")}}, nil)
	assert.False(t, neq.RowVisible(map[string]any{"amount": float64(100)}, num), "NaN is uncomparable, so even != fails closed")
}

// TestRowVisible_Inf_FailsClosed: ParseFloat also accepts "Inf"/"+Inf"/"-Inf"/
// "Infinity" (any case), and an infinite bound would make _gt/_lt admit every
// finite row — a fail-open the query path can't reproduce (ClickHouse rejects
// binding an Inf-spelled string to an integer column). Either operand parsing
// to ±Inf must withhold the row instead.
func TestRowVisible_Inf_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"amount": intNumeric()}

	byClaim := evalRowFilter(t, map[string]Filter{"amount": {Gt: new("{{ jwt.min }}")}}, map[string]any{"min": "-Inf"})
	assert.False(t, byClaim.RowVisible(map[string]any{"amount": float64(5)}, num), "-Inf lower bound must not admit finite rows")

	lt := evalRowFilter(t, map[string]Filter{"amount": {Lt: new("Infinity")}}, nil)
	assert.False(t, lt.RowVisible(map[string]any{"amount": float64(5)}, num), "Infinity upper bound must not admit finite rows")

	byRow := evalRowFilter(t, map[string]Filter{"amount": {Gt: new("100")}}, nil)
	assert.False(t, byRow.RowVisible(map[string]any{"amount": "+Inf"}, num), "Inf row value is uncomparable, fail closed")
}

// TestRowVisible_NumericEquality_ExactBeyondFloat64: ingest accepts string-encoded
// numerics precisely so 64-bit IDs survive JS precision loss; equality must not
// collapse distinct IDs that round to the same float64 (adjacent values past 2^53).
func TestRowVisible_NumericEquality_ExactBeyondFloat64(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"id": intNumeric()}

	perms := evalRowFilter(t, map[string]Filter{"id": {Eq: new("9007199254740993")}}, nil)
	assert.False(t, perms.RowVisible(map[string]any{"id": "9007199254740992"}, num), "float64-equal neighbors are not equal")
	assert.True(t, perms.RowVisible(map[string]any{"id": "9007199254740993"}, num))
	assert.True(t, perms.RowVisible(map[string]any{"id": "9007199254740993.0"}, num), "same value in a different rendering still matches")

	// Bare JSON numbers reach RowVisible as json.Number (the stream decodes with
	// UseNumber), so the same exactness holds without string-encoding: a lossy
	// float64 decode would have collapsed these neighbors and delivered another
	// tenant's row.
	assert.False(t, perms.RowVisible(map[string]any{"id": json.Number("9007199254740992")}, num), "json.Number neighbor is not equal")
	assert.True(t, perms.RowVisible(map[string]any{"id": json.Number("9007199254740993")}, num))

	neq := evalRowFilter(t, map[string]Filter{"id": {Neq: new("9007199254740993")}}, nil)
	assert.True(t, neq.RowVisible(map[string]any{"id": "9007199254740992"}, num), "the exact comparison keeps distinct IDs unequal for !=")
}

// TestRowVisible_OverlongNumericOperand_FailsClosed: an over-long operand is
// refused by the O(1) length pre-gate (maxNumericOperandChars) before ANY scan
// of its bytes — the row operand is client-controlled and the comparison runs
// per subscriber per event on the fan-out goroutine, so even a linear
// json.Valid pass over it is a cost an attacker controls. The gate is
// verdict-preserving: anything longer already fails the canonical digit bound.
// Withheld, never read; real-width values unaffected.
func TestRowVisible_OverlongNumericOperand_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"amount": intNumeric()}
	perms := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("100")}}, nil)

	long := "100." + strings.Repeat("0", 200_000) + "1" // far past the operand length gate
	assert.False(t, perms.RowVisible(map[string]any{"amount": json.Number(long)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"amount": long}, num), "string-encoded operand is bounded too")
	assert.True(t, perms.RowVisible(map[string]any{"amount": json.Number("100.0")}, num), "real-width values still compare")
}

// TestRowVisible_LossyFloatClaim_FailsClosed: a claim that arrives as a float64
// at or past 2^53 has already collapsed onto its float neighbors — rendering
// digits for it could name another tenant (the fail-open caught in #381 review:
// the claim rendered as "1e+16" and matched the neighbor's rows). CanonicalScalar
// now refuses such a float64 outright, so the predicate matches NOTHING: not the
// neighbor the float equals, and not even the row whose exact ID the claim
// originally carried — availability, never confidentiality.
func TestRowVisible_LossyFloatClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"tenant_id": intNumeric()}
	filter := map[string]Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}

	lossy := evalRowFilter(t, filter, map[string]any{"tenant": float64(10000000000000001)}) // already 1e16
	assert.False(t, lossy.RowVisible(map[string]any{"tenant_id": json.Number("10000000000000000")}, num),
		"the float64-equal neighbor tenant's rows must not be delivered")
	assert.False(t, lossy.RowVisible(map[string]any{"tenant_id": json.Number("10000000000000001")}, num),
		"the original tenant's own rows are withheld too — the digits are unrecoverable")

	// The exact-digit path — a json.Number claim, which is what jwt.Parse yields
	// since WithJSONNumber — scopes to precisely one tenant.
	exact := evalRowFilter(t, filter, map[string]any{"tenant": json.Number("10000000000000001")})
	assert.True(t, exact.RowVisible(map[string]any{"tenant_id": json.Number("10000000000000001")}, num))
	assert.False(t, exact.RowVisible(map[string]any{"tenant_id": json.Number("10000000000000000")}, num))
}

// rfc3339Spec is a ColumnTime spec whose parser reads RFC 3339 strings — a
// hermetic stand-in for discovery's grammar, which is exercised in
// internal/discovery (Column.TimeParser) and end-to-end in the hub tests.
func rfc3339Spec() ColumnSpec {
	return ColumnSpec{Kind: ColumnTime, ParseTime: func(v any) (time.Time, bool) {
		s, ok := v.(string)
		if !ok {
			return time.Time{}, false
		}
		ts, err := time.Parse(time.RFC3339, s)
		return ts, err == nil
	}}
}

// TestRowVisible_TimeColumn: DateTime/DateTime64 operands compare as instants,
// so equality holds across spellings of the same instant, ordering works (the
// time-window policy shape), and any operand the parser refuses — junk on either
// side, or a ColumnTime spec missing its parser — withholds the row.
func TestRowVisible_TimeColumn(t *testing.T) {
	t.Parallel()
	cols := map[string]ColumnSpec{"created_at": rfc3339Spec()}

	eq := evalRowFilter(t, map[string]Filter{"created_at": {Eq: new("2026-06-21T06:00:00+02:00")}}, nil)
	assert.True(t, eq.RowVisible(map[string]any{"created_at": "2026-06-21T04:00:00Z"}, cols),
		"different spelling, same instant ⇒ equal")
	assert.False(t, eq.RowVisible(map[string]any{"created_at": "2026-06-21T04:00:01Z"}, cols))
	assert.False(t, eq.RowVisible(map[string]any{"created_at": "junk"}, cols), "unparseable payload withholds")

	gt := evalRowFilter(t, map[string]Filter{"created_at": {Gt: new("2026-06-21T00:00:00Z")}}, nil)
	assert.True(t, gt.RowVisible(map[string]any{"created_at": "2026-06-21T04:00:00Z"}, cols))
	assert.False(t, gt.RowVisible(map[string]any{"created_at": "2026-06-20T04:00:00Z"}, cols))

	bad := evalRowFilter(t, map[string]Filter{"created_at": {Eq: new("not-a-time")}}, nil)
	assert.False(t, bad.RowVisible(map[string]any{"created_at": "2026-06-21T04:00:00Z"}, cols),
		"unparseable constant withholds")

	noParser := map[string]ColumnSpec{"created_at": {Kind: ColumnTime}}
	assert.False(t, eq.RowVisible(map[string]any{"created_at": "2026-06-21T04:00:00Z"}, noParser),
		"ColumnTime without a parser refuses the comparison — never a text fallback")
}

// TestRowVisible_TimeColumn_OverlongOperand_Gated: the O(1) length gate refuses
// an over-long timestamp operand — string OR json.Number (a Unix-epoch payload
// shape) — BEFORE the parser scans it, so a client-controlled megabyte "value"
// can't stall the per-subscriber fan-out. The parser here counts every call, so
// a gated operand must produce zero calls.
func TestRowVisible_TimeColumn_OverlongOperand_Gated(t *testing.T) {
	t.Parallel()
	var calls int
	counting := ColumnSpec{Kind: ColumnTime, ParseTime: func(v any) (time.Time, bool) {
		calls++
		return time.Time{}, false
	}}
	cols := map[string]ColumnSpec{"created_at": counting}
	perms := evalRowFilter(t, map[string]Filter{"created_at": {Eq: new("2026-06-21T04:00:00Z")}}, nil)

	huge := strings.Repeat("9", maxTimeOperandChars+1)
	assert.False(t, perms.RowVisible(map[string]any{"created_at": huge}, cols))
	assert.False(t, perms.RowVisible(map[string]any{"created_at": json.Number(huge)}, cols))
	assert.Zero(t, calls, "an over-long operand must be refused before the parser is called")
}

func TestRowVisible_NilReceiver_AllVisible(t *testing.T) {
	t.Parallel()
	var perms *ResolvedPermissions
	assert.True(t, perms.RowVisible(map[string]any{"x": "y"}, nil))
	assert.False(t, perms.HasRowFilter())
}

// TestRowVisible_NonScalar_FailsClosed: a nested/array/null event value can't be
// compared to a scalar filter value, so the predicate fails closed rather than guess.
func TestRowVisible_NonScalar_FailsClosed(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"tenant_id": {Eq: new("acme")}}, nil)
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": []any{"acme"}}, nil))
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": nil}, nil))
}

// TestRowVisible_MultiplePredicates_AllMustPass: predicates are ANDed, matching the
// query path's "AND"-joined WHERE clause.
func TestRowVisible_MultiplePredicates_AllMustPass(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{
		"tenant_id": {Eq: new("{{ jwt.tenant }}")},
		"amount":    {Gt: new("100")},
	}, map[string]any{"tenant": "acme"})
	num := map[string]ColumnSpec{"amount": intNumeric()}
	assert.True(t, perms.RowVisible(map[string]any{"tenant_id": "acme", "amount": float64(250)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "acme", "amount": float64(9)}, num), "amount fails")
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "globex", "amount": float64(250)}, num), "tenant fails")
}

// TestRowFilter_UnresolvableClaim_NoRowsOnBothPaths pins the #457 fail-closed
// rule on BOTH read surfaces at once: a filter template whose claim the token
// doesn't carry renders the constant-false predicate on the query path AND
// withholds every row in the stream's in-memory evaluation. One Evaluate
// resolution drives both, so a claim-less token can never see zero rows on
// /v1/query yet every row on /v1/stream. HasRowFilter must stay true for the
// failed predicate — dropping it would put the role back on the unfiltered
// once-per-role fast path, the exact fail-open this test exists to prevent.
func TestRowFilter_UnresolvableClaim_NoRowsOnBothPaths(t *testing.T) {
	t.Parallel()
	noTenant := map[string]any{"role": "user"} // validly signed token, no tenant claim
	tests := []struct {
		name   string
		filter map[string]Filter
		claims map[string]any
	}{
		{"_eq", map[string]Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}, noTenant},
		{"_neq, the leak direction", map[string]Filter{"tenant_id": {Neq: new("{{ jwt.tenant }}")}}, noTenant},
		{"_gt", map[string]Filter{"tenant_id": {Gt: new("{{ jwt.tenant }}")}}, noTenant},
		{"_in with surrounding text", map[string]Filter{"tenant_id": {In: new("t-{{ jwt.tenant }}")}}, noTenant},
		{
			"object claim in a scalar slot",
			map[string]Filter{"tenant_id": {Eq: new("{{ jwt.meta }}")}},
			map[string]any{"meta": map[string]any{"tenant": "acme"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			perms := evalRowFilter(t, tt.filter, tt.claims)
			assert.Equal(t, "1 = 0", perms.WhereClause, "query path: constant-false predicate")
			assert.Empty(t, perms.WhereParams)
			assert.True(t, perms.HasRowFilter(), "failed predicate must keep the stream on the per-subscriber path")
			assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "acme"}, nil), "stream path: every row withheld")
		})
	}
}

// TestRowVisible_FloatNarrowing: Float32/Float64 columns compare in the
// column's float domain — BOTH operands narrowed, exactly as ClickHouse
// narrows the stored value at insert and the bound constant at compare. The
// Float32 case is the #381 review repro: payload 16777217 stores as 16777216,
// so `_gt: "16777216"` must NOT admit the event (the SQL predicate over the
// stored row is false), while equality against the same spelling matches on
// both surfaces because the constant narrows too. The integer family, by
// contrast, keeps such neighbors distinct.
func TestRowVisible_FloatNarrowing(t *testing.T) {
	t.Parallel()
	f32 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericFloat, Bits: 32}}}
	f64 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericFloat, Bits: 64}}}
	intCol := map[string]ColumnSpec{"v": intNumeric()}

	gt := evalRowFilter(t, map[string]Filter{"v": {Gt: new("16777216")}}, nil)
	assert.False(t, gt.RowVisible(map[string]any{"v": json.Number("16777217")}, f32),
		"stored Float32(16777217) is 16777216, not > 16777216 — the ordering fail-open, closed")
	assert.True(t, gt.RowVisible(map[string]any{"v": json.Number("16777218")}, f32),
		"16777218 is Float32-representable and greater on both surfaces")
	assert.True(t, gt.RowVisible(map[string]any{"v": json.Number("16777217")}, intCol),
		"an integer column stores 16777217 exactly, so the same event IS greater there")

	eq := evalRowFilter(t, map[string]Filter{"v": {Eq: new("16777217")}}, nil)
	assert.True(t, eq.RowVisible(map[string]any{"v": json.Number("16777217")}, f32),
		"the constant narrows like the stored value — ClickHouse matches `= '16777217'` too")
	assert.True(t, eq.RowVisible(map[string]any{"v": json.Number("16777216")}, f32),
		"the Float32-equal neighbor matches on both surfaces — the column type gave that distinction away")
	assert.False(t, eq.RowVisible(map[string]any{"v": json.Number("16777216")}, intCol),
		"integer storage keeps the neighbors distinct")

	eq64 := evalRowFilter(t, map[string]Filter{"v": {Eq: new("9007199254740993")}}, nil)
	assert.True(t, eq64.RowVisible(map[string]any{"v": json.Number("9007199254740992")}, f64),
		"Float64 column: 2^53 neighbors collapse in the storage domain, matching SQL")
	assert.False(t, eq64.RowVisible(map[string]any{"v": json.Number("9007199254740992")}, intCol))

	// A magnitude the float domain can't hold (ClickHouse would store ±Inf)
	// refuses the comparison — withheld, never a guessed verdict.
	overflow := evalRowFilter(t, map[string]Filter{"v": {Gt: new("0")}}, nil)
	assert.False(t, overflow.RowVisible(map[string]any{"v": json.Number("1e39")}, f32),
		"beyond Float32 range ⇒ withhold")
}

// TestRowVisible_DecimalScaleTruncation: Decimal columns compare after
// truncating BOTH operands to the column's scale — ClickHouse's cast semantics
// (1.005, 1.006 and 1.009 all store as 1.00 in a Decimal(10,2), and a bound
// constant '1.005' truncates the same way, so `= '1.005'` matches a stored
// 1.00 while `> '1.004'` matches nothing; verified on 25.5 and 26.6).
func TestRowVisible_DecimalScaleTruncation(t *testing.T) {
	t.Parallel()
	dec2 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericDecimal, Precision: 10, Scale: 2}}}

	gt := evalRowFilter(t, map[string]Filter{"v": {Gt: new("1.004")}}, nil)
	assert.False(t, gt.RowVisible(map[string]any{"v": json.Number("1.005")}, dec2),
		"stored 1.00 vs constant 1.00: not greater — the pre-narrowing payload must not leak through")
	assert.True(t, gt.RowVisible(map[string]any{"v": json.Number("1.02")}, dec2))

	eq := evalRowFilter(t, map[string]Filter{"v": {Eq: new("1.005")}}, nil)
	assert.True(t, eq.RowVisible(map[string]any{"v": json.Number("1.006")}, dec2),
		"both operands truncate to 1.00 — ClickHouse matches this pair too")
	assert.False(t, eq.RowVisible(map[string]any{"v": json.Number("1.02")}, dec2))

	lt := evalRowFilter(t, map[string]Filter{"v": {Lt: new("-1")}}, nil)
	assert.False(t, lt.RowVisible(map[string]any{"v": json.Number("-1.005")}, dec2),
		"truncation is toward zero: -1.005 stores as -1.00, which is not < -1")
}

// TestRowVisible_IntegerFractionalOperand_FailsClosed: an integer column
// refuses fractional operands on either side — a fractional constant is a
// per-query type error on the SQL path (the role reads no rows there), and a
// fractional payload was never storable in the column — so the stream
// withholds rather than inventing a verdict SQL cannot produce. An integral
// value in fractional SPELLING is a different thing entirely: it
// canonicalizes to its integer and compares normally.
func TestRowVisible_IntegerFractionalOperand_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"v": intNumeric()}

	frac := evalRowFilter(t, map[string]Filter{"v": {Neq: new("1.5")}}, nil)
	assert.False(t, frac.RowVisible(map[string]any{"v": json.Number("2")}, num),
		"fractional constant: SQL errors the query, the stream withholds — neither returns rows")

	pay := evalRowFilter(t, map[string]Filter{"v": {Gt: new("1")}}, nil)
	assert.False(t, pay.RowVisible(map[string]any{"v": json.Number("2.5")}, num),
		"fractional payload was never storable in an integer column")
	assert.True(t, pay.RowVisible(map[string]any{"v": json.Number("2.0")}, num),
		"integral value in fractional spelling canonicalizes to 2 and compares")
}

// TestRowVisible_ConstantSpellingFidelity: on an integer column a literal
// constant that is not its own canonical form ("1e3", "1.5") refuses the
// comparison — the SQL path binds the spelling as written and ClickHouse's
// integer cast errors the query there, so the role reads no rows; admitting
// the canonical reading here would deliver rows SQL never returns. Float and
// Decimal casts accept every JSON-number spelling ('1e3' casts to
// Decimal(10,2) as 1000 — verified against ClickHouse), so those families
// compare such constants normally.
func TestRowVisible_ConstantSpellingFidelity(t *testing.T) {
	t.Parallel()
	intCol := map[string]ColumnSpec{"v": intNumeric()}
	dec2 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericDecimal, Precision: 10, Scale: 2}}}
	f32 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericFloat, Bits: 32}}}

	exp := evalRowFilter(t, map[string]Filter{"v": {Eq: new("1e3")}}, nil)
	assert.False(t, exp.RowVisible(map[string]any{"v": json.Number("1000")}, intCol),
		"integer column: SQL errors on '1e3', so the stream must not admit its canonical reading")
	assert.True(t, exp.RowVisible(map[string]any{"v": json.Number("1000")}, f32),
		"float column: ClickHouse casts '1e3' fine, so the stream compares it")
	assert.True(t, exp.RowVisible(map[string]any{"v": json.Number("1000")}, dec2),
		"Decimal column: ClickHouse casts '1e3' fine too, so the stream compares it")

	trailing := evalRowFilter(t, map[string]Filter{"v": {Gt: new("1.50")}}, nil)
	assert.True(t, trailing.RowVisible(map[string]any{"v": json.Number("2")}, dec2),
		"a trailing-zero Decimal spelling casts on both surfaces and compares by value")
}

// TestRowVisible_OutOfRangeOperand_FailsClosed: an operand outside the
// column's numeric range refuses the comparison on either side. ClickHouse's
// reading of such a CONSTANT was measured to vary by pair on one release —
// error (negative vs unsigned: the role reads no rows), mathematical promotion
// ('256' vs UInt8), or a width-boundary wrap that compares against a DIFFERENT
// value than written (2^63 vs Int64 reads as −2^63, where exact-precision
// comparison would admit the −2^63 rows SQL hides under !=) — so no single
// model is safe to reproduce, and refusal is. A PAYLOAD out of range was never
// storable (the insert is rejected), so withholding matches the stored world.
func TestRowVisible_OutOfRangeOperand_FailsClosed(t *testing.T) {
	t.Parallel()
	u64 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericInteger, Bits: 64, Unsigned: true}}}
	u8 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericInteger, Bits: 8, Unsigned: true}}}
	i8 := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericInteger, Bits: 8}}}
	dec := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericDecimal, Precision: 10, Scale: 2}}}

	neg := evalRowFilter(t, map[string]Filter{"v": {Gt: new("-5")}}, nil)
	assert.False(t, neg.RowVisible(map[string]any{"v": json.Number("7")}, u64),
		"negative constant on an unsigned column: SQL errors, the stream must not admit everything")

	wrap := evalRowFilter(t, map[string]Filter{"v": {Neq: new("18446744073709551616")}}, nil)
	assert.False(t, wrap.RowVisible(map[string]any{"v": json.Number("7")}, u64),
		"a width-boundary constant may wrap on the SQL side — comparing it as written risks admitting rows SQL hides")

	wide := evalRowFilter(t, map[string]Filter{"v": {Lt: new("99999999999999999999999")}}, nil)
	assert.False(t, wide.RowVisible(map[string]any{"v": json.Number("7")}, u64),
		"a constant past the width has no reliable SQL reading; the stream withholds")

	over8 := evalRowFilter(t, map[string]Filter{"v": {Neq: new("256")}}, nil)
	assert.False(t, over8.RowVisible(map[string]any{"v": json.Number("7")}, u8))
	assert.False(t, over8.RowVisible(map[string]any{"v": json.Number("300")}, u8),
		"an out-of-range payload was never storable — withheld")
	ok8 := evalRowFilter(t, map[string]Filter{"v": {Neq: new("254")}}, nil)
	assert.True(t, ok8.RowVisible(map[string]any{"v": json.Number("7")}, u8), "in-range operands still compare")

	i8lo := evalRowFilter(t, map[string]Filter{"v": {Gt: new("-129")}}, nil)
	assert.False(t, i8lo.RowVisible(map[string]any{"v": json.Number("0")}, i8))
	i8ok := evalRowFilter(t, map[string]Filter{"v": {Gt: new("-128")}}, nil)
	assert.True(t, i8ok.RowVisible(map[string]any{"v": json.Number("0")}, i8), "the signed minimum itself is in range")

	prec := evalRowFilter(t, map[string]Filter{"v": {Eq: new("999999999")}}, nil)
	assert.False(t, prec.RowVisible(map[string]any{"v": json.Number("5")}, dec),
		"9 integer digits exceed Decimal(10,2)'s 8-digit budget: unmodelable on the SQL side, withheld here")
	precOK := evalRowFilter(t, map[string]Filter{"v": {Eq: new("99999999")}}, nil)
	assert.True(t, precOK.RowVisible(map[string]any{"v": json.Number("99999999")}, dec), "the budget's edge is in range")

	// A hand-built spec with an incoherent Scale must degrade to refusal —
	// never a panic on the fan-out goroutine (truncateScale slices by Scale).
	badScale := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericDecimal, Precision: 10, Scale: -1}}}
	assert.NotPanics(t, func() {
		assert.False(t, precOK.RowVisible(map[string]any{"v": json.Number("1.5")}, badScale))
	})
}

// TestRowVisible_NumericWithoutStorageModel_FailsClosed pins the NumericSpec
// zero value: a ColumnNumeric spec carrying no storage model must refuse every
// comparison, so a numeric type the classifier doesn't recognize (or a caller
// that forgot to set the model) can never compare under guessed semantics.
func TestRowVisible_NumericWithoutStorageModel_FailsClosed(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"v": {Eq: new("1")}}, nil)

	unmodeled := map[string]ColumnSpec{"v": {Kind: ColumnNumeric}}
	assert.False(t, perms.RowVisible(map[string]any{"v": json.Number("1")}, unmodeled))

	// A float family with no (or a bogus) bit width must refuse too:
	// ParseFloat would silently treat any other bitSize as 64 and compare a
	// Float32 column in the wider domain — the fail-open direction.
	widthless := map[string]ColumnSpec{"v": {Kind: ColumnNumeric, Numeric: NumericSpec{Family: NumericFloat}}}
	assert.False(t, perms.RowVisible(map[string]any{"v": json.Number("1")}, widthless))
}
