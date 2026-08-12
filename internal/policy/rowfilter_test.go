package policy

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
	kinds := map[string]ColumnSpec{"amount": {Kind: ColumnNumeric}}
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
	numeric := map[string]ColumnSpec{"amount": {Kind: ColumnNumeric}}

	assert.False(t, perms.RowVisible(small, numeric), "numeric: 9 is not > 100")
	assert.True(t, perms.RowVisible(big, numeric))

	assert.False(t, perms.RowVisible(small, nil), `no schema: fail closed — never the "9" > "100" text leak`)
	assert.False(t, perms.RowVisible(big, nil), "no schema: fail closed even when the numbers would pass")
	assert.False(t, perms.RowVisible(small, map[string]ColumnSpec{"other": {Kind: ColumnNumeric}}),
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
	num := map[string]ColumnSpec{"amount": {Kind: ColumnNumeric}}
	assert.True(t, perms.RowVisible(map[string]any{"amount": float64(100)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"amount": float64(101)}, num))
}

// TestRowVisible_NaN_FailsClosed: strconv.ParseFloat accepts "NaN", and NaN's
// three-way comparison would otherwise read as "equal to everything" — a fail-open
// that delivers a row the query path's WHERE excludes. Either operand parsing to
// NaN must withhold the row instead.
func TestRowVisible_NaN_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"amount": {Kind: ColumnNumeric}}

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
	num := map[string]ColumnSpec{"amount": {Kind: ColumnNumeric}}

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
	num := map[string]ColumnSpec{"id": {Kind: ColumnNumeric}}

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
	assert.True(t, neq.RowVisible(map[string]any{"id": "9007199254740992"}, num), "the exact tie-break keeps distinct IDs unequal for !=")
}

// TestRowVisible_LossyFloatClaim_FailsClosed: a claim that arrives as a float64
// at or past 2^53 has already collapsed onto its float neighbors — rendering
// digits for it could name another tenant (the fail-open caught in #381 review:
// fmt.Sprint turned the claim into "1e+16", which compareExact then matched
// against the neighbor's exact digits). The predicate must match NOTHING: not
// the neighbor the float equals, and not even the row whose exact ID the claim
// originally carried — availability, never confidentiality.
func TestRowVisible_LossyFloatClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]ColumnSpec{"tenant_id": {Kind: ColumnNumeric}}
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
	num := map[string]ColumnSpec{"amount": {Kind: ColumnNumeric}}
	assert.True(t, perms.RowVisible(map[string]any{"tenant_id": "acme", "amount": float64(250)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "acme", "amount": float64(9)}, num), "amount fails")
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "globex", "amount": float64(250)}, num), "tenant fails")
}
