package policy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestRowVisible_Neq(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"status": {Neq: new("deleted")}}, nil)
	assert.True(t, perms.RowVisible(map[string]any{"status": "active"}, nil))
	assert.False(t, perms.RowVisible(map[string]any{"status": "deleted"}, nil))
}

// TestRowVisible_Ordering_NumericVsLexicographic is the schema-informed behavior: an
// event value of 9 is numerically LESS than 100 but lexicographically GREATER
// ("9" > "100"). With the column marked numeric the row is hidden (matching
// ClickHouse); the no-schema lexicographic fallback would leak it — the footgun the
// schema closes.
func TestRowVisible_Ordering_NumericVsLexicographic(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"amount": {Gt: new("100")}}, nil)
	row := map[string]any{"amount": float64(9)}

	assert.False(t, perms.RowVisible(row, map[string]bool{"amount": true}), "numeric: 9 is not > 100")
	assert.True(t, perms.RowVisible(row, nil), `lexicographic fallback: "9" > "100"`)
	assert.True(t, perms.RowVisible(map[string]any{"amount": float64(250)}, map[string]bool{"amount": true}))
}

// TestRowVisible_NumericEquality_FloatFormatting: a JSON float64(100) equals the
// string filter value "100" under numeric comparison, so integer-valued numeric
// columns aren't tripped up by float formatting.
func TestRowVisible_NumericEquality_FloatFormatting(t *testing.T) {
	t.Parallel()
	perms := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("100")}}, nil)
	num := map[string]bool{"amount": true}
	assert.True(t, perms.RowVisible(map[string]any{"amount": float64(100)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"amount": float64(101)}, num))
}

// TestRowVisible_NaN_FailsClosed: strconv.ParseFloat accepts "NaN", and NaN's
// three-way comparison would otherwise read as "equal to everything" — a fail-open
// that delivers a row the query path's WHERE excludes. Either operand parsing to
// NaN must withhold the row instead.
func TestRowVisible_NaN_FailsClosed(t *testing.T) {
	t.Parallel()
	num := map[string]bool{"amount": true}

	byRow := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("100")}}, nil)
	assert.False(t, byRow.RowVisible(map[string]any{"amount": "NaN"}, num), "NaN row value must not equal 100")

	byClaim := evalRowFilter(t, map[string]Filter{"amount": {Eq: new("{{ jwt.cap }}")}}, map[string]any{"cap": "NaN"})
	assert.False(t, byClaim.RowVisible(map[string]any{"amount": float64(100)}, num), "NaN claim must not match any row")

	neq := evalRowFilter(t, map[string]Filter{"amount": {Neq: new("NaN")}}, nil)
	assert.False(t, neq.RowVisible(map[string]any{"amount": float64(100)}, num), "NaN is uncomparable, so even != fails closed")
}

// TestRowVisible_NumericEquality_ExactBeyondFloat64: ingest accepts string-encoded
// numerics precisely so 64-bit IDs survive JS precision loss; equality must not
// collapse distinct IDs that round to the same float64 (adjacent values past 2^53).
func TestRowVisible_NumericEquality_ExactBeyondFloat64(t *testing.T) {
	t.Parallel()
	num := map[string]bool{"id": true}

	perms := evalRowFilter(t, map[string]Filter{"id": {Eq: new("9007199254740993")}}, nil)
	assert.False(t, perms.RowVisible(map[string]any{"id": "9007199254740992"}, num), "float64-equal neighbors are not equal")
	assert.True(t, perms.RowVisible(map[string]any{"id": "9007199254740993"}, num))
	assert.True(t, perms.RowVisible(map[string]any{"id": "9007199254740993.0"}, num), "same value in a different rendering still matches")

	neq := evalRowFilter(t, map[string]Filter{"id": {Neq: new("9007199254740993")}}, nil)
	assert.True(t, neq.RowVisible(map[string]any{"id": "9007199254740992"}, num), "the exact tie-break keeps distinct IDs unequal for !=")
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
	num := map[string]bool{"amount": true}
	assert.True(t, perms.RowVisible(map[string]any{"tenant_id": "acme", "amount": float64(250)}, num))
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "acme", "amount": float64(9)}, num), "amount fails")
	assert.False(t, perms.RowVisible(map[string]any{"tenant_id": "globex", "amount": float64(250)}, num), "tenant fails")
}

// TestCompiledRowFilter_MatchesEvaluatePath pins the parity the compiled per-subscriber
// stream path depends on: for every (filter, claims, row) below, CompileRowFilter +
// RowVisible must return exactly what the query path (Evaluate → resolved predicates →
// matches) returns. Were the two to diverge, a subscriber could see rows the query path
// hides (or vice versa) — the
// row-level-security bug this path exists to prevent. The matrix exercises every operator,
// constant / whole-claim / mixed-template / space-padded / nested-path values, and
// missing columns, empty claim sets, and numeric-vs-lexicographic comparison.
func TestCompiledRowFilter_MatchesEvaluatePath(t *testing.T) {
	t.Parallel()
	filters := []map[string]Filter{
		{"tenant_id": {Eq: new("{{ jwt.tenant }}")}},
		{"tenant_id": {Neq: new("{{ jwt.tenant }}")}},
		{"amount": {Gt: new("100")}},
		{"amount": {Lt: new("{{ jwt.cap }}")}},
		{"status": {Eq: new("active")}},                                   // constant
		{"region": {In: new("{{ jwt.regions }}")}},                        // claim list
		{"tag": {In: new("{{ jwt.tag }}")}},                               // claim scalar
		{"kind": {In: new("published")}},                                  // constant in-set
		{"label": {Eq: new("v-{{ jwt.tier }}")}},                          // mixed template
		{"tenant_id": {Eq: new(" {{ jwt.tenant }} ")}},                    // space-padded ⇒ stays a template
		{"tenant_id": {Eq: new("{{ jwt.tenant }}"), Neq: new("blocked")}}, // two predicates on one column
		{"org": {Eq: new("{{ jwt.org.id }}")}},                            // nested claim path
	}
	claimsets := []map[string]any{
		nil,
		{"tenant": "acme"},
		{"tenant": "acme", "cap": float64(500), "regions": []any{"us", "eu"}, "tag": "x", "tier": "pro", "org": map[string]any{"id": "o1"}},
		{"tenant": ""},
		{"tenant": "acme", "regions": []any{}}, // empty claim list
	}
	rows := []map[string]any{
		{"tenant_id": "acme", "amount": float64(250), "status": "active", "region": "us", "tag": "x", "kind": "published", "label": "v-pro", "org": "o1"},
		{"tenant_id": "globex", "amount": float64(9), "status": "off", "region": "apac", "tag": "y", "kind": "draft", "label": "v-free", "org": "o2"},
		{"amount": float64(500)}, // several columns missing ⇒ fail closed
		{"tenant_id": "", "amount": "NaN", "org": "o1"},
	}
	numeric := map[string]bool{"amount": true}

	for fi, f := range filters {
		p := &Policy{Tables: map[string]TablePolicy{
			"t": {Select: map[string]RolePermissions{"viewer": {Filter: f}}},
		}}
		compiled := CompileRowFilter(p, "viewer", "t", "select")
		assert.Equalf(t, Evaluate(p, "viewer", "t", "select", nil).HasRowFilter(), compiled.HasRowFilter(),
			"HasRowFilter parity, filter#%d", fi)
		for _, claims := range claimsets {
			resolved := Evaluate(p, "viewer", "t", "select", claims)
			for ri, row := range rows {
				want := resolved.RowVisible(row, numeric)
				got := compiled.RowVisible(row, claims, numeric)
				assert.Equalf(t, want, got,
					"filter#%d claims=%v row#%d — compiled=%v resolved=%v", fi, claims, ri, got, want)
			}
		}
	}
}

// FuzzCompiledRowFilterParity is the property form of the matrix test above: for ANY
// operator, filter value, claim, and row the fuzzer synthesizes, the compiled
// per-subscriber stream path (CompileRowFilter + RowVisible) must agree with the query
// path (Evaluate + resolved predicates). The fuzzer probes the template-parsing edges
// where a divergence would hide — stray "{{", "jwt.", nested dots, whitespace, partial
// templates — that a hand-written matrix can't enumerate. The seed corpus also runs as a
// plain regression test under `go test` (no -fuzz needed). The column is fixed to a
// bind-safe name so the role always resolves as allowed; RowVisible parity is defined
// only for an allowed role (a denied role never reaches the per-subscriber check).
func FuzzCompiledRowFilterParity(f *testing.F) {
	f.Add(uint8(0), "{{ jwt.x }}", "acme", "acme", true, false, false)   // eq claim-ref, match
	f.Add(uint8(1), " {{ jwt.x }} ", "acme", "acme", true, false, false) // space-padded ⇒ template
	f.Add(uint8(2), "100", "", "250", true, true, false)                 // gt numeric constant
	f.Add(uint8(3), "{{ jwt.x }}", "500", "250", true, true, false)      // lt numeric claim
	f.Add(uint8(4), "{{ jwt.x }}", "a,b,c", "b", true, false, true)      // in claim-list
	f.Add(uint8(0), "v-{{ jwt.x }}", "pro", "v-pro", true, false, false) // mixed template
	f.Add(uint8(0), "{{ jwt.x }}", "acme", "acme", false, false, false)  // column absent ⇒ fail closed
	f.Add(uint8(0), "{{ jwt.a.b }}", "z", "z", true, false, false)       // nested claim path

	f.Fuzz(func(t *testing.T, opSel uint8, filterVal, claimVal, rowVal string, colPresent, numeric, claimList bool) {
		var filter Filter
		switch opSel % 5 {
		case 0:
			filter.Eq = &filterVal
		case 1:
			filter.Neq = &filterVal
		case 2:
			filter.Gt = &filterVal
		case 3:
			filter.Lt = &filterVal
		case 4:
			filter.In = &filterVal
		}
		p := &Policy{Tables: map[string]TablePolicy{
			"t": {Select: map[string]RolePermissions{"r": {Filter: map[string]Filter{"col": filter}}}},
		}}

		var claim any = claimVal
		if claimList { // exercise the _in []any path, not just a scalar claim
			parts := strings.Split(claimVal, ",")
			list := make([]any, len(parts))
			for i, s := range parts {
				list[i] = s
			}
			claim = list
		}
		claims := map[string]any{"x": claim}
		row := map[string]any{}
		if colPresent {
			row["col"] = rowVal
		}
		nc := map[string]bool{}
		if numeric {
			nc["col"] = true
		}

		compiled := CompileRowFilter(p, "r", "t", "select")
		resolved := Evaluate(p, "r", "t", "select", claims)
		require.True(t, resolved.Allowed, "fixed bind-safe setup must resolve as allowed")
		require.Equalf(t, resolved.RowVisible(row, nc), compiled.RowVisible(row, claims, nc),
			"parity broke: op=%d filter=%q claim=%v(list=%v) row=%q(present=%v) numeric=%v",
			opSel%5, filterVal, claim, claimList, rowVal, colPresent, numeric)
	})
}
