package policy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluate_NilPolicy(t *testing.T) {
	t.Parallel()
	// A nil policy (policies.json empty) fails fully closed: nobody passes, not
	// even the admin role — emptying the policy is a total lockout. A policy
	// only ever arrives via the settings directory, never over HTTP.
	assert.False(t, Evaluate(nil, "viewer", "clicks", "select", nil).Allowed,
		"nil policy must deny a non-admin role")
	assert.False(t, Evaluate(nil, "", "clicks", "select", nil).Allowed,
		"nil policy must deny a roleless request")
	assert.False(t, Evaluate(nil, "admin", "clicks", "select", nil).Allowed,
		"nil policy must deny even the admin role (total lockout)")
}

func TestEvaluate_NoTablePolicy_AdminAllowed(t *testing.T) {
	t.Parallel()
	p := &Policy{Tables: map[string]TablePolicy{}}
	perms := Evaluate(p, "admin", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
}

func TestEvaluate_NoTablePolicy_ViewerDenied(t *testing.T) {
	t.Parallel()
	p := &Policy{Tables: map[string]TablePolicy{}}
	perms := Evaluate(p, "viewer", "clicks", "select", nil)
	assert.False(t, perms.Allowed)
}

func TestEvaluate_ExactRoleMatch(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"page", "count"}},
				},
			},
		},
	}
	perms := Evaluate(p, "viewer", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
	assert.Equal(t, []string{"page", "count"}, perms.AllowColumns)
}

func TestEvaluate_NoMatchingRole_NonAdminDenied(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"editor": {},
				},
			},
		},
	}
	perms := Evaluate(p, "viewer", "clicks", "select", nil)
	assert.False(t, perms.Allowed)
}

func TestEvaluate_NoMatchingRole_AdminAllowed(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"editor": {},
				},
			},
		},
	}
	perms := Evaluate(p, "admin", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
}

func TestEvaluate_ExplicitAdminEntry_AdminStillUnrestricted(t *testing.T) {
	t.Parallel()
	// The admin bypass is unconditional: even a policy that names the admin role
	// with a narrow allow-list must NOT restrict admin. Admin always gets full,
	// unrestricted access — no column/row scoping is read from such an entry.
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"admin": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	perms := Evaluate(p, "admin", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
	assert.Empty(t, perms.AllowColumns, "admin is never column-restricted, even by an explicit admin entry")
}

func TestEvaluate_UnknownOperation(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {},
		},
	}
	perms := Evaluate(p, "viewer", "clicks", "delete", nil)
	assert.False(t, perms.Allowed)
}

func TestEvaluate_FilterWithClaimTemplate(t *testing.T) {
	t.Parallel()
	eqVal := "{{ jwt.org_id }}"
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"user": {
						Filter: map[string]Filter{
							"org_id": {Eq: &eqVal},
						},
					},
				},
			},
		},
	}
	claims := map[string]any{"org_id": "org-123"}
	perms := Evaluate(p, "user", "clicks", "select", claims)
	assert.True(t, perms.Allowed)
	assert.Contains(t, perms.WhereClause, "`org_id` = ?")
	require.Len(t, perms.WhereParams, 1)
	assert.Equal(t, "org-123", perms.WhereParams[0])
}

func TestEvaluate_CheckClauses(t *testing.T) {
	t.Parallel()
	eqVal := "{{ jwt.org_id }}"
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Insert: map[string]RolePermissions{
					"user": {
						Check: map[string]Filter{
							"org_id": {Eq: &eqVal},
						},
					},
				},
			},
		},
	}
	claims := map[string]any{"org_id": "org-456"}
	perms := Evaluate(p, "user", "clicks", "insert", claims)
	assert.True(t, perms.Allowed)
	require.Contains(t, perms.CheckClauses, "org_id")
	assert.Equal(t, "org-456", perms.CheckClauses["org_id"])
}

// TestEvaluate_CheckClauses_StaticLiteralTyped: a placeholder-free check
// value is wrapped as LiteralValue — the marker that lets the ingest
// comparison accept its numeric reading — while a claim-derived value (above)
// stays a plain string, so a string-typed claim can never gain that reading.
func TestEvaluate_CheckClauses_StaticLiteralTyped(t *testing.T) {
	t.Parallel()
	eqVal := "1.0"
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Insert: map[string]RolePermissions{
					"user": {
						Check: map[string]Filter{
							"count": {Eq: &eqVal},
						},
					},
				},
			},
		},
	}
	perms := Evaluate(p, "user", "clicks", "insert", map[string]any{})
	assert.True(t, perms.Allowed)
	require.Contains(t, perms.CheckClauses, "count")
	assert.Equal(t, LiteralValue("1.0"), perms.CheckClauses["count"])
}

func TestEvaluate_AggregationLimits(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"analyst": {
						AllowedAggregations: []string{"count", "sum"},
						DeniedAggregations:  []string{"avg"},
						MaxRows:             1000,
						MaxExecutionTime:    Millis(5000),
						MaxRowsToRead:       2_000_000,
						MaxMemoryUsage:      4 << 30,
					},
				},
			},
		},
	}
	perms := Evaluate(p, "analyst", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
	assert.Equal(t, []string{"count", "sum"}, perms.AllowedAggregations)
	assert.Equal(t, []string{"avg"}, perms.DeniedAggregations)
	assert.Equal(t, 1000, perms.MaxRows)
	// The server-side resource caps (#316) must survive Evaluate so the query
	// path can turn them into ClickHouse settings.
	assert.Equal(t, Millis(5000), perms.MaxExecutionTime)
	assert.Equal(t, 5*time.Second, perms.MaxExecutionTime.Duration())
	assert.Equal(t, int64(2_000_000), perms.MaxRowsToRead)
	assert.Equal(t, ByteSize(4<<30), perms.MaxMemoryUsage)
}

func TestIsColumnAllowed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		perms *ResolvedPermissions
		col   string
		want  bool
	}{
		{"nil perms", nil, "any", true},
		{"no lists - all allowed", &ResolvedPermissions{Allowed: true}, "any", true},
		{"in allow list", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"a", "b"}}, "a", true},
		{"not in allow list", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"a", "b"}}, "c", false},
		{"wildcard allow", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}}, "anything", true},
		{"in deny list", &ResolvedPermissions{Allowed: true, DenyColumns: []string{"secret"}}, "secret", false},
		{"not in deny list", &ResolvedPermissions{Allowed: true, DenyColumns: []string{"secret"}}, "page", true},
		{"deny overrides allow", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"a"}, DenyColumns: []string{"a"}}, "a", false},
		// "*" is now a LITERAL column name, not a wildcard sentinel: it follows the
		// same allow/deny rules as any column. (The all-columns wildcard is the
		// caller's SelectAll, expanded by AllowedProjection — never run through
		// here, which is what closed the #223 footgun structurally.) The builder
		// additionally gates it on schema membership, so it only resolves when a
		// real column is named "*".
		{"literal star, empty allow → allowed", &ResolvedPermissions{Allowed: true}, "*", true},
		{"literal star, wildcard allow → allowed", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}}, "*", true},
		{"literal star, deny of another column → allowed", &ResolvedPermissions{Allowed: true, DenyColumns: []string{"secret"}}, "*", true},
		{"literal star, specific allow without it → denied", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"page"}}, "*", false},
		{"literal star, explicitly denied → denied", &ResolvedPermissions{Allowed: true, DenyColumns: []string{"*"}}, "*", false},
		{"star arg, denied table → denied", &ResolvedPermissions{Allowed: false}, "*", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.perms.IsColumnAllowed(tt.col)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAllowedProjection(t *testing.T) {
	t.Parallel()
	all := []string{"page", "user_id", "payload", "ts"}
	tests := []struct {
		name  string
		perms *ResolvedPermissions
		cols  []string
		want  []string
	}{
		{"nil perms returns input unchanged", nil, all, all},
		{"no lists - all pass", &ResolvedPermissions{Allowed: true}, all, all},
		{
			"allow list keeps order and subset",
			&ResolvedPermissions{Allowed: true, AllowColumns: []string{"ts", "page"}},
			all,
			[]string{"page", "ts"}, // preserves input order, not allow-list order
		},
		{
			"deny list drops denied",
			&ResolvedPermissions{Allowed: true, DenyColumns: []string{"payload"}},
			all,
			[]string{"page", "user_id", "ts"},
		},
		{
			"wildcard allow with deny drops only denied",
			&ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}, DenyColumns: []string{"payload"}},
			all,
			[]string{"page", "user_id", "ts"},
		},
		{
			"allow list disjoint from schema yields empty",
			&ResolvedPermissions{Allowed: true, AllowColumns: []string{"nonexistent"}},
			all,
			[]string{},
		},
		{
			"denied table yields empty",
			&ResolvedPermissions{Allowed: false},
			all,
			[]string{},
		},
		{"empty input yields empty", &ResolvedPermissions{Allowed: true}, []string{}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.perms.AllowedProjection(tt.cols)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRestrictsColumns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		perms *ResolvedPermissions
		want  bool
	}{
		{"nil perms - unrestricted", nil, false},
		{"denied role - restricts everything", &ResolvedPermissions{Allowed: false}, true},
		{"no lists - unrestricted", &ResolvedPermissions{Allowed: true}, false},
		{"bare wildcard allow - unrestricted", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}}, false},
		{"wildcard among allows - unrestricted", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"page", "*"}}, false},
		{"concrete allow list - restricted", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"page"}}, true},
		{"deny list - restricted", &ResolvedPermissions{Allowed: true, DenyColumns: []string{"payload"}}, true},
		{"wildcard allow but deny set - restricted", &ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}, DenyColumns: []string{"payload"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.perms.RestrictsColumns())
		})
	}
}

// TestColumnPrimitives_AgreeOnVisibility pins the invariant that RestrictsColumns
// and IsColumnAllowed/AllowedProjection never disagree: an "unrestricted" role
// must see every schema column, and a "restricted" role must hide at least one.
// If a future refactor drifts the allow/deny precedence in one but not the
// other, the SELECT * expansion decision (which trusts RestrictsColumns) would
// part ways with the per-column check — the exact failure mode behind #223.
func TestColumnPrimitives_AgreeOnVisibility(t *testing.T) {
	t.Parallel()
	schema := []string{"page", "user_id", "payload", "ts"}
	cases := []*ResolvedPermissions{
		{Allowed: false}, // denied role: restricted, sees nothing
		{Allowed: true},
		{Allowed: true, AllowColumns: []string{"*"}},
		{Allowed: true, AllowColumns: []string{"page", "*"}},
		{Allowed: true, AllowColumns: []string{"page"}},
		{Allowed: true, DenyColumns: []string{"payload"}},
		{Allowed: true, AllowColumns: []string{"*"}, DenyColumns: []string{"payload"}},
	}
	for i, perms := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			t.Parallel()
			visible := perms.AllowedProjection(schema)
			if perms.RestrictsColumns() {
				assert.Less(t, len(visible), len(schema),
					"a restricted role must hide at least one schema column")
			} else {
				assert.Equal(t, schema, visible,
					"an unrestricted role must see every schema column, so SELECT * is safe")
			}
		})
	}
}

func TestIsAggregationAllowed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		perms *ResolvedPermissions
		fn    string
		want  bool
	}{
		{"nil perms", nil, "count", true},
		{"no lists", &ResolvedPermissions{Allowed: true}, "count", true},
		{"in denied list", &ResolvedPermissions{Allowed: true, DeniedAggregations: []string{"avg"}}, "avg", false},
		{"not in denied", &ResolvedPermissions{Allowed: true, DeniedAggregations: []string{"avg"}}, "count", true},
		{"in allowed list", &ResolvedPermissions{Allowed: true, AllowedAggregations: []string{"count", "sum"}}, "count", true},
		{"not in allowed", &ResolvedPermissions{Allowed: true, AllowedAggregations: []string{"count"}}, "sum", false},
		{"empty allowed = all", &ResolvedPermissions{Allowed: true, AllowedAggregations: []string{}}, "anything", true},
		{"denied wins despite caller upper-case", &ResolvedPermissions{Allowed: true, DeniedAggregations: []string{"sum"}}, "SUM", false},
		{"denied wins despite caller mixed-case", &ResolvedPermissions{Allowed: true, DeniedAggregations: []string{"sum"}}, "Sum", false},
		{"allowed despite caller upper-case", &ResolvedPermissions{Allowed: true, AllowedAggregations: []string{"count"}}, "COUNT", true},
		{"denied beats allowed despite caller upper-case", &ResolvedPermissions{Allowed: true, DeniedAggregations: []string{"sum"}, AllowedAggregations: []string{"sum"}}, "SUM", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.perms.IsAggregationAllowed(tt.fn)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNavigateClaims(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		claims map[string]any
		parts  []string
		want   any
	}{
		{"nil claims", nil, []string{"a"}, nil},
		{"empty parts", map[string]any{"a": 1}, []string{}, nil},
		{"top-level", map[string]any{"org": "abc"}, []string{"org"}, "abc"},
		{"nested", map[string]any{"a": map[string]any{"b": "val"}}, []string{"a", "b"}, "val"},
		{"missing key", map[string]any{"a": 1}, []string{"b"}, nil},
		{"non-map intermediate", map[string]any{"a": "str"}, []string{"a", "b"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := navigateClaims(tt.claims, tt.parts)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolveTemplate(t *testing.T) {
	t.Parallel()
	claims := map[string]any{
		"org_id":   "org-123",
		"nested":   map[string]any{"val": "deep"},
		"tids":     []any{"a", "b"},
		"is_admin": true,
		"big":      json.Number("12345678901234567890"),
		"price":    json.Number("1.0"),
		"exp3":     json.Number("1e3"),
		"huge":     json.Number("1e400"),
	}
	tests := []struct {
		name   string
		tmpl   string
		want   string
		wantOK bool
	}{
		{"single claim", "{{ jwt.org_id }}", "org-123", true},
		{"nested claim", "{{ jwt.nested.val }}", "deep", true},
		{"missing claim", "{{ jwt.missing }}", "", false},
		{"literal value", "literal", "literal", true},
		// A template-free empty string is the policy author's literal value, not
		// an unresolvable claim — it must stay ok so `_eq: ""` binds as written.
		{"empty literal", "", "", true},
		// One unresolvable claim poisons the whole template, even alongside a
		// resolvable one or surrounding text.
		{"mixed resolvable and missing", "{{ jwt.org_id }}-{{ jwt.missing }}", "org-123-", false},
		// A structured claim value fails closed: an object usually means a
		// dropped path segment ({{ jwt.nested }} for {{ jwt.nested.val }}), and
		// its "map[…]"/"[…]" stringification is never a sensible bound value.
		{"object claim fails closed", "{{ jwt.nested }}", "", false},
		{"array claim fails closed", "{{ jwt.tids }}", "", false},
		{"array claim with surrounding text fails closed", "t-{{ jwt.tids }}", "t-", false},
		// Scalars stay bound as today — booleans stringify, and a json.Number
		// keeps every digit (the >2^53 case float64 would silently round).
		{"boolean claim binds", "{{ jwt.is_admin }}", "true", true},
		{"large integer claim binds exactly", "{{ jwt.big }}", "12345678901234567890", true},
		// Numeric claims bind in canonical decimal form, not the token's
		// spelling — "1.0"/"1e3" error as TYPE_MISMATCH against a numeric
		// column if bound verbatim. A magnitude only JSON can hold fails
		// closed like any other unresolvable claim.
		{"float spelling binds canonically", "{{ jwt.price }}", "1", true},
		{"exponent spelling binds canonically", "{{ jwt.exp3 }}", "1000", true},
		{"beyond-float64 number fails closed", "{{ jwt.huge }}", "", false},
		// A static literal binds exactly as written even when it spells a JSON
		// number: canonicalizing it here would move read filters on String
		// columns (`_neq: "1.0"` on a version column would stop excluding rows
		// storing "1.0"). The insert-check comparison accepts the numeric
		// reading at compare time instead (CanonicalNumericLiteral).
		{"numeric-spelled literal binds as written", "1.0", "1.0", true},
		{"exponent-spelled literal binds as written", "1e400", "1e400", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := resolveTemplate(tt.tmpl, claims)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestResolveTemplate_NilClaims(t *testing.T) {
	t.Parallel()
	got, ok := resolveTemplate("{{ jwt.org_id }}", nil)
	assert.Equal(t, "", got)
	assert.False(t, ok)
}

func TestResolveTemplate_MultipleTemplates(t *testing.T) {
	t.Parallel()
	claims := map[string]any{"a": "1", "b": "2"}
	result, ok := resolveTemplate("{{ jwt.a }}-{{ jwt.b }}", claims)
	assert.Equal(t, "1-2", result)
	assert.True(t, ok)
}

// TestCanonicalScalar pins the one rule every bound value flows through — claim
// templates, _in elements, and the ingest check comparison alike. The canonical
// form is exact at every width and precision (integers via big.Int, fractions
// and exponents via canonicalDecimal — never a float64 round-trip, which would
// collapse "1e-400" to "0" and round wide decimals onto their neighbors), and
// a literal or exact form past the 100-digit bound fails closed in both
// directions.
func TestCanonicalScalar(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v    any
		want string
		ok   bool
	}{
		{"string passes through", "org-123", "org-123", true},
		{"empty string passes through", "", "", true},
		{"boolean", true, "true", true},
		{"go int from a hand-built claims map", 42, "42", true},
		{"integer literal", json.Number("7"), "7", true},
		{"negative integer literal", json.Number("-7"), "-7", true},
		{"large integer exact at any width", json.Number("12345678901234567890"), "12345678901234567890", true},
		{"float spelling of an integer", json.Number("1.0"), "1", true},
		{"exponent spelling", json.Number("1e3"), "1000", true},
		{"uppercase exponent spelling", json.Number("1E3"), "1000", true},
		{"fraction kept", json.Number("1.5"), "1.5", true},
		{"trailing fraction zeros trim", json.Number("2.50"), "2.5", true},
		// The sign must survive the non-integer path: dropping it would bind a
		// value on the other side of zero, so `_lt` against a claim of -1.5
		// would match everything below 1.5 — this PR's whole bug class.
		{"negative fraction keeps sign", json.Number("-2.50"), "-2.5", true},
		{"negative exponent expands exactly", json.Number("25e-4"), "0.0025", true},
		{"negative exponent keeps sign through the 0. prefix", json.Number("-25e-4"), "-0.0025", true},
		{"high-precision fraction stays exact", json.Number("0.1000000000000000000001"), "0.1000000000000000000001", true},
		{"wide decimal stays exact", json.Number("12345678901234567890.5"), "12345678901234567890.5", true},
		{"negative zero folds to zero", json.Number("-0.0"), "0", true},
		{"underflow has no canonical form", json.Number("1e-400"), "", false},
		{"overflow has no canonical form", json.Number("1e400"), "", false},
		{"negative overflow has no canonical form", json.Number("-1e400"), "", false},
		{"wide finite magnitude has no canonical form", json.Number("1.5e200"), "", false},
		// A zero mantissa reaches the exponent bound before anything else can
		// reject it — the exact-form length check can't (every spelling of zero
		// is one character), and that bound is what stops a short literal from
		// allocating a gigabyte-scale expansion before the length check runs.
		{"zero mantissa with out-of-range exponent", json.Number("0e201"), "", false},
		// The 100-digit literal bound: big.Int work is superlinear in digit
		// count and the ingest path hands this function client-controlled
		// literals, so anything longer fails closed before any parsing. The
		// bound counts digits, not bytes — sign and exponent markers ride free —
		// so two spellings of one value pass or fail together.
		{"100-digit integer at the bound stays exact", json.Number(strings.Repeat("9", 100)), strings.Repeat("9", 100), true},
		{"101-digit literal has no canonical form", json.Number(strings.Repeat("9", 101)), "", false},
		{"digit bound ignores sign and exponent bytes", json.Number("-1e99"), "-1" + strings.Repeat("0", 99), true},
		{"written-out spelling of -1e99 binds identically", json.Number("-1" + strings.Repeat("0", 99)), "-1" + strings.Repeat("0", 99), true},
		{"null has no canonical form", nil, "", false},
		{"object has no canonical form", map[string]any{"id": 1}, "", false},
		{"array has no canonical form", []any{"a"}, "", false},
		// float64 is what a plain json.Unmarshal hands a hand-built claims map
		// (production claims are json.Number via jwt.WithJSONNumber). At/past
		// 2^53 the decode already collapsed neighboring integers, so any digits
		// rendered could be another principal's ID — refuse; below it, render
		// positionally, never fmt.Sprint's "1e+06" exponent spelling.
		{"float64 below 2^53 renders positionally", float64(1_000_000), "1000000", true},
		{"float64 fraction", float64(1.5), "1.5", true},
		{"float64 at 2^53 refused — digits lost at decode", float64(1 << 53), "", false},
		{"float64 past 2^53 refused", float64(10000000000000001), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := CanonicalScalar(tt.v)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

// TestCanonicalNumericLiteral pins the numeric reading of a policy-authored
// check literal: only spellings JSON itself can produce canonicalize — the
// json.Valid gate rejects big.Int-acceptable forms like "+5" and "007" that
// no decoded claim or payload value ever carries, so the check comparison's
// second reading can't accept a spelling the first side can't produce.
func TestCanonicalNumericLiteral(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		lit  string
		want string
		ok   bool
	}{
		{"float spelling of an integer", "1.0", "1", true},
		{"exponent spelling", "25e-4", "0.0025", true},
		{"negative fraction", "-2.50", "-2.5", true},
		{"integer passes through", "7", "7", true},
		{"leading plus is not JSON", "+5", "", false},
		{"leading zero is not JSON", "007", "", false},
		{"whitespace-padded number is not a bare literal", " 5", "", false},
		{"non-numeric literal", "org-123", "", false},
		{"boolean literal is valid JSON but not a number", "true", "", false},
		{"empty literal", "", "", false},
		{"no canonical form past the bound", "1e400", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := CanonicalNumericLiteral(tt.lit)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	tmpl := "{{ jwt.tenant_id }}"
	tests := []struct {
		name    string
		policy  *Policy
		wantErr bool
		wantMsg string
	}{
		{
			// The explicit spelling of #460's fail-open: an operator-less filter
			// entry resolves to zero predicates (no row restriction). The typo
			// route to the same shape is closed by strict decoding.
			name: "operator-less filter entry rejected",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"viewer": {Filter: map[string]Filter{"tenant_id": {}}},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: `filter column "tenant_id" sets no operator`,
		},
		{
			name: "operator-less check entry rejected",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Insert: map[string]RolePermissions{
							"writer": {Check: map[string]Filter{"user_id": {}}},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: `check column "user_id" sets no operator`,
		},
		{
			name: "filter under insert grant rejected",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Insert: map[string]RolePermissions{
							"writer": {Filter: map[string]Filter{"tenant_id": {Eq: &tmpl}}},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "filter has no effect on insert",
		},
		{
			// The mirror, and the fail-open direction: the author believes reads
			// are row-scoped, but nothing on the select/stream paths reads a
			// check entry — every viewer would see every row.
			name: "check under select grant rejected",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"viewer": {Check: map[string]Filter{"tenant_id": {Eq: &tmpl}}},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "check has no effect on select",
		},
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: true,
			wantMsg: "nil",
		},
		{
			name: "valid policy",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"viewer": {AllowColumns: []string{"page"}, MaxRows: 100},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "negative max_rows",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"viewer": {MaxRows: -1},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "max_rows",
		},
		{
			name: "negative max_execution_time",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Insert: map[string]RolePermissions{
							"user": {MaxExecutionTime: Millis(-500)},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "max_execution_time",
		},
		{
			name: "negative max_rows_to_read",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"viewer": {MaxRowsToRead: -1},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "max_rows_to_read",
		},
		{
			name: "negative max_memory_usage",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"viewer": {MaxMemoryUsage: -1},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "max_memory_usage",
		},
		{
			name: "empty role key rejected",
			policy: &Policy{
				Tables: map[string]TablePolicy{
					"clicks": {
						Select: map[string]RolePermissions{
							"": {AllowColumns: []string{"page"}},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "empty role",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := Validate(tt.policy)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEvaluate_NilRolePermsMap_AdminAllowed(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {}, // No select/insert maps.
		},
	}
	perms := Evaluate(p, "admin", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
}

func TestEvaluate_NilRolePermsMap_ViewerDenied(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {},
		},
	}
	perms := Evaluate(p, "viewer", "clicks", "select", nil)
	assert.False(t, perms.Allowed)
}

func TestEvaluate_CustomAdminRole(t *testing.T) {
	t.Parallel()
	p := &Policy{AdminRole: "superuser", Tables: map[string]TablePolicy{}}
	assert.True(t, Evaluate(p, "superuser", "clicks", "select", nil).Allowed,
		"the configured admin_role bypasses like admin")
	assert.False(t, Evaluate(p, "admin", "clicks", "select", nil).Allowed,
		`with a custom admin_role, the literal "admin" is an ordinary (denied) role`)
	assert.False(t, Evaluate(p, "service", "clicks", "select", nil).Allowed,
		`"service" is no longer a privileged role`)
}

// TestEvaluate_EmptyRoleDoesNotMatchEmptyKey is the evaluation-time guard that
// pairs with TestValidate_RejectsEmptyRoleKey: even if a policy with an
// empty-string role key reaches the engine (e.g. loaded from KV, written
// before the validation existed), an empty/absent role must NOT match it and
// must fail closed. Only the admin role or a configured default_role may
// authorize a roleless request — the "*" any-role wildcard was removed.
func TestEvaluate_EmptyRoleDoesNotMatchEmptyKey(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	perms := Evaluate(p, "", "clicks", "select", nil)
	assert.False(t, perms.Allowed, "an empty role must not match an empty-string role key")
}

// TestEvaluate_WildcardRoleKeyDoesNotGrant pins the removal of the "*" any-role
// wildcard: a "*" role key is now an ordinary (unreachable) literal, so neither a
// real role that doesn't equal it nor a roleless request gets access from it.
// Broad grants must list each role explicitly; roleless access comes only from a
// concrete default_role.
func TestEvaluate_WildcardRoleKeyDoesNotGrant(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"*": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	assert.False(t, Evaluate(p, "viewer", "clicks", "select", nil).Allowed,
		"a \"*\" role key must not grant a non-matching role")
	assert.False(t, Evaluate(p, "", "clicks", "select", nil).Allowed,
		"a \"*\" role key must not grant a roleless request")
}

func TestResolveFilters_MultipleOperators(t *testing.T) {
	t.Parallel()
	neqVal := "deleted"
	gtVal := "0"
	filters := map[string]Filter{
		"status": {Neq: &neqVal},
		"count":  {Gt: &gtVal},
	}
	clauses, params := resolveFilters(filters, nil)
	assert.Len(t, clauses, 2)
	assert.Len(t, params, 2)
}

func TestResolveFilters_LtOperator(t *testing.T) {
	t.Parallel()
	ltVal := "100"
	filters := map[string]Filter{
		"price": {Lt: &ltVal},
	}
	clauses, params := resolveFilters(filters, nil)
	require.Len(t, clauses, 1)
	assert.Contains(t, clauses[0], "`price` < ?")
	assert.Equal(t, "100", params[0])
}

// TestResolveFilters_UnresolvableClaim_FailsClosed: the #385 fix — a row-filter
// template referencing a claim the token doesn't carry emits a constant-false
// predicate for EVERY operator, never a real comparison against the empty
// string it renders to (where not-equals / greater-than on a string column
// would match essentially all rows).
func TestResolveFilters_UnresolvableClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	tmpl := "{{ jwt.tenant_id }}"
	partial := "t-{{ jwt.tenant_id }}"
	claims := map[string]any{"role": "user"} // validly-signed token, no tenant_id
	tests := []struct {
		name   string
		filter Filter
	}{
		{"_eq", Filter{Eq: &tmpl}},
		{"_neq", Filter{Neq: &tmpl}},
		{"_gt", Filter{Gt: &tmpl}},
		{"_lt", Filter{Lt: &tmpl}},
		{"_eq partial", Filter{Eq: &partial}},
		{"_in template text", Filter{In: &partial}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clauses, params := resolveFilters(map[string]Filter{"tenant_id": tt.filter}, claims)
			require.Len(t, clauses, 1)
			assert.Equal(t, "1 = 0", clauses[0])
			assert.Empty(t, params)
		})
	}
}

// TestResolveFilters_StructuredClaim_FailsClosed: a claim that resolves to a
// JSON object or array — usually a policy typo that dropped the final path
// segment ({{ jwt.meta }} for {{ jwt.meta.tenant_id }}) — fails closed on every
// operator instead of binding its "map[…]"/"[…]" stringification, which _neq
// would match against essentially every row. The one legitimate structured
// shape is unaffected: a bare-claim _in against an ARRAY binds its elements
// (TestResolveFilters_InArrayClaim).
func TestResolveFilters_StructuredClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	obj := "{{ jwt.meta }}"
	arr := "{{ jwt.tids }}"
	arrText := "t-{{ jwt.tids }}"
	claims := map[string]any{
		"meta": map[string]any{"tenant_id": "acme"},
		"tids": []any{"a", "b"},
	}
	tests := []struct {
		name   string
		filter Filter
	}{
		{"_eq object", Filter{Eq: &obj}},
		{"_neq object", Filter{Neq: &obj}},
		{"_gt array", Filter{Gt: &arr}},
		{"_in object", Filter{In: &obj}},
		{"_in array with text", Filter{In: &arrText}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clauses, params := resolveFilters(map[string]Filter{"tenant_id": tt.filter}, claims)
			require.Len(t, clauses, 1)
			assert.Equal(t, "1 = 0", clauses[0])
			assert.Empty(t, params)
		})
	}
}

// TestResolveFilters_LiteralEmptyValue_Binds: a template-free literal "" is the
// policy author's chosen value, not a resolution failure — it must keep binding
// an equality against the empty string rather than be mistaken for the #385
// fail-closed case.
func TestResolveFilters_LiteralEmptyValue_Binds(t *testing.T) {
	t.Parallel()
	empty := ""
	clauses, params := resolveFilters(map[string]Filter{"status": {Eq: &empty}}, nil)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`status` = ?", clauses[0])
	assert.Equal(t, []any{""}, params)
}

// TestResolveFilters_EmptyStringClaim_Binds: the boundary of the #385
// fail-closed rule — only an ABSENT or null claim fails closed. A claim present
// as an empty string resolves and binds normally, so `_neq` against an
// empty-string claim still emits a real `col != ?` predicate bound to the
// empty string, never a constant-false predicate.
func TestResolveFilters_EmptyStringClaim_Binds(t *testing.T) {
	t.Parallel()
	neq := "{{ jwt.tenant_id }}"
	claims := map[string]any{"tenant_id": ""}
	clauses, params := resolveFilters(map[string]Filter{"tenant_id": {Neq: &neq}}, claims)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`tenant_id` != ?", clauses[0])
	assert.Equal(t, []any{""}, params)
}

// TestResolveFilters_InEmptyStringClaim_Binds: an _in whose claim is present as
// an empty STRING is a scalar, not an empty set — it binds as the one-element
// set `IN (?)` bound to the empty string. Failing closed is reserved for
// absent/null claims and empty arrays (TestResolveFilters_InEmptyClaim_FailsClosed).
func TestResolveFilters_InEmptyStringClaim_Binds(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenant_id }}"
	claims := map[string]any{"tenant_id": ""}
	clauses, params := resolveFilters(map[string]Filter{"tenant_id": {In: &in}}, claims)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`tenant_id` IN (?)", clauses[0])
	assert.Equal(t, []any{""}, params)
}

// TestResolveFilters_InTemplateWithText_Binds: the resolvable half of the _in
// surrounding-text branch — a template with literal text AND a claim the token
// carries binds the rendered one-element set `IN (?)`. Its fail-closed twin
// (unresolvable claim → 1 = 0) is TestResolveFilters_UnresolvableClaim_FailsClosed;
// this guards against a regression to an unconditional nil, which would deny every
// row for a policy of this shape while the whole suite still passed.
func TestResolveFilters_InTemplateWithText_Binds(t *testing.T) {
	t.Parallel()
	in := "t-{{ jwt.tenant_id }}"
	claims := map[string]any{"tenant_id": "x"}
	clauses, params := resolveFilters(map[string]Filter{"tenant_id": {In: &in}}, claims)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`tenant_id` IN (?)", clauses[0])
	assert.Equal(t, []any{"t-x"}, params)
}

// TestEvaluate_CheckUnresolvableClaim_ResolvesEmptyString: the deliberate
// read/write asymmetry of #385 — a check _eq keeps the resolved scalar even
// when the claim is unresolvable, so an omitted column auto-injects the empty
// string and any other supplied value is rejected at ingest. Whether the write
// path should instead reject the insert is tracked in #463; this pins today's
// behavior so neither path drifts silently.
func TestEvaluate_CheckUnresolvableClaim_ResolvesEmptyString(t *testing.T) {
	t.Parallel()
	eq := "{{ jwt.org_id }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Insert: map[string]RolePermissions{
			"user": {Check: map[string]Filter{"org_id": {Eq: &eq}}},
		}},
	}}
	perms := Evaluate(p, "user", "clicks", "insert", map[string]any{"role": "user"}) // no org_id claim
	require.True(t, perms.Allowed)
	require.Contains(t, perms.CheckClauses, "org_id")
	assert.Equal(t, "", perms.CheckClauses["org_id"])
}

// TestEvaluate_FilterUnresolvableClaim_FailsClosed: the issue #385 scenario
// end-to-end — a select policy scoping tenant_id to {{ jwt.tenant_id }} against
// a token without that claim yields a constant-false WHERE, never an equality
// binding the empty string.
func TestEvaluate_FilterUnresolvableClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	eq := "{{ jwt.tenant_id }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Select: map[string]RolePermissions{
			"user": {Filter: map[string]Filter{"tenant_id": {Eq: &eq}}},
		}},
	}}
	perms := Evaluate(p, "user", "clicks", "select", map[string]any{"role": "user"})
	require.True(t, perms.Allowed)
	assert.Equal(t, "1 = 0", perms.WhereClause)
	assert.Empty(t, perms.WhereParams)
}

// TestValidate_RejectsBindUnsafeFilterColumn: a policy whose row-filter column
// contains '?' is refused at write time — it would shift clickhouse-go's
// positional value binding when interpolated into the WHERE clause.
func TestValidate_RejectsBindUnsafeFilterColumn(t *testing.T) {
	t.Parallel()
	eq := "{{ jwt.org }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Select: map[string]RolePermissions{
			"viewer": {Filter: map[string]Filter{"weird?col": {Eq: &eq}}},
		}},
	}}
	err := Validate(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contains '?'")
}

// TestValidate_RejectsMalformedClaimTemplate: a filter or check value whose
// {{ … }} claim path is outside the grammar (hyphenated, a namespaced OIDC URL, a
// wrong-case prefix, indexing, or unterminated) is bound as literal text by
// resolveTemplate — a fail-open on the filter path (`_neq`/`_lt` match ~every row)
// and silent row corruption on the check path — so it is refused at write time
// (#385/#457), on both the select/filter and insert/check sides.
func TestValidate_RejectsMalformedClaimTemplate(t *testing.T) {
	t.Parallel()
	for _, tmpl := range []string{
		"{{ jwt.tenant-id }}",                         // hyphen
		"{{ jwt.https://app.example.com/tenant_id }}", // namespaced OIDC claim
		"{{ JWT.tenant_id }}",                         // wrong-case prefix
		"{{ jwt.tenant_id[0] }}",                      // indexing
		"{{ jwt. }}",                                  // empty path
		"{{ jwt.tenant_id",                            // unterminated
	} {
		t.Run(tmpl, func(t *testing.T) {
			t.Parallel()
			val := tmpl
			filterPolicy := &Policy{Tables: map[string]TablePolicy{
				"clicks": {Select: map[string]RolePermissions{
					"viewer": {Filter: map[string]Filter{"tenant_id": {Neq: &val}}},
				}},
			}}
			require.Error(t, Validate(filterPolicy), "malformed template must be rejected on the filter path")

			checkPolicy := &Policy{Tables: map[string]TablePolicy{
				"clicks": {Insert: map[string]RolePermissions{
					"writer": {Check: map[string]Filter{"tenant_id": {Eq: &val}}},
				}},
			}}
			require.Error(t, Validate(checkPolicy), "malformed template must be rejected on the check path")
		})
	}
}

// TestValidate_AcceptsWellFormedTemplates: the boundary of the write-time guard —
// a bare template, a nested template, a template with surrounding literal text, a
// plain literal, and an empty literal all remain valid.
func TestValidate_AcceptsWellFormedTemplates(t *testing.T) {
	t.Parallel()
	for _, tmpl := range []string{
		"{{ jwt.tenant_id }}",
		"{{ jwt.app_metadata.tenant_id }}",
		"acct-{{ jwt.org_id }}",
		"literal-value",
		"",
	} {
		t.Run(tmpl, func(t *testing.T) {
			t.Parallel()
			val := tmpl
			p := &Policy{Tables: map[string]TablePolicy{
				"clicks": {Select: map[string]RolePermissions{
					"viewer": {Filter: map[string]Filter{"tenant_id": {Eq: &val}}},
				}},
			}}
			require.NoError(t, Validate(p))
		})
	}
}

// TestResolveFilters_InArrayClaim: the headline #224 fix — an _in filter whose
// value is a single array-valued claim expands to `col IN (?, …)` with one bound
// param per element, scoping the role to that set instead of producing no
// predicate (the former fail-open).
func TestResolveFilters_InArrayClaim(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenants }}"
	filters := map[string]Filter{"tenant_id": {In: &in}}
	claims := map[string]any{"tenants": []any{"a", "b", "c"}}
	clauses, params := resolveFilters(filters, claims)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`tenant_id` IN (?,?,?)", clauses[0])
	assert.Equal(t, []any{"a", "b", "c"}, params)
}

// TestResolveFilters_InScalarClaim: a non-array claim yields a single-element IN,
// so _in degrades gracefully to the _eq case rather than erroring.
func TestResolveFilters_InScalarClaim(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenant }}"
	filters := map[string]Filter{"tenant_id": {In: &in}}
	claims := map[string]any{"tenant": "solo"}
	clauses, params := resolveFilters(filters, claims)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`tenant_id` IN (?)", clauses[0])
	assert.Equal(t, []any{"solo"}, params)
}

// TestResolveFilters_InNonScalarElement_FailsClosed: the structured-claim rule
// (TestResolveFilters_StructuredClaim_FailsClosed) extends INSIDE a bare-claim
// _in array — one object, null, nested-array, or canonical-form-less numeric
// element fails the WHOLE set closed. Binding such an element's
// "map[…]"/"<nil>" rendering would bind a value no row legitimately carries,
// and binding only the clean remainder would silently shrink the set the
// policy author declared.
func TestResolveFilters_InNonScalarElement_FailsClosed(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenants }}"
	filters := map[string]Filter{"tenant_id": {In: &in}}
	tests := []struct {
		name    string
		tenants []any
	}{
		{"object element", []any{map[string]any{"id": 1}, map[string]any{"id": 2}}},
		{"null element", []any{"a", nil}},
		{"nested array element", []any{[]any{"1", "2"}, []any{"3"}}},
		{"beyond-float64 numeric element", []any{"a", json.Number("1e400")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clauses, params := resolveFilters(filters, map[string]any{"tenants": tt.tenants})
			require.Len(t, clauses, 1)
			assert.Equal(t, "1 = 0", clauses[0])
			assert.Empty(t, params)
		})
	}
}

// TestResolveFilters_InNumericElements_BindCanonically: scalar elements of a
// bare-claim _in array bind through the same CanonicalScalar rule as every
// other operator — numeric spellings canonicalize, large integers keep exact
// digits, strings pass through.
func TestResolveFilters_InNumericElements_BindCanonically(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenants }}"
	filters := map[string]Filter{"tenant_id": {In: &in}}
	claims := map[string]any{"tenants": []any{json.Number("1.0"), json.Number("12345678901234567890"), "b"}}
	clauses, params := resolveFilters(filters, claims)
	require.Len(t, clauses, 1)
	assert.Equal(t, "`tenant_id` IN (?,?,?)", clauses[0])
	assert.Equal(t, []any{"1", "12345678901234567890", "b"}, params)
}

// TestResolveFilters_NumericClaimBinding pins the SQL surface of numeric claim
// rendering for a hand-built claims map: a json.Number claim binds its canonical
// exact digits; a float64 below 2^53 binds positionally (never the "1e+06"
// spelling ClickHouse integer columns reject); a float64 at or past 2^53 lost
// its digits at decode, so the predicate renders `1 = 0` — matching no rows, the
// same verdict RowVisible reaches in memory — alone or as one _in element.
func TestResolveFilters_NumericClaimBinding(t *testing.T) {
	t.Parallel()
	tmpl := "{{ jwt.tenant }}"
	eq := map[string]Filter{"tenant_id": {Eq: &tmpl}}

	clauses, params := resolveFilters(eq, map[string]any{"tenant": json.Number("10000000000000001")})
	require.Equal(t, []string{"`tenant_id` = ?"}, clauses)
	assert.Equal(t, []any{"10000000000000001"}, params, "json.Number binds exact digits")

	clauses, params = resolveFilters(eq, map[string]any{"tenant": float64(1_000_000)})
	require.Equal(t, []string{"`tenant_id` = ?"}, clauses)
	assert.Equal(t, []any{"1000000"}, params, "small float binds positionally, not 1e+06")

	clauses, params = resolveFilters(eq, map[string]any{"tenant": float64(10000000000000001)})
	assert.Equal(t, []string{"1 = 0"}, clauses, "lossy float64 claim matches no rows")
	assert.Empty(t, params)

	in := "{{ jwt.tenants }}"
	clauses, params = resolveFilters(map[string]Filter{"tenant_id": {In: &in}},
		map[string]any{"tenants": []any{"a", float64(1 << 60)}})
	assert.Equal(t, []string{"1 = 0"}, clauses, "one poisoned element resolves the whole set empty")
	assert.Empty(t, params)
}

// TestCompareCanonicalDecimals pins the digit-string ordering over canonical
// forms — the comparison twin of canonicalDecimal, exact at any width, never a
// float round-trip. Each pair is asserted in both directions.
func TestCompareCanonicalDecimals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want int
	}{
		{"0", "0", 0},
		{"1", "2", -1},
		{"9", "100", -1},
		{"-1", "1", -1},
		{"-2", "-1", -1},
		{"-100", "-9", -1},
		{"1.5", "1.5", 0},
		{"1.05", "1.5", -1},
		{"0.5", "0.55", -1},
		{"2", "2.5", -1},
		{"-1.5", "-1", -1},
		{"0.0025", "0.003", -1},
		{"12345678901234567890", "12345678901234567891", -1},
		{"9007199254740992", "9007199254740993", -1},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, compareCanonicalDecimals(tt.a, tt.b), "%s vs %s", tt.a, tt.b)
		assert.Equal(t, -tt.want, compareCanonicalDecimals(tt.b, tt.a), "%s vs %s reversed", tt.b, tt.a)
	}
}

// TestResolveFilters_InEmptyClaim_FailsClosed: an empty set makes the predicate
// match no rows (a constant-false predicate) rather than widen to all rows — the
// fail-closed direction. `IN ()` is invalid SQL. Two distinct branches of
// resolveInValues reach this: an absent claim (navigateClaims returns nil, which
// CanonicalScalar rejects in its nil/object/array case, so resolveInValues'
// `default` branch fails the set closed) and a present-but-empty array (the
// `case []any` branch with zero elements). Both must fail closed.
func TestResolveFilters_InEmptyClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenants }}"
	filters := map[string]Filter{"tenant_id": {In: &in}}
	cases := map[string]map[string]any{
		"absent claim":        nil,
		"present empty array": {"tenants": []any{}},
	}
	for name, claims := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clauses, params := resolveFilters(filters, claims)
			require.Len(t, clauses, 1)
			assert.Equal(t, "1 = 0", clauses[0])
			assert.Empty(t, params)
		})
	}
}

// TestEvaluate_FilterInClause: end-to-end through Evaluate, an _in filter lands
// in the role's WhereClause/WhereParams (exercising the bind-safe guard + IN
// assembly), not just the resolveFilters unit.
func TestEvaluate_FilterInClause(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.app_metadata.tenant_ids }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Select: map[string]RolePermissions{
			"user": {Filter: map[string]Filter{"tenant_id": {In: &in}}},
		}},
	}}
	claims := map[string]any{"app_metadata": map[string]any{"tenant_ids": []any{"t1", "t2"}}}
	perms := Evaluate(p, "user", "clicks", "select", claims)
	require.True(t, perms.Allowed)
	assert.Contains(t, perms.WhereClause, "`tenant_id` IN (?,?)")
	assert.Equal(t, []any{"t1", "t2"}, perms.WhereParams)
}

// TestEvaluate_CheckInResolvesToSet: an _in check resolves to a []any set in
// CheckClauses (vs a scalar required value), which the ingest path enforces as
// membership.
func TestEvaluate_CheckInResolvesToSet(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenants }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Insert: map[string]RolePermissions{
			"writer": {Check: map[string]Filter{"tenant_id": {In: &in}}},
		}},
	}}
	claims := map[string]any{"tenants": []any{"a", "b"}}
	perms := Evaluate(p, "writer", "clicks", "insert", claims)
	require.True(t, perms.Allowed)
	require.Contains(t, perms.CheckClauses, "tenant_id")
	assert.Equal(t, []any{"a", "b"}, perms.CheckClauses["tenant_id"])
}

// TestValidate_AllowsFilterIn: _in is now enforced on the filter path (#224), so
// a filter authored with it passes validation rather than being rejected.
func TestValidate_AllowsFilterIn(t *testing.T) {
	t.Parallel()
	in := "{{ jwt.tenants }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Select: map[string]RolePermissions{
			"viewer": {Filter: map[string]Filter{"tenant_id": {In: &in}}},
		}},
	}}
	require.NoError(t, Validate(p))
}

// TestValidate_RejectsComparisonCheckOps: check honors only _eq and _in; the
// comparison operators have no insert-time semantics, so each stays a loud
// write-time error (#224). _in and _eq are accepted (covered elsewhere).
func TestValidate_RejectsComparisonCheckOps(t *testing.T) {
	t.Parallel()
	v := "{{ jwt.sub }}"
	cases := map[string]Filter{
		"_neq": {Neq: &v},
		"_gt":  {Gt: &v},
		"_lt":  {Lt: &v},
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := &Policy{Tables: map[string]TablePolicy{
				"clicks": {Insert: map[string]RolePermissions{
					"writer": {Check: map[string]Filter{"user_id": f}},
				}},
			}}
			err := Validate(p)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "does not honor")
		})
	}
}

// TestValidate_RejectsMixedCheckOps: a check column may carry only one
// required-value operator. Setting both _eq and _in is ambiguous — Evaluate's
// switch honors _eq and silently drops _in — so it is rejected at write time
// rather than enforcing an arbitrary branch (the accept-but-ignore gap #224
// closes).
func TestValidate_RejectsMixedCheckOps(t *testing.T) {
	t.Parallel()
	v := "{{ jwt.sub }}"
	set := "{{ jwt.tenants }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Insert: map[string]RolePermissions{
			"writer": {Check: map[string]Filter{"tenant_id": {Eq: &v, In: &set}}},
		}},
	}}
	err := Validate(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both _eq and _in")
}

// TestValidate_AllowsEnforcedOperators: every operator the engine enforces —
// _eq/_neq/_gt/_lt/_in on filter, _eq/_in on check — passes validation, so the
// guards don't over-reject a well-formed policy.
func TestValidate_AllowsEnforcedOperators(t *testing.T) {
	t.Parallel()
	v := "{{ jwt.sub }}"
	set := "{{ jwt.tenants }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {
			Select: map[string]RolePermissions{
				"viewer": {Filter: map[string]Filter{"a": {Eq: &v}, "b": {Neq: &v}, "c": {Gt: &v}, "d": {Lt: &v}, "e": {In: &set}}},
			},
			Insert: map[string]RolePermissions{
				"writer": {Check: map[string]Filter{"user_id": {Eq: &v}, "tenant_id": {In: &set}}},
			},
		},
	}}
	require.NoError(t, Validate(p))
}

// TestEvaluate_DeniesBindUnsafeFilterColumn: defense-in-depth — a bind-unsafe
// filter column that somehow reaches Evaluate (which does not re-validate) denies
// the role fail-closed rather than emitting a binding-shifted query.
func TestEvaluate_DeniesBindUnsafeFilterColumn(t *testing.T) {
	t.Parallel()
	eq := "{{ jwt.org }}"
	p := &Policy{Tables: map[string]TablePolicy{
		"clicks": {Select: map[string]RolePermissions{
			"viewer": {AllowColumns: []string{"page"}, Filter: map[string]Filter{"weird?col": {Eq: &eq}}},
		}},
	}}
	perms := Evaluate(p, "viewer", "clicks", "select", map[string]any{"org": "o1"})
	assert.False(t, perms.Allowed, "a bind-unsafe filter column must deny the role")
}

func TestResolveRole(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		adminRole   string
		defaultRole string
		role        string
		want        string
	}{
		{"non-empty role unchanged", "", "viewer", "editor", "editor"},
		{"empty role -> default", "", "viewer", "", "viewer"},
		{"empty role, no default -> empty", "", "", "", ""},
		// default_role == admin is honored (a local/dev convenience): a roleless
		// request resolves to admin. The store warns loudly when such a policy
		// is adopted; ResolveRole itself does not block it.
		{"empty role, default==admin resolves to admin", "", "admin", "", "admin"},
		{"empty role, default==custom admin resolves to it", "superuser", "superuser", "", "superuser"},
		// Matching is exact and case-sensitive — these are NOT the admin role.
		{"service default allowed (no longer privileged)", "", "service", "", "service"},
		{"ADMIN (case) is its own role", "", "ADMIN", "", "ADMIN"},
		{"padded admin (no trim) is its own role", "", "  admin  ", "", "  admin  "},
		{"non-empty role ignores admin default", "", "admin", "viewer", "viewer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, ResolveRole(&Policy{AdminRole: tt.adminRole, DefaultRole: tt.defaultRole}, tt.role))
		})
	}
}

func TestResolveRole_NilPolicy(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "editor", ResolveRole(nil, "editor"))
	assert.Equal(t, "", ResolveRole(nil, ""))
}

// TestEvaluate_DefaultRoleSubstitution: a roleless request is evaluated as the
// configured default_role and gets that role's permissions.
func TestEvaluate_DefaultRoleSubstitution(t *testing.T) {
	t.Parallel()
	p := &Policy{
		DefaultRole: "viewer",
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	perms := Evaluate(p, "", "clicks", "select", nil)
	assert.True(t, perms.Allowed, "empty role should resolve to default_role viewer")
	assert.Equal(t, []string{"page"}, perms.AllowColumns)
}

// TestEvaluate_DefaultRoleUnset_RolelessDenied: with no default_role a roleless
// request still fails closed (preserves the #159/#172 guarantee).
func TestEvaluate_DefaultRoleUnset_RolelessDenied(t *testing.T) {
	t.Parallel()
	p := &Policy{
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	perms := Evaluate(p, "", "clicks", "select", nil)
	assert.False(t, perms.Allowed, "no default_role → roleless request denied")
}

// TestEvaluate_DefaultRole_DoesNotClobberRealRole: a non-empty role is used as
// itself, never replaced by default_role.
func TestEvaluate_DefaultRole_DoesNotClobberRealRole(t *testing.T) {
	t.Parallel()
	p := &Policy{
		DefaultRole: "viewer",
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
					"editor": {AllowColumns: []string{"page", "secret"}},
				},
			},
		},
	}
	perms := Evaluate(p, "editor", "clicks", "select", nil)
	assert.True(t, perms.Allowed)
	assert.Equal(t, []string{"page", "secret"}, perms.AllowColumns, "editor keeps its own perms")
}

// TestEvaluate_DefaultRoleAdmin_GrantsAdmin: a default_role equal to the admin
// role grants a roleless request full admin access. This is the local/dev
// convenience the store warns loudly about — ResolveRole maps the empty role to
// admin, and IsAdmin then bypasses the table policy.
func TestEvaluate_DefaultRoleAdmin_GrantsAdmin(t *testing.T) {
	t.Parallel()
	p := &Policy{
		DefaultRole: "admin",
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	perms := Evaluate(p, "", "clicks", "select", nil)
	assert.True(t, perms.Allowed, "default_role=admin grants a roleless request full admin access")
}

func TestValidate_AllowsDefaultRoleEqualToAdmin(t *testing.T) {
	t.Parallel()
	// default_role == admin is permitted (a local/dev convenience): it grants
	// every roleless request admin, and the store warns loudly, but it is not a
	// validation error. See DefaultRoleGrantsAdmin / TestStore_Put_WarnsDefaultRoleAdmin.
	assert.NoError(t, Validate(&Policy{DefaultRole: "admin", Tables: map[string]TablePolicy{}}))

	// Same for a custom admin_role.
	assert.NoError(t, Validate(&Policy{AdminRole: "superuser", DefaultRole: "superuser", Tables: map[string]TablePolicy{}}))

	// DefaultRoleGrantsAdmin pins exactly which values trip the warning: exact,
	// case-sensitive, no trimming, so these are NOT the admin role.
	assert.True(t, DefaultRoleGrantsAdmin(&Policy{DefaultRole: "admin"}))
	for _, dr := range []string{"service", "ADMIN", "Service", "  admin  "} {
		assert.False(t, DefaultRoleGrantsAdmin(&Policy{DefaultRole: dr}),
			"default_role %q is not the admin role", dr)
	}
}

func TestValidate_AllowsNormalDefaultRole(t *testing.T) {
	t.Parallel()
	assert.NoError(t, Validate(&Policy{DefaultRole: "viewer", Tables: map[string]TablePolicy{}}))
}
