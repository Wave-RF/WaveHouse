package policy

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluate_NilPolicy(t *testing.T) {
	t.Parallel()
	// A nil policy (none configured yet, or deleted from KV) fails fully closed:
	// nobody passes, not even the admin role — deleting the policy is a total
	// lockout. A fresh deployment is bootstrapped from the policy file, not over HTTP.
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

// resolved renders a template, requiring the renderable (ok=true) path — the
// counterpart tests for unrenderable claims assert ok=false explicitly.
func resolved(t *testing.T, tmpl string, claims map[string]any) string {
	t.Helper()
	s, ok := resolveTemplate(tmpl, claims)
	require.True(t, ok, "template %q must be renderable", tmpl)
	return s
}

func TestResolveTemplate(t *testing.T) {
	t.Parallel()
	claims := map[string]any{
		"org_id": "org-123",
		"nested": map[string]any{"val": "deep"},
	}
	assert.Equal(t, "org-123", resolved(t, "{{ jwt.org_id }}", claims))
	assert.Equal(t, "deep", resolved(t, "{{ jwt.nested.val }}", claims))
	assert.Equal(t, "", resolved(t, "{{ jwt.missing }}", claims))
	assert.Equal(t, "literal", resolved(t, "literal", claims))
}

func TestResolveTemplate_NilClaims(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", resolved(t, "{{ jwt.org_id }}", nil))
}

func TestResolveTemplate_MultipleTemplates(t *testing.T) {
	t.Parallel()
	claims := map[string]any{"a": "1", "b": "2"}
	assert.Equal(t, "1-2", resolved(t, "{{ jwt.a }}-{{ jwt.b }}", claims))
}

// TestResolveTemplate_NumericClaims pins how numeric claims render into filter
// and check constants: json.Number (what jwt.Parse yields since WithJSONNumber)
// passes through with its exact digits; a float64 below 2^53 renders positionally
// (fmt.Sprint's "1e+06" spelling would bind a constant ClickHouse integer columns
// reject); a float64 at or past 2^53 has already collapsed onto its float
// neighbors, so it is unrenderable — ok=false — rather than a stand-in that could
// name another principal.
func TestResolveTemplate_NumericClaims(t *testing.T) {
	t.Parallel()
	claims := map[string]any{
		"exact": json.Number("10000000000000001"),
		"round": float64(1_000_000),
		"frac":  float64(1.5),
		"big":   float64(10000000000000001), // already 1e16 by the time it's a float64
	}
	assert.Equal(t, "10000000000000001", resolved(t, "{{ jwt.exact }}", claims))
	assert.Equal(t, "1000000", resolved(t, "{{ jwt.round }}", claims))
	assert.Equal(t, "1.5", resolved(t, "{{ jwt.frac }}", claims))

	_, ok := resolveTemplate("{{ jwt.big }}", claims)
	assert.False(t, ok, "a float64 at/past 2^53 lost its digits upstream — refuse, never guess")
}

func TestValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		policy  *Policy
		wantErr bool
		wantMsg string
	}{
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

// TestResolveFilters_NumericClaimBinding pins the SQL surface of claimString: a
// json.Number claim binds its exact digits; a round float64 below 2^53 binds
// positionally ("1000000", never the "1e+06" ClickHouse integer columns reject);
// an unrenderable claim — float64 at/past 2^53, alone or as one element of an
// _in set — renders the whole predicate as `1 = 0`, matching no rows, the same
// verdict RowVisible reaches in memory.
func TestResolveFilters_NumericClaimBinding(t *testing.T) {
	t.Parallel()
	eq := map[string]Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}

	clauses, params := resolveFilters(eq, map[string]any{"tenant": json.Number("10000000000000001")})
	require.Equal(t, []string{"`tenant_id` = ?"}, clauses)
	assert.Equal(t, []any{"10000000000000001"}, params, "json.Number binds exact digits")

	clauses, params = resolveFilters(eq, map[string]any{"tenant": float64(1_000_000)})
	require.Equal(t, []string{"`tenant_id` = ?"}, clauses)
	assert.Equal(t, []any{"1000000"}, params, "small float binds positionally, not 1e+06")

	clauses, params = resolveFilters(eq, map[string]any{"tenant": float64(10000000000000001)})
	assert.Equal(t, []string{"1 = 0"}, clauses, "lossy float64 claim matches no rows")
	assert.Empty(t, params)

	in := map[string]Filter{"tenant_id": {In: new("{{ jwt.tenants }}")}}
	clauses, params = resolveFilters(in, map[string]any{"tenants": []any{"a", float64(1 << 60)}})
	assert.Equal(t, []string{"1 = 0"}, clauses, "one poisoned element resolves the whole set empty")
	assert.Empty(t, params)
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

// TestResolveFilters_InEmptyClaim_FailsClosed: an empty set makes the predicate
// match no rows (a constant-false predicate) rather than widen to all rows — the
// fail-closed direction. `IN ()` is invalid SQL. Two distinct branches of
// resolveInValues reach this: an absent claim (navigateClaims returns nil → the
// `case nil` branch) and a present-but-empty array (the `case []any` branch with
// zero elements). Both must fail closed.
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
// config-load error (#224). _in and _eq are accepted (covered elsewhere).
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
// switch honors _eq and silently drops _in — so it is rejected at config load
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
