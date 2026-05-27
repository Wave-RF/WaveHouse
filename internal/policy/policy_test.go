package policy

import (
	"testing"

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
	assert.Contains(t, perms.WhereClause, "org_id = ?")
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
						MaxExecutionTimeMs:  5000,
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
	assert.Equal(t, 5000, perms.MaxExecutionTimeMs)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.perms.IsColumnAllowed(tt.col)
			assert.Equal(t, tt.want, got)
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
		"org_id": "org-123",
		"nested": map[string]any{"val": "deep"},
	}
	assert.Equal(t, "org-123", resolveTemplate("{{ jwt.org_id }}", claims))
	assert.Equal(t, "deep", resolveTemplate("{{ jwt.nested.val }}", claims))
	assert.Equal(t, "", resolveTemplate("{{ jwt.missing }}", claims))
	assert.Equal(t, "literal", resolveTemplate("literal", claims))
}

func TestResolveTemplate_NilClaims(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", resolveTemplate("{{ jwt.org_id }}", nil))
}

func TestResolveTemplate_MultipleTemplates(t *testing.T) {
	t.Parallel()
	claims := map[string]any{"a": "1", "b": "2"}
	result := resolveTemplate("{{ jwt.a }}-{{ jwt.b }}", claims)
	assert.Equal(t, "1-2", result)
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
							"user": {MaxExecutionTimeMs: -500},
						},
					},
				},
			},
			wantErr: true,
			wantMsg: "max_execution_time_ms",
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
	assert.Contains(t, clauses[0], "price < ?")
	assert.Equal(t, "100", params[0])
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
