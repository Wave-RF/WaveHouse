package policy

import (
	"fmt"
	"regexp"
	"strings"
)

// Policy is the top-level access control configuration.
type Policy struct {
	// DefaultRole is the role an empty/absent role resolves to (a tokenless or
	// roleless request). Empty means no public access. Setting it equal to
	// AdminRole grants every roleless request full admin — permitted as a
	// local/dev convenience, but the store warns loudly when such a policy is
	// adopted (see DefaultRoleGrantsAdmin); avoid it in production.
	DefaultRole string `json:"default_role" yaml:"default_role"`
	// AdminRole is the role granted full access and the allowlist bypass.
	// Configurable per org (case-sensitive, exact match); defaults to "admin"
	// when unset (see AdminRole()).
	AdminRole string                 `json:"admin_role,omitempty" yaml:"admin_role,omitempty"`
	Tables    map[string]TablePolicy `json:"tables" yaml:"tables"`
}

// TablePolicy defines access control for a single table.
type TablePolicy struct {
	Select map[string]RolePermissions `json:"select,omitempty" yaml:"select,omitempty"`
	Insert map[string]RolePermissions `json:"insert,omitempty" yaml:"insert,omitempty"`
}

// RolePermissions defines what a role can do on a table for a specific operation.
type RolePermissions struct {
	AllowColumns        []string          `json:"allow_columns,omitempty" yaml:"allow_columns,omitempty"`
	DenyColumns         []string          `json:"deny_columns,omitempty" yaml:"deny_columns,omitempty"`
	Filter              map[string]Filter `json:"filter,omitempty" yaml:"filter,omitempty"`
	Check               map[string]Filter `json:"check,omitempty" yaml:"check,omitempty"`
	AllowedAggregations []string          `json:"allowed_aggregations,omitempty" yaml:"allowed_aggregations,omitempty"`
	DeniedAggregations  []string          `json:"denied_aggregations,omitempty" yaml:"denied_aggregations,omitempty"`
	MaxRows             int               `json:"max_rows,omitempty" yaml:"max_rows,omitempty"`
	MaxExecutionTimeMs  int               `json:"max_execution_time_ms,omitempty" yaml:"max_execution_time_ms,omitempty"`
}

// Filter represents a single comparison operation.
type Filter struct {
	Eq  *string `json:"_eq,omitempty" yaml:"_eq,omitempty"`
	Neq *string `json:"_neq,omitempty" yaml:"_neq,omitempty"`
	Gt  *string `json:"_gt,omitempty" yaml:"_gt,omitempty"`
	Lt  *string `json:"_lt,omitempty" yaml:"_lt,omitempty"`
	In  *string `json:"_in,omitempty" yaml:"_in,omitempty"`
}

// ResolvedPermissions is the result of evaluating a policy against JWT claims.
type ResolvedPermissions struct {
	Allowed             bool
	AllowColumns        []string
	DenyColumns         []string
	WhereClause         string
	WhereParams         []any
	CheckClauses        map[string]any // column → required value (for inserts)
	AllowedAggregations []string
	DeniedAggregations  []string
	MaxRows             int
	MaxExecutionTimeMs  int
}

// claimTemplateRe matches {{ jwt.claim.path }} templates.
var claimTemplateRe = regexp.MustCompile(`\{\{\s*jwt\.([a-zA-Z0-9_.]+)\s*\}\}`)

// ResolveRole maps an empty/absent role to the policy's default_role, so a
// request with no role — a token without a role claim, or no token at all when
// public access is configured — is evaluated as the configured default.
// Matching is exact and case-sensitive (no normalization): roles are opaque
// strings, mirroring the admin check in IsAdmin. An empty default_role (no
// public access configured) leaves the role empty so the request fails closed.
// A default_role equal to the admin role IS honored — a roleless request then
// resolves to admin and receives full access — which is permitted as a local/dev
// convenience; the store warns loudly when such a policy is adopted (see
// DefaultRoleGrantsAdmin). A non-empty role is returned unchanged — roles do not
// inherit the default's permissions.
func ResolveRole(p *Policy, role string) string {
	if role != "" || p == nil {
		return role
	}
	return p.DefaultRole
}

// Evaluate resolves a policy for a given role, table, and operation against JWT claims.
func Evaluate(p *Policy, role, table, operation string, claims map[string]any) *ResolvedPermissions {
	// Map an empty/absent role to the configured default_role (no-op if none,
	// or if the policy is nil). A non-empty role is unchanged — roles never
	// inherit the default's permissions.
	role = ResolveRole(p, role)

	// Admin bypasses all policy: full, unrestricted access regardless of table,
	// operation, or any explicit per-role entry — an admin is never column- or
	// row-restricted, even by a policy that names the admin role. This single
	// early exit is the only admin branch, so every return below is the non-admin
	// path and denies plainly. It also folds in the nil-policy case: IsAdmin(nil)
	// is false (a deleted/absent policy is a total lockout — nobody passes, not
	// even admin; a fresh deployment is bootstrapped from the policy file, not
	// over HTTP), so a nil policy falls through to the deny just below. Mirrors
	// RoleAllowed's admin short-circuit on the pipe path.
	if IsAdmin(p, role) {
		return &ResolvedPermissions{Allowed: true}
	}

	// Beyond here the role is non-admin (non-empty only if it carried a concrete
	// role or matched a real default_role); any failure to find a matching entry
	// is a plain deny.
	if p == nil {
		return &ResolvedPermissions{Allowed: false}
	}

	tp, ok := p.Tables[table]
	if !ok {
		// No policy for this table — default deny.
		return &ResolvedPermissions{Allowed: false}
	}

	var rolePerms map[string]RolePermissions
	switch operation {
	case "select":
		rolePerms = tp.Select
	case "insert":
		rolePerms = tp.Insert
	default:
		return &ResolvedPermissions{Allowed: false}
	}

	if rolePerms == nil {
		return &ResolvedPermissions{Allowed: false}
	}

	// An empty/absent role must never match a role entry — a roleless request is
	// authorized only after ResolveRole maps it to a concrete default_role above,
	// or via the admin role (handled by the short-circuit at the top). A stray ""
	// role key in a policy therefore grants nothing: the policy-side twin of the
	// empty-AllowedRoles-entry footgun closed for pipes in #159. Matching is
	// exact — there is no "*" any-role wildcard.
	perms, ok := RolePermissions{}, false
	if role != "" {
		perms, ok = rolePerms[role]
	}
	if !ok {
		return &ResolvedPermissions{Allowed: false}
	}

	resolved := &ResolvedPermissions{
		Allowed:             true,
		AllowColumns:        perms.AllowColumns,
		DenyColumns:         perms.DenyColumns,
		AllowedAggregations: perms.AllowedAggregations,
		DeniedAggregations:  perms.DeniedAggregations,
		MaxRows:             perms.MaxRows,
		MaxExecutionTimeMs:  perms.MaxExecutionTimeMs,
	}

	// Resolve filters into WHERE clause.
	if len(perms.Filter) > 0 {
		clauses, params := resolveFilters(perms.Filter, claims)
		if len(clauses) > 0 {
			resolved.WhereClause = strings.Join(clauses, " AND ")
			resolved.WhereParams = params
		}
	}

	// Resolve check rules (for inserts).
	if len(perms.Check) > 0 {
		resolved.CheckClauses = make(map[string]any, len(perms.Check))
		for col, f := range perms.Check {
			if f.Eq != nil {
				resolved.CheckClauses[col] = resolveTemplate(*f.Eq, claims)
			}
		}
	}

	return resolved
}

// resolveFilters converts filter definitions with claim templates into SQL WHERE clauses.
func resolveFilters(filters map[string]Filter, claims map[string]any) ([]string, []any) {
	var clauses []string
	var params []any
	for col, f := range filters {
		if f.Eq != nil {
			val := resolveTemplate(*f.Eq, claims)
			clauses = append(clauses, fmt.Sprintf("%s = ?", col))
			params = append(params, val)
		}
		if f.Neq != nil {
			val := resolveTemplate(*f.Neq, claims)
			clauses = append(clauses, fmt.Sprintf("%s != ?", col))
			params = append(params, val)
		}
		if f.Gt != nil {
			val := resolveTemplate(*f.Gt, claims)
			clauses = append(clauses, fmt.Sprintf("%s > ?", col))
			params = append(params, val)
		}
		if f.Lt != nil {
			val := resolveTemplate(*f.Lt, claims)
			clauses = append(clauses, fmt.Sprintf("%s < ?", col))
			params = append(params, val)
		}
	}
	return clauses, params
}

// resolveTemplate resolves {{ jwt.claim.path }} templates against JWT claims.
// If a claim path cannot be resolved, the template placeholder is replaced with
// an empty string to prevent "<nil>" from leaking into SQL filters.
func resolveTemplate(tmpl string, claims map[string]any) string {
	return claimTemplateRe.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := claimTemplateRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return ""
		}
		parts := strings.Split(sub[1], ".")
		val := navigateClaims(claims, parts)
		if val == nil {
			return ""
		}
		return fmt.Sprint(val)
	})
}

// navigateClaims traverses nested claim maps using dot-separated path parts.
func navigateClaims(claims map[string]any, parts []string) any {
	if len(parts) == 0 || claims == nil {
		return nil
	}
	val, ok := claims[parts[0]]
	if !ok {
		return nil
	}
	if len(parts) == 1 {
		return val
	}
	nested, ok := val.(map[string]any)
	if !ok {
		return nil
	}
	return navigateClaims(nested, parts[1:])
}

// IsColumnAllowed checks if a column is permitted by the resolved permissions.
// A nil receiver means no policy applies — all columns are allowed.
//
// This is the single source of truth for the per-column read decision. Every
// read path defers to it: the query builder checks every column a structured
// query references against it (internal/query.Build), and the stream path's
// filterEventColumns drops any event field it rejects. Keep it the one decision
// function so the surfaces can never drift apart.
func (rp *ResolvedPermissions) IsColumnAllowed(col string) bool {
	if rp == nil {
		return true
	}
	if !rp.Allowed {
		return false
	}
	// A literal "*" is never a concrete, grantable column. It is the caller
	// asking for "everything", which the projection layer (AllowedProjection)
	// must resolve to the role's allowed columns — not something to wave through
	// here. Returning true would re-open the allowlist whenever AllowColumns is
	// empty or itself a wildcard (the deny-list footgun behind the structured
	// query SELECT * bypass, #223). A real column literally named "*" cannot
	// reach a query: it is not a valid SQL identifier, so the builder's
	// schema/identifier validation rejects it regardless.
	if col == "*" {
		return false
	}
	// Check deny list first.
	for _, d := range rp.DenyColumns {
		if d == col {
			return false
		}
	}
	// If allow list is empty or contains "*", all non-denied columns are allowed.
	if len(rp.AllowColumns) == 0 {
		return true
	}
	for _, a := range rp.AllowColumns {
		if a == "*" || a == col {
			return true
		}
	}
	return false
}

// AllowedProjection returns the subset of cols this role may read, preserving
// input order. It is the batch form of IsColumnAllowed and the single source of
// truth for expanding an unqualified "all columns" read (the SQL the builder
// would otherwise emit as SELECT *) into the concrete set a role is permitted to
// see — the projection counterpart to the stream path's filterEventColumns. A
// nil receiver (no policy) returns cols unchanged.
func (rp *ResolvedPermissions) AllowedProjection(cols []string) []string {
	if rp == nil {
		return cols
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if rp.IsColumnAllowed(c) {
			out = append(out, c)
		}
	}
	return out
}

// RestrictsColumns reports whether this role constrains which columns may be
// read — whether any column-level allow/deny rule applies. It is false only when
// the role can read every column (no deny list, and an allow list that is empty
// or a bare "*" wildcard); there a SELECT * exposes nothing the role isn't
// already entitled to, so the builder leaves it untouched. When true, the
// builder must expand SELECT * into AllowedProjection so denied columns never
// reach the result (#223). A nil receiver (no policy) is unrestricted. The
// allow/deny precedence here mirrors IsColumnAllowed exactly so the "is this
// role restricted?" and "is this column allowed?" questions can never disagree.
func (rp *ResolvedPermissions) RestrictsColumns() bool {
	if rp == nil {
		return false
	}
	if len(rp.DenyColumns) > 0 {
		return true
	}
	if len(rp.AllowColumns) == 0 {
		return false
	}
	for _, a := range rp.AllowColumns {
		if a == "*" {
			return false
		}
	}
	return true
}

// IsAggregationAllowed checks if an aggregation function is permitted.
// A nil receiver means no policy applies — all aggregations are allowed.
func (rp *ResolvedPermissions) IsAggregationAllowed(fn string) bool {
	if rp == nil {
		return true
	}
	if !rp.Allowed {
		return false
	}
	// Check deny list.
	for _, d := range rp.DeniedAggregations {
		if strings.ToLower(d) == fn {
			return false
		}
	}
	// If allow list is empty, all non-denied are allowed.
	if len(rp.AllowedAggregations) == 0 {
		return true
	}
	fn = strings.ToLower(fn)
	for _, a := range rp.AllowedAggregations {
		if strings.ToLower(a) == fn {
			return true
		}
	}
	return false
}

// Validate checks that a policy is well-formed.
func Validate(p *Policy) error {
	if p == nil {
		return fmt.Errorf("policy is nil")
	}
	// Note: a default_role equal to the admin role is intentionally allowed (it
	// grants every roleless request full admin — a local/dev convenience). The
	// store warns loudly when such a policy is adopted (see
	// DefaultRoleGrantsAdmin); it is not a validation error.
	for tableName, tp := range p.Tables {
		for role, perms := range tp.Select {
			if err := validateRolePerms(tableName, "select", role, perms); err != nil {
				return err
			}
		}
		for role, perms := range tp.Insert {
			if err := validateRolePerms(tableName, "insert", role, perms); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRolePerms(table, op, role string, perms RolePermissions) error {
	if strings.TrimSpace(role) == "" {
		return fmt.Errorf("table %q, op %q: empty role name is not allowed", table, op)
	}
	if perms.MaxRows < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_rows must be non-negative", table, op, role)
	}
	if perms.MaxExecutionTimeMs < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_execution_time_ms must be non-negative", table, op, role)
	}
	return nil
}
