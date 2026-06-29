package policy

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
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
	// MaxExecutionTime, MaxRowsToRead, and MaxMemoryUsage are enforced
	// server-side by ClickHouse (the max_execution_time / max_rows_to_read /
	// max_memory_usage settings, #316), not just as a client-side context
	// deadline. They cap wall-clock time, rows scanned, and peak query memory so
	// a heavy aggregation can't exhaust the box within the time budget.
	// MaxExecutionTime and MaxMemoryUsage are human-readable scalars ("5s",
	// "4GiB") to match clickhouse.query_timeout; MaxRowsToRead is a plain count
	// (int64, since it can exceed 2^31 on a large table).
	MaxExecutionTime Millis   `json:"max_execution_time,omitempty" yaml:"max_execution_time,omitempty"`
	MaxRowsToRead    int64    `json:"max_rows_to_read,omitempty" yaml:"max_rows_to_read,omitempty"`
	MaxMemoryUsage   ByteSize `json:"max_memory_usage,omitempty" yaml:"max_memory_usage,omitempty"`
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
	MaxExecutionTime    Millis
	MaxRowsToRead       int64
	MaxMemoryUsage      ByteSize
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
		MaxExecutionTime:    perms.MaxExecutionTime,
		MaxRowsToRead:       perms.MaxRowsToRead,
		MaxMemoryUsage:      perms.MaxMemoryUsage,
	}

	// Resolve filters into WHERE clause. A bind-unsafe filter column can't be
	// emitted safely — a '?' in it would shift clickhouse-go's positional value
	// binding, including this RLS filter's own bound value — so deny the role
	// fail-closed rather than drop the predicate (which would widen row access)
	// or emit a mis-bound query. validateRolePerms rejects such a policy at write
	// time; this guards the query path as defense-in-depth (Evaluate does not
	// re-validate the policy it is handed).
	if len(perms.Filter) > 0 {
		for col := range perms.Filter {
			if chsql.BindUnsafe(col) {
				return &ResolvedPermissions{Allowed: false}
			}
		}
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
		// Quote the policy-authored column the same way the query builder quotes
		// caller columns, so a row-filter on a weird-but-legal column name (dots,
		// spaces, keywords) is emitted safely.
		qcol := chsql.QuoteIdent(col)
		if f.Eq != nil {
			val := resolveTemplate(*f.Eq, claims)
			clauses = append(clauses, fmt.Sprintf("%s = ?", qcol))
			params = append(params, val)
		}
		if f.Neq != nil {
			val := resolveTemplate(*f.Neq, claims)
			clauses = append(clauses, fmt.Sprintf("%s != ?", qcol))
			params = append(params, val)
		}
		if f.Gt != nil {
			val := resolveTemplate(*f.Gt, claims)
			clauses = append(clauses, fmt.Sprintf("%s > ?", qcol))
			params = append(params, val)
		}
		if f.Lt != nil {
			val := resolveTemplate(*f.Lt, claims)
			clauses = append(clauses, fmt.Sprintf("%s < ?", qcol))
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
	// Precedence is most-restrictive-wins: the deny list is consulted before the
	// allow list, so a column in BOTH is denied. The order of the two loops is
	// cosmetic — the result is the conjunction "in allow AND not in deny" either
	// way; what would be unsafe is allow-WINS (returning true before consulting
	// deny), which we never do. See access-control.md "deny_columns always wins".
	//
	// "*" carries no special meaning here: in a query it is a literal column name,
	// decided by these same rules (and gated by schema membership in the builder,
	// so it only resolves when a real column is named "*"). The all-columns
	// wildcard is the caller's SelectAll, expanded by AllowedProjection — never a
	// bare "*" run through this function, which was the #223 footgun and is now
	// closed structurally.
	for _, d := range rp.DenyColumns {
		if d == col {
			return false
		}
	}
	// An empty allow list (or one containing the "*" wildcard token) means "all
	// columns": every non-denied column is permitted. A non-empty allow list
	// without "*" is an allowlist — only its members (never the denied) pass.
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
// reach the result (#223). A nil receiver (no policy) is unrestricted; a denied
// role (`Allowed` false) restricts everything. The precedence here mirrors
// IsColumnAllowed exactly — including the `!Allowed` deny-all — so the "is this
// role restricted?" and "is this column allowed?" questions can never disagree.
func (rp *ResolvedPermissions) RestrictsColumns() bool {
	if rp == nil {
		return false
	}
	// A denied role can read no column, so it restricts everything. Without this
	// guard RestrictsColumns would return false ("unrestricted") for an
	// `Allowed:false` receiver while IsColumnAllowed denies all — and
	// resolveProjection's SelectAll branch trusts RestrictsColumns, so the
	// disagreement would emit a bare `SELECT *` over every column. Fail closed.
	if !rp.Allowed {
		return true
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
	// Normalize case once so both deny and allow checks are case-insensitive;
	// otherwise a cased aggregation (SUM) bypasses a lowercase deny entry (sum).
	fn = strings.ToLower(fn)
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
	if perms.MaxExecutionTime < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_execution_time must be non-negative", table, op, role)
	}
	if perms.MaxRowsToRead < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_rows_to_read must be non-negative", table, op, role)
	}
	if perms.MaxMemoryUsage < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_memory_usage must be non-negative", table, op, role)
	}
	// Filter and check column names are interpolated into SQL (backtick-quoted) at
	// query time, so a '?' in one would shift clickhouse-go's positional value
	// binding. Refuse such a policy at write time, mirroring the query builder's
	// chsql.BindUnsafe guard on caller-supplied columns.
	for col := range perms.Filter {
		if chsql.BindUnsafe(col) {
			return fmt.Errorf("table %q, op %q, role %q: filter column %q contains '?' (unsupported)", table, op, role, col)
		}
	}
	for col := range perms.Check {
		if chsql.BindUnsafe(col) {
			return fmt.Errorf("table %q, op %q, role %q: check column %q contains '?' (unsupported)", table, op, role, col)
		}
	}
	return nil
}
