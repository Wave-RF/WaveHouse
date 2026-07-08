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
	Allowed      bool
	AllowColumns []string
	DenyColumns  []string
	WhereClause  string
	WhereParams  []any
	// rowFilter is the same row-level-security predicate as WhereClause/WhereParams,
	// kept in resolved form so the stream path can evaluate it in memory (RowVisible)
	// while the query path renders it to SQL. Both derive from one resolvePredicates
	// call in Evaluate, so the two read surfaces can't drift. See rowfilter.go.
	rowFilter           []resolvedPredicate
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

// wholeClaimRe matches a value that is EXACTLY one {{ jwt.path }} reference with
// no surrounding text (anchored, unlike claimTemplateRe). It lets the _in
// resolver pull an array-valued claim through whole — col IN (its elements) —
// rather than stringifying it as resolveTemplate would.
var wholeClaimRe = regexp.MustCompile(`^\{\{\s*jwt\.([a-zA-Z0-9_.]+)\s*\}\}$`)

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

// resolveRolePerms navigates the policy to the RolePermissions governing
// (role, table, operation), applying role resolution, the admin bypass, and
// default-deny. It returns admin=true for the unconditional-bypass role (perms is the
// zero value — the caller grants full access) and ok=false for any denial (perms is
// the zero value). Evaluate (query path) and CompileRowFilter (stream path) share
// this one navigation so they can never disagree on who is authorized or which policy
// entry applies.
func resolveRolePerms(p *Policy, role, table, operation string) (perms RolePermissions, admin, ok bool) {
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
		return RolePermissions{}, true, false
	}

	// Beyond here the role is non-admin (non-empty only if it carried a concrete
	// role or matched a real default_role); any failure to find a matching entry
	// is a plain deny.
	if p == nil {
		return RolePermissions{}, false, false
	}

	tp, found := p.Tables[table]
	if !found {
		// No policy for this table — default deny.
		return RolePermissions{}, false, false
	}

	var rolePerms map[string]RolePermissions
	switch operation {
	case "select":
		rolePerms = tp.Select
	case "insert":
		rolePerms = tp.Insert
	default:
		return RolePermissions{}, false, false
	}

	if rolePerms == nil {
		return RolePermissions{}, false, false
	}

	// An empty/absent role must never match a role entry — a roleless request is
	// authorized only after ResolveRole maps it to a concrete default_role above,
	// or via the admin role (handled by the short-circuit at the top). A stray ""
	// role key in a policy therefore grants nothing: the policy-side twin of the
	// empty-AllowedRoles-entry footgun closed for pipes in #159. Matching is
	// exact — there is no "*" any-role wildcard.
	if role == "" {
		return RolePermissions{}, false, false
	}
	perms, ok = rolePerms[role]
	return perms, false, ok
}

// Evaluate resolves a policy for a given role, table, and operation against JWT claims.
func Evaluate(p *Policy, role, table, operation string, claims map[string]any) *ResolvedPermissions {
	perms, admin, ok := resolveRolePerms(p, role, table, operation)
	if admin {
		return &ResolvedPermissions{Allowed: true}
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

	// Resolve the row-filter once into predicates, then render both read surfaces
	// from that single source so they can't drift: the query path binds them into a
	// SQL WHERE here; the stream path evaluates the same compiled predicates in memory
	// (CompiledRowFilter.RowVisible). resolveFilterPredicates fails closed on a
	// bind-unsafe column (see there).
	if len(perms.Filter) > 0 {
		preds, deny := resolveFilterPredicates(perms.Filter, claims)
		if deny {
			return &ResolvedPermissions{Allowed: false}
		}
		resolved.rowFilter = preds
		clauses, params := predicatesToSQL(preds)
		if len(clauses) > 0 {
			resolved.WhereClause = strings.Join(clauses, " AND ")
			resolved.WhereParams = params
		}
	}

	// Resolve check rules (for inserts).
	if len(perms.Check) > 0 {
		resolved.CheckClauses = make(map[string]any, len(perms.Check))
		for col, f := range perms.Check {
			switch {
			case f.Eq != nil:
				resolved.CheckClauses[col] = resolveTemplate(*f.Eq, claims)
			case f.In != nil:
				// A []any value marks a set-membership check (vs a scalar required
				// value); ingest enforces "inserted value must be one of these".
				resolved.CheckClauses[col] = resolveInValues(*f.In, claims)
			}
		}
	}

	return resolved
}

// resolveFilterPredicates resolves a role's row-filter map into predicates for the
// query path, first failing closed (deny=true) if any filter column is bind-unsafe: a
// '?' in the column would shift clickhouse-go's positional value binding — including
// this filter's own bound value — so the role is denied rather than the predicate
// dropped (which would widen row access) or a mis-bound query emitted. validateRolePerms
// rejects such a policy at write time; this is defense-in-depth (the resolver does not
// re-validate the policy it is handed). The stream path applies the same bind-unsafe
// gate in CompileRowFilter, so both read surfaces reject the same unsafe policy.
func resolveFilterPredicates(filters map[string]Filter, claims map[string]any) (preds []resolvedPredicate, deny bool) {
	if filterBindUnsafe(filters) {
		return nil, true
	}
	return resolvePredicates(filters, claims), false
}

// filterBindUnsafe reports whether any filter column can't be bound safely (see
// resolveFilterPredicates for why that denies the role). Claims-independent, so the
// stream can check it once per bucket in CompileRowFilter.
func filterBindUnsafe(filters map[string]Filter) bool {
	for col := range filters {
		if chsql.BindUnsafe(col) {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// Compiled row filter: claims-independent structure, per-subscriber claim binding.
//
// The stream fan-out resolves a role's row-filter ONCE per bucket into compiled
// predicates (CompileRowFilter), then binds each subscriber's claims at evaluation
// time (CompiledRowFilter.RowVisible). It is the deferred-binding twin of
// resolvePredicates, which eagerly resolves for the query path's single claim set.
// The two must agree on every (filter, claims) pair — that parity is pinned by
// TestCompiledRowFilter_MatchesEvaluatePath. The optimization is that a whole
// {{ jwt.path }} value binds via a map walk (navigateClaims) and a constant binds to
// itself, so a filtered high-fan-out topic pays no per-subscriber regexp/predicate
// allocation.
// -----------------------------------------------------------------------------

type valueKind uint8

const (
	valConstant valueKind = iota // literal, no claim template
	valClaim                     // exactly one {{ jwt.path }} — bound via navigateClaims
	valTemplate                  // text with embedded {{ jwt.… }} — bound via resolveTemplate
)

// compiledValue is a filter value with its claim-template structure classified once,
// so binding a subscriber's claims is a map walk (valClaim) or a no-op (valConstant)
// rather than a regexp pass.
type compiledValue struct {
	kind valueKind
	s    string   // valConstant literal, or valTemplate raw text
	path []string // valClaim: the pre-split jwt claim path
}

// compileScalarValue classifies an _eq/_neq/_gt/_lt value the way resolveTemplate reads
// it — a substring replace, with a whole-value (no surrounding text) {{ jwt.path }}
// short-circuited to a direct claim lookup. It matches on the UNtrimmed value so a
// space-padded ref stays a template, exactly as resolveTemplate would keep the spaces.
func compileScalarValue(v string) compiledValue {
	if m := wholeClaimRe.FindStringSubmatch(v); m != nil {
		return compiledValue{kind: valClaim, path: strings.Split(m[1], ".")}
	}
	if claimTemplateRe.MatchString(v) {
		return compiledValue{kind: valTemplate, s: v}
	}
	return compiledValue{kind: valConstant, s: v}
}

// compileInValue classifies an _in value the way resolveInValues reads it: a
// TrimSpace'd whole {{ jwt.path }} is a claim list; anything else is a single
// (possibly templated) value.
func compileInValue(v string) compiledValue {
	if m := wholeClaimRe.FindStringSubmatch(strings.TrimSpace(v)); m != nil {
		return compiledValue{kind: valClaim, path: strings.Split(m[1], ".")}
	}
	if claimTemplateRe.MatchString(v) {
		return compiledValue{kind: valTemplate, s: v}
	}
	return compiledValue{kind: valConstant, s: v}
}

// bindScalar resolves a scalar value against claims — the deferred twin of
// resolveTemplate. A whole-claim ref returns the claim's string form (no allocation
// when the claim is already a string; fmt.Sprint otherwise, matching resolveTemplate),
// and a missing claim resolves to "" exactly as resolveTemplate does.
func (cv compiledValue) bindScalar(claims map[string]any) string {
	switch cv.kind {
	case valClaim:
		val := navigateClaims(claims, cv.path)
		if val == nil {
			return ""
		}
		if s, ok := val.(string); ok {
			return s
		}
		return fmt.Sprint(val)
	case valTemplate:
		return resolveTemplate(cv.s, claims)
	case valConstant:
		return cv.s
	default:
		return cv.s // unreachable: every valueKind has an explicit case above
	}
}

// bindIn resolves an _in value against claims into the set of bound values (the
// deferred twin of resolveInValues). A claim list yields one bound value per element;
// any other value yields the single scalar-bound value.
func (cv compiledValue) bindIn(claims map[string]any) []string {
	if cv.kind == valClaim {
		switch v := navigateClaims(claims, cv.path).(type) {
		case nil:
			return nil
		case []any:
			out := make([]string, 0, len(v))
			for _, e := range v {
				out = append(out, fmt.Sprint(e))
			}
			return out
		default:
			return []string{fmt.Sprint(v)}
		}
	}
	return []string{cv.bindScalar(claims)}
}

// compiledPredicate is one row-filter comparison with its value structure compiled but
// the claim binding deferred to evaluation time.
type compiledPredicate struct {
	Column string
	Op     string
	Value  compiledValue
}

// compilePredicates compiles a role's row-filter into per-subscriber-bindable
// predicates, mirroring resolvePredicates' column/operator expansion (and order) so the
// compiled and resolved paths produce the same predicate set.
func compilePredicates(filters map[string]Filter) []compiledPredicate {
	var preds []compiledPredicate
	for col, f := range filters {
		if f.Eq != nil {
			preds = append(preds, compiledPredicate{col, "=", compileScalarValue(*f.Eq)})
		}
		if f.Neq != nil {
			preds = append(preds, compiledPredicate{col, "!=", compileScalarValue(*f.Neq)})
		}
		if f.Gt != nil {
			preds = append(preds, compiledPredicate{col, ">", compileScalarValue(*f.Gt)})
		}
		if f.Lt != nil {
			preds = append(preds, compiledPredicate{col, "<", compileScalarValue(*f.Lt)})
		}
		if f.In != nil {
			preds = append(preds, compiledPredicate{col, "in", compileInValue(*f.In)})
		}
	}
	return preds
}

// CompiledRowFilter is a role/table's row-level-security filter compiled once
// (claims-independent) so the stream fan-out can reuse it across every subscriber in a
// bucket: resolve it per bucket with CompileRowFilter, then call RowVisible per
// subscriber with that subscriber's claims. It binds claims at evaluation time, so a
// filtered high-fan-out topic pays no per-subscriber predicate resolution — the
// deferred-binding counterpart to the query path's resolvePredicates (which binds a
// single claim set eagerly for SQL).
type CompiledRowFilter struct {
	allowed bool
	preds   []compiledPredicate
}

// CompileRowFilter compiles the row-filter governing (role, table, operation),
// claims-independently. It shares Evaluate's role resolution, admin bypass, and
// default-deny (via resolveRolePerms) and the same bind-unsafe gate, so it agrees with
// the query path on who is authorized and which policy is rejected. Compile once per
// role bucket; the per-subscriber cost is then only RowVisible.
func CompileRowFilter(p *Policy, role, table, operation string) *CompiledRowFilter {
	perms, admin, ok := resolveRolePerms(p, role, table, operation)
	if admin {
		return &CompiledRowFilter{allowed: true} // admin: no predicates ⇒ every row visible
	}
	if !ok {
		return &CompiledRowFilter{allowed: false}
	}
	if filterBindUnsafe(perms.Filter) {
		return &CompiledRowFilter{allowed: false} // fail closed, as Evaluate does
	}
	return &CompiledRowFilter{allowed: true, preds: compilePredicates(perms.Filter)}
}

// resolvedPredicate is one row-filter comparison with its claim templates already
// resolved to concrete string values — the shared, render-agnostic form the query
// path turns into SQL (predicatesToSQL) and the stream path evaluates in memory
// (RowVisible). Op is one of "=", "!=", ">", "<", "in". Values holds one element
// for the scalar operators and zero-or-more for "in" (empty ⇒ matches no rows).
type resolvedPredicate struct {
	Column string
	Op     string
	Values []string
}

// resolvePredicates resolves each filter's claim templates into predicates for the
// query path. It compiles the filter — the single, claims-independent source the stream
// path also uses (CompileRowFilter) — then binds this request's claims, so the two read
// surfaces derive from one place and can't drift. Operator order within a column
// (=, !=, >, <, in) mirrors compilePredicates and the former inline SQL.
func resolvePredicates(filters map[string]Filter, claims map[string]any) []resolvedPredicate {
	var preds []resolvedPredicate
	for _, cp := range compilePredicates(filters) {
		preds = append(preds, cp.resolve(claims))
	}
	return preds
}

// resolve binds a compiled predicate's claim value(s) into a resolvedPredicate — the
// eager, query-path counterpart to compiledPredicate.visible (which the stream
// evaluates against the row in memory instead of materializing this form).
func (cp compiledPredicate) resolve(claims map[string]any) resolvedPredicate {
	if cp.Op == "in" {
		return resolvedPredicate{cp.Column, "in", cp.Value.bindIn(claims)}
	}
	return resolvedPredicate{cp.Column, cp.Op, []string{cp.Value.bindScalar(claims)}}
}

// predicatesToSQL renders resolved predicates into WHERE clauses and bound params.
func predicatesToSQL(preds []resolvedPredicate) ([]string, []any) {
	var clauses []string
	var params []any
	for _, p := range preds {
		// Quote the policy-authored column the same way the query builder quotes
		// caller columns, so a row-filter on a weird-but-legal column name (dots,
		// spaces, keywords) is emitted safely.
		qcol := chsql.QuoteIdent(p.Column)
		switch p.Op {
		case "in":
			if len(p.Values) == 0 {
				// An empty/unresolvable set fails closed: a row filter scoped to no
				// values matches no rows, never widening to all of them (the #224
				// fail-open). `IN ()` is not valid SQL, so emit a constant false.
				clauses = append(clauses, "1 = 0")
			} else {
				placeholders := strings.TrimSuffix(strings.Repeat("?,", len(p.Values)), ",")
				clauses = append(clauses, fmt.Sprintf("%s IN (%s)", qcol, placeholders))
				for _, v := range p.Values {
					params = append(params, v)
				}
			}
		default:
			clauses = append(clauses, fmt.Sprintf("%s %s ?", qcol, p.Op))
			params = append(params, p.Values[0])
		}
	}
	return clauses, params
}

// resolveFilters converts filter definitions with claim templates into SQL WHERE
// clauses. Retained as the predicates→SQL composition the query-path tests target.
func resolveFilters(filters map[string]Filter, claims map[string]any) ([]string, []any) {
	return predicatesToSQL(resolvePredicates(filters, claims))
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

// resolveInValues resolves a templated _in value into the set of bound values
// for a `col IN (?, …)` predicate. When the template is a single bare claim
// reference and that claim is a JSON array, every element becomes one value —
// the multi-tenant case, where a token's tenant_ids list scopes the predicate.
// A scalar claim (or any template with surrounding text) yields a single value,
// matching resolveTemplate. Elements are stringified like the other operators so
// policy filters stay uniformly string-valued. Returns nil when the claim is
// absent so the caller can fail the predicate closed.
func resolveInValues(tmpl string, claims map[string]any) []any {
	if m := wholeClaimRe.FindStringSubmatch(strings.TrimSpace(tmpl)); m != nil {
		switch v := navigateClaims(claims, strings.Split(m[1], ".")).(type) {
		case nil:
			return nil
		case []any:
			out := make([]any, 0, len(v))
			for _, e := range v {
				out = append(out, fmt.Sprint(e))
			}
			return out
		default:
			return []any{fmt.Sprint(v)}
		}
	}
	return []any{resolveTemplate(tmpl, claims)}
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
// filterColumns drops any event field it rejects. Keep it the one decision
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
// see — the projection counterpart to the stream path's filterColumns. A
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
		// The filter path enforces every operator (_eq/_neq/_gt/_lt/_in), so none is
		// rejected here — only the bind-unsafe column name above.
	}
	for col, f := range perms.Check {
		if chsql.BindUnsafe(col) {
			return fmt.Errorf("table %q, op %q, role %q: check column %q contains '?' (unsupported)", table, op, role, col)
		}
		// The check/insert path honors only _eq (a required value) and _in (a
		// required set); the comparison operators have no insert-time semantics and
		// no auto-inject, so reject them loudly at config load (#224).
		if f.Neq != nil || f.Gt != nil || f.Lt != nil {
			return fmt.Errorf("table %q, op %q, role %q: check column %q uses _neq/_gt/_lt, which check does not honor (use _eq or _in)", table, op, role, col)
		}
		// _eq and _in are both honored, but they carry different required-value
		// semantics (a single value vs. set membership) and Evaluate resolves only
		// one — _eq wins, silently dropping _in. Reject the ambiguous pair at config
		// load rather than enforce an arbitrary branch (the same accept-but-ignore
		// gap #224 closes).
		if f.Eq != nil && f.In != nil {
			return fmt.Errorf("table %q, op %q, role %q: check column %q sets both _eq and _in; use exactly one", table, op, role, col)
		}
	}
	return nil
}
