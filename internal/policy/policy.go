package policy

import (
	"fmt"
	"regexp"
	"strings"
)

// Policy is the top-level access control configuration.
type Policy struct {
	DefaultRole string                 `json:"default_role" yaml:"default_role"`
	Tables      map[string]TablePolicy `json:"tables" yaml:"tables"`
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
	RawSQL              bool              `json:"raw_sql,omitempty" yaml:"raw_sql,omitempty"`
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
	RawSQL              bool
}

// claimTemplateRe matches {{ jwt.claim.path }} templates.
var claimTemplateRe = regexp.MustCompile(`\{\{\s*jwt\.([a-zA-Z0-9_.]+)\s*\}\}`)

// Evaluate resolves a policy for a given role, table, and operation against JWT claims.
func Evaluate(p *Policy, role, table, operation string, claims map[string]any) *ResolvedPermissions {
	if p == nil {
		return &ResolvedPermissions{Allowed: true, RawSQL: true}
	}

	tp, ok := p.Tables[table]
	if !ok {
		// No policy for this table — default deny for non-admin.
		if role == "admin" || role == "service" {
			return &ResolvedPermissions{Allowed: true, RawSQL: true}
		}
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
		if role == "admin" || role == "service" {
			return &ResolvedPermissions{Allowed: true, RawSQL: true}
		}
		return &ResolvedPermissions{Allowed: false}
	}

	perms, ok := rolePerms[role]
	if !ok {
		// Try wildcard role.
		perms, ok = rolePerms["*"]
		if !ok {
			if role == "admin" || role == "service" {
				return &ResolvedPermissions{Allowed: true, RawSQL: true}
			}
			return &ResolvedPermissions{Allowed: false}
		}
	}

	resolved := &ResolvedPermissions{
		Allowed:             true,
		AllowColumns:        perms.AllowColumns,
		DenyColumns:         perms.DenyColumns,
		AllowedAggregations: perms.AllowedAggregations,
		DeniedAggregations:  perms.DeniedAggregations,
		MaxRows:             perms.MaxRows,
		MaxExecutionTimeMs:  perms.MaxExecutionTimeMs,
		RawSQL:              perms.RawSQL,
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
func (rp *ResolvedPermissions) IsColumnAllowed(col string) bool {
	if rp == nil {
		return true
	}
	if !rp.Allowed {
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
	if perms.MaxRows < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_rows must be non-negative", table, op, role)
	}
	if perms.MaxExecutionTimeMs < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_execution_time_ms must be non-negative", table, op, role)
	}
	return nil
}
