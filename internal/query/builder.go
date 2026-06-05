package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// validIdentifierRe matches safe SQL identifiers (letters, digits, underscores).
var validIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// DefaultMaxRows is applied when no explicit LIMIT is specified and no policy MaxRows is set.
// This prevents unbounded queries from consuming excessive memory.
const DefaultMaxRows = 10000

// BuildResult holds the generated SQL and bound parameters.
type BuildResult struct {
	SQL    string
	Params []any
}

// Build converts a StructuredQuery into parameterized ClickHouse SQL.
//
// Every column the query references — in the projection, an aggregation
// argument, a filter, group_by, order_by, or time_range — is validated against
// the schema (to prevent SQL injection via unknown identifiers) AND authorized
// against perms, the caller's resolved column permissions. perms may be nil,
// which means "no policy" — every column is allowed (used by callers that gate
// access elsewhere, and by tests). Centralizing the authorization here, at the
// one place that already enumerates every column reference, is what keeps a
// denied column from slipping through any single clause: scattering the checks
// across the handler is exactly what let group_by, order_by, filters, and the
// empty-or-"*" projection bypass the allowlist (#223).
func Build(table string, q *StructuredQuery, schema *discovery.TableSchema, perms *policy.ResolvedPermissions, bucketSeconds int) (*BuildResult, error) {
	colSet := schemaColumnSet(schema)

	// Validate + authorize every referenced column in one pass.
	if err := validateAndAuthorizeColumns(q, colSet, perms); err != nil {
		return nil, err
	}

	// Resolve the row projection. A full-row read (no explicit columns, or a "*"
	// wildcard) expands to the role's allowed columns when the role is column-
	// restricted, so the builder never emits a bare SELECT * that would return
	// denied columns; unrestricted roles keep SELECT * unchanged.
	projection, err := resolveProjection(q, schema, perms)
	if err != nil {
		return nil, err
	}

	var params []any

	// SELECT clause: resolved row columns followed by any aggregation expressions.
	selectParts := make([]string, 0, len(projection)+len(q.Aggregations))
	selectParts = append(selectParts, projection...)
	for _, a := range q.Aggregations {
		selectParts = append(selectParts, aggregationExpr(a))
	}
	// resolveProjection guarantees a non-empty projection (or an error) for a
	// full-row read, and an aggregation-only query fills selectParts above, so an
	// empty SELECT should be unreachable. Guard fail-CLOSED anyway: a bare
	// SELECT * fallback here would be exactly the fail-open this fix exists to
	// remove.
	if len(selectParts) == 0 {
		return nil, ErrNoReadableColumns
	}

	// Double-backtick escaping is the Go SQL driver standard for identifiers
	escapedTable := strings.ReplaceAll(table, "`", "``")
	sql := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(selectParts, ", "), escapedTable)

	// WHERE clause.
	whereParts, whereParams, err := buildWhere(q.Filters, q.TimeRange, bucketSeconds)
	if err != nil {
		return nil, fmt.Errorf("building WHERE clause: %w", err)
	}
	params = append(params, whereParams...)
	if len(whereParts) > 0 {
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	// GROUP BY.
	if len(q.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(q.GroupBy, ", ")
	}

	// ORDER BY.
	if len(q.OrderBy) > 0 {
		var orderParts []string
		for _, o := range q.OrderBy {
			dir := "ASC"
			if strings.ToLower(o.Dir) == "desc" {
				dir = "DESC"
			}
			orderParts = append(orderParts, fmt.Sprintf("%s %s", o.Column, dir))
		}
		sql += " ORDER BY " + strings.Join(orderParts, ", ")
	}

	// LIMIT — apply explicit or default maximum.
	if q.Limit > 0 && q.Limit <= DefaultMaxRows {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit)
	} else {
		sql += fmt.Sprintf(" LIMIT %d", DefaultMaxRows)
	}

	return &BuildResult{SQL: sql, Params: params}, nil
}

// InjectPermissionFilters adds policy-derived WHERE clauses to an existing BuildResult.
func InjectPermissionFilters(result *BuildResult, whereClause string, whereParams []any) {
	if whereClause == "" {
		return
	}
	if strings.Contains(result.SQL, " WHERE ") {
		result.SQL = strings.Replace(result.SQL, " WHERE ", " WHERE ("+whereClause+") AND ", 1)
	} else {
		// Insert WHERE before GROUP BY, ORDER BY, or LIMIT.
		insertPoint := findInsertPoint(result.SQL)
		result.SQL = result.SQL[:insertPoint] + " WHERE " + whereClause + result.SQL[insertPoint:]
	}
	result.Params = append(whereParams, result.Params...)
}

// ApplyMaxRows enforces a maximum row limit.
func ApplyMaxRows(result *BuildResult, maxRows int) {
	if maxRows <= 0 {
		return
	}
	// Check if LIMIT already exists.
	upper := strings.ToUpper(result.SQL)
	if idx := strings.LastIndex(upper, " LIMIT "); idx >= 0 {
		// Parse existing limit.
		after := strings.TrimSpace(result.SQL[idx+7:])
		if existing, err := strconv.Atoi(after); err == nil && existing > maxRows {
			result.SQL = result.SQL[:idx] + fmt.Sprintf(" LIMIT %d", maxRows)
		}
	} else {
		result.SQL += fmt.Sprintf(" LIMIT %d", maxRows)
	}
}

// aggregationExpr renders a single aggregation as a SELECT expression, e.g.
// {Fn:"count", Column:"*", Alias:"n"} → "count(*) AS n". The function name,
// column, and alias have all been validated/authorized by
// validateAndAuthorizeColumns, so they are safe to interpolate verbatim.
func aggregationExpr(a Aggregation) string {
	expr := fmt.Sprintf("%s(%s)", a.Fn, a.Column)
	if a.Alias != "" {
		expr += " AS " + a.Alias
	}
	return expr
}

// validateAndAuthorizeColumns checks, in a single pass, that every column the
// query references (1) exists in the schema — blocking SQL injection via unknown
// identifiers — and (2) is permitted by the role's column allowlist — blocking a
// denied column from leaking through ANY clause. This is the one chokepoint that
// enumerates every column-bearing field, so none can silently skip the policy
// check the way scattered handler checks did (#223 and its group_by / order_by /
// filter siblings).
//
// Two wildcards are intentionally not treated as concrete columns here:
//   - "*" in the projection columns, and an aggregation's count(*) argument, mean
//     "all columns" and are resolved by resolveProjection, not authorized as a
//     literal column.
//   - ORDER BY may name an aggregation alias (not a schema column); aliases carry
//     no column policy because the aggregation that defines them was authorized
//     above.
func validateAndAuthorizeColumns(q *StructuredQuery, colSet map[string]bool, perms *policy.ResolvedPermissions) error {
	authorize := func(col string) error {
		if !perms.IsColumnAllowed(col) {
			return &ForbiddenColumnError{Column: col}
		}
		return nil
	}
	check := func(col string) error {
		if err := validateColumn(col, colSet); err != nil {
			return err
		}
		return authorize(col)
	}

	for _, c := range q.Columns {
		if c == "*" {
			continue // all-columns wildcard, resolved by resolveProjection
		}
		if err := check(c); err != nil {
			return err
		}
	}
	for _, a := range q.Aggregations {
		if a.Column != "*" {
			if err := check(a.Column); err != nil {
				return err
			}
		}
		if !isValidAggFn(a.Fn) {
			return fmt.Errorf("unsupported aggregation function: %s", a.Fn)
		}
		if !perms.IsAggregationAllowed(a.Fn) {
			return &ForbiddenAggregationError{Fn: a.Fn}
		}
		// The alias is interpolated into the SELECT list verbatim by
		// aggregationExpr ("fn(col) AS <alias>"), so it must be a safe bare
		// identifier — an unvalidated alias is a SQL-injection vector (e.g.
		// "n FROM secrets --" reparents the whole query). Validate it exactly as
		// ORDER BY aliases are validated below.
		if a.Alias != "" && !validIdentifierRe.MatchString(a.Alias) {
			return fmt.Errorf("invalid aggregation alias: %s", a.Alias)
		}
	}
	for _, f := range q.Filters {
		if err := check(f.Column); err != nil {
			return err
		}
	}
	for _, g := range q.GroupBy {
		if err := check(g); err != nil {
			return err
		}
	}
	for _, o := range q.OrderBy {
		if err := validateColumn(o.Column, colSet); err != nil {
			// Not a schema column — only a valid-identifier alias is allowed
			// (e.g. ORDER BY an aggregation's AS name). Aliases are not policy-
			// checked; the aggregation that defines them already was.
			if !validIdentifierRe.MatchString(o.Column) {
				return fmt.Errorf("invalid order column: %s", o.Column)
			}
			continue
		}
		if err := authorize(o.Column); err != nil {
			return err
		}
	}
	if q.TimeRange != nil && q.TimeRange.Column != "" {
		if err := check(q.TimeRange.Column); err != nil {
			return err
		}
	}
	return nil
}

// resolveProjection returns the concrete columns the SELECT clause should project
// (aggregations are appended separately by Build). Explicit, non-wildcard columns
// pass through unchanged — they were already validated and authorized. A "*"
// wildcard, or a query with no columns and no aggregations, is a full-row read:
// for a column-restricted role it expands to the role's AllowedProjection so
// denied columns never reach the result; for an unrestricted role it stays "*".
// An aggregation-only query projects no row columns (returns nil). A restricted
// role that may read the table but no columns gets ErrNoReadableColumns rather
// than a fail-open SELECT *.
func resolveProjection(q *StructuredQuery, schema *discovery.TableSchema, perms *policy.ResolvedPermissions) ([]string, error) {
	wildcard := false
	explicit := make([]string, 0, len(q.Columns))
	for _, c := range q.Columns {
		if c == "*" {
			wildcard = true
			continue
		}
		explicit = append(explicit, c)
	}

	switch {
	case !wildcard && len(explicit) > 0:
		// Caller named concrete columns — use them verbatim.
		return explicit, nil
	case !wildcard && len(q.Aggregations) > 0:
		// Aggregation-only query: no row columns to project.
		return nil, nil
	}

	// Full-row read ("*" wildcard, or an empty query).
	if !perms.RestrictsColumns() {
		return []string{"*"}, nil
	}
	allowed := perms.AllowedProjection(schema.ColumnNames())
	if len(allowed) == 0 {
		return nil, ErrNoReadableColumns
	}
	return allowed, nil
}

func buildWhere(filters []Filter, timeRange *TimeRange, bucketSeconds int) ([]string, []any, error) {
	var parts []string
	var params []any

	for _, f := range filters {
		clause, p, err := filterToSQL(f)
		if err != nil {
			return nil, nil, fmt.Errorf("filter on column %q: %w", f.Column, err)
		}
		if clause == "" {
			// How did I get here...?
			return nil, nil, fmt.Errorf("empty WHERE clause for filter: %+v", f)
		}
		parts = append(parts, clause)
		params = append(params, p...)
	}

	if timeRange != nil && timeRange.Column != "" && timeRange.Since != "" {
		sinceTime := resolveTimeValue(timeRange.Since, bucketSeconds)
		parts = append(parts, fmt.Sprintf("%s >= ?", timeRange.Column))
		params = append(params, sinceTime)

		if timeRange.Until != "" {
			untilTime := resolveTimeValue(timeRange.Until, bucketSeconds)
			parts = append(parts, fmt.Sprintf("%s <= ?", timeRange.Column))
			params = append(params, untilTime)
		}
	}

	return parts, params, nil
}

func filterToSQL(f Filter) (string, []any, error) {
	val := coerceFilterValue(f.Value)
	switch strings.ToLower(f.Op) {
	case "eq":
		return f.Column + " = ?", []any{val}, nil
	case "neq":
		return f.Column + " != ?", []any{val}, nil
	case "gt":
		return f.Column + " > ?", []any{val}, nil
	case "gte":
		return f.Column + " >= ?", []any{val}, nil
	case "lt":
		return f.Column + " < ?", []any{val}, nil
	case "lte":
		return f.Column + " <= ?", []any{val}, nil
	case "like":
		return f.Column + " LIKE ?", []any{val}, nil
	case "in":
		if vals, ok := f.Value.([]any); ok && len(vals) > 0 {
			placeholders := strings.Repeat("?,", len(vals))
			placeholders = placeholders[:len(placeholders)-1]
			return fmt.Sprintf("%s IN (%s)", f.Column, placeholders), vals, nil
		}
		return "", nil, fmt.Errorf("invalid value for 'in' operator")
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", f.Op)
	}
}

// coerceFilterValue converts string values that look like RFC3339 timestamps
// to ClickHouse-compatible DateTime strings preserving sub-second precision.
// The clickhouse-go driver's time.Time formatting uses toDateTime() (second
// precision), which loses milliseconds needed for DateTime64 cursor comparisons.
// Returning a formatted string lets ClickHouse parse it with full precision.
func coerceFilterValue(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC().Format("2006-01-02 15:04:05.999999999")
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC().Format("2006-01-02 15:04:05")
	}
	return v
}

// resolveTimeValue parses an RFC3339 timestamp or a relative duration like "1h", "30m".
// When bucketSeconds > 0, timestamps are bucketed (truncated) to the nearest boundary.
// Failure to parse returns the original string, which will likely cause a ClickHouse error.
func resolveTimeValue(val string, bucketSeconds int) string {
	// Try relative duration first (e.g., "1h", "30m", "5m").
	if d, err := time.ParseDuration(val); err == nil {
		t := time.Now().UTC().Add(-d)
		return bucketTime(t, bucketSeconds).Format(time.RFC3339)
	}
	// Try RFC3339 timestamp.
	if t, err := time.Parse(time.RFC3339, val); err == nil {
		return bucketTime(t, bucketSeconds).Format(time.RFC3339)
	}
	// Try RFC3339Nano.
	if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
		return bucketTime(t, bucketSeconds).Format(time.RFC3339)
	}
	return val
}

// bucketTime truncates a time to the nearest bucket boundary.
func bucketTime(t time.Time, bucketSeconds int) time.Time {
	if bucketSeconds <= 0 {
		return t
	}
	d := time.Duration(bucketSeconds) * time.Second
	return t.Truncate(d)
}

func schemaColumnSet(schema *discovery.TableSchema) map[string]bool {
	m := make(map[string]bool, len(schema.Columns))
	for _, c := range schema.Columns {
		m[c.Name] = true
	}
	return m
}

func validateColumn(col string, validCols map[string]bool) error {
	if !validCols[col] {
		return fmt.Errorf("unknown column: %s", col)
	}
	if !validIdentifierRe.MatchString(col) {
		return fmt.Errorf("invalid column name: %s", col)
	}
	return nil
}

func isValidAggFn(fn string) bool {
	switch strings.ToLower(fn) {
	case "count", "sum", "avg", "min", "max",
		"countdistinct", "uniq", "uniqexact",
		"any", "anylast",
		"argmin", "argmax",
		"grouparray",
		"median", "quantile",
		"stddevpop", "stddevsamp",
		"varpop", "varsamp":
		return true
	}
	return false
}

func findInsertPoint(sql string) int {
	upper := strings.ToUpper(sql)
	for _, kw := range []string{" GROUP BY ", " ORDER BY ", " LIMIT "} {
		if idx := strings.Index(upper, kw); idx >= 0 {
			return idx
		}
	}
	return len(sql)
}
