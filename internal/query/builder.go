package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// validIdentifierRe matches safe SQL identifiers (letters, digits, underscores).
var validIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// BuildResult holds the generated SQL and bound parameters.
type BuildResult struct {
	SQL    string
	Params []any
}

// Build converts a StructuredQuery into parameterized ClickHouse SQL.
// All column names are validated against the schema to prevent SQL injection.
func Build(table string, q *StructuredQuery, schema *discovery.TableSchema, bucketSeconds int) (*BuildResult, error) {
	colSet := schemaColumnSet(schema)

	// Validate all referenced columns against schema.
	for _, c := range q.Columns {
		if c == "*" {
			continue
		}
		if err := validateColumn(c, colSet); err != nil {
			return nil, err
		}
	}
	for _, a := range q.Aggregations {
		if a.Column != "*" {
			if err := validateColumn(a.Column, colSet); err != nil {
				return nil, err
			}
		}
		if !isValidAggFn(a.Fn) {
			return nil, fmt.Errorf("unsupported aggregation function: %s", a.Fn)
		}
	}
	for _, f := range q.Filters {
		if err := validateColumn(f.Column, colSet); err != nil {
			return nil, err
		}
	}
	for _, g := range q.GroupBy {
		if err := validateColumn(g, colSet); err != nil {
			return nil, err
		}
	}
	for _, o := range q.OrderBy {
		if err := validateColumn(o.Column, colSet); err != nil {
			// Allow ordering by alias.
			if !validIdentifierRe.MatchString(o.Column) {
				return nil, fmt.Errorf("invalid order column: %s", o.Column)
			}
		}
	}
	if q.TimeRange != nil && q.TimeRange.Column != "" {
		if err := validateColumn(q.TimeRange.Column, colSet); err != nil {
			return nil, err
		}
	}

	var params []any

	// SELECT clause.
	selectParts := buildSelectParts(q)
	if len(selectParts) == 0 {
		selectParts = []string{"*"}
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), table)

	// WHERE clause.
	whereParts, whereParams := buildWhere(q.Filters, q.TimeRange, bucketSeconds)
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

	// LIMIT.
	if q.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit)
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

func buildSelectParts(q *StructuredQuery) []string {
	var parts []string
	parts = append(parts, q.Columns...)
	for _, a := range q.Aggregations {
		expr := fmt.Sprintf("%s(%s)", a.Fn, a.Column)
		if a.Alias != "" {
			expr += " AS " + a.Alias
		}
		parts = append(parts, expr)
	}
	return parts
}

func buildWhere(filters []Filter, timeRange *TimeRange, bucketSeconds int) ([]string, []any) {
	var parts []string
	var params []any

	for _, f := range filters {
		clause, p := filterToSQL(f)
		if clause != "" {
			parts = append(parts, clause)
			params = append(params, p...)
		}
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

	return parts, params
}

func filterToSQL(f Filter) (string, []any) {
	switch strings.ToLower(f.Op) {
	case "eq":
		return f.Column + " = ?", []any{f.Value}
	case "neq":
		return f.Column + " != ?", []any{f.Value}
	case "gt":
		return f.Column + " > ?", []any{f.Value}
	case "gte":
		return f.Column + " >= ?", []any{f.Value}
	case "lt":
		return f.Column + " < ?", []any{f.Value}
	case "lte":
		return f.Column + " <= ?", []any{f.Value}
	case "like":
		return f.Column + " LIKE ?", []any{f.Value}
	case "in":
		if vals, ok := f.Value.([]any); ok && len(vals) > 0 {
			placeholders := strings.Repeat("?,", len(vals))
			placeholders = placeholders[:len(placeholders)-1]
			return fmt.Sprintf("%s IN (%s)", f.Column, placeholders), vals
		}
		return "", nil
	default:
		return "", nil
	}
}

// resolveTimeValue parses an RFC3339 timestamp or a relative duration like "1h", "30m".
// When bucketSeconds > 0, timestamps are bucketed (truncated) to the nearest boundary.
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
