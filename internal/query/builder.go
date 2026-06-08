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

// DefaultMaxRows is applied when no explicit LIMIT is specified and no policy MaxRows is set.
// This prevents unbounded queries from consuming excessive memory.
const DefaultMaxRows = 10000

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
		sinceTime, err := resolveTimeValue(timeRange.Since, bucketSeconds)
		if err != nil {
			return nil, nil, fmt.Errorf("time_range since: %w", err)
		}
		parts = append(parts, fmt.Sprintf("%s >= ?", timeRange.Column))
		params = append(params, sinceTime)

		if timeRange.Until != "" {
			untilTime, err := resolveTimeValue(timeRange.Until, bucketSeconds)
			if err != nil {
				return nil, nil, fmt.Errorf("time_range until: %w", err)
			}
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
//
// A value that isn't a timestamp (a plain string, a number, etc.) is a valid
// non-temporal filter value, so the parse "failure" is just the expected
// non-timestamp case — pass it through unchanged rather than treat it as an error.
func coerceFilterValue(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	// RFC3339Nano parses both fractional and whole-second RFC3339 input.
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return formatClickHouseTime(t)
	}
	return v
}

// clickHouseDateTimeLayout renders a time in ClickHouse's native DateTime text
// format. The fractional ".999999999" preserves sub-second precision when
// present and drops trailing zeros, so a whole-second time has no decimal point.
const clickHouseDateTimeLayout = "2006-01-02 15:04:05.999999999"

func formatClickHouseTime(t time.Time) string {
	return t.UTC().Format(clickHouseDateTimeLayout)
}

// dayWeekRe matches a duration component with a day ("d") or week ("w") unit —
// the two units time.ParseDuration rejects (it stops at hours). The magnitude
// may be fractional, e.g. "0.5d".
var dayWeekRe = regexp.MustCompile(`(\d+(?:\.\d+)?)([dw])`)

// expandDayWeek rewrites the day/week components of a duration string into hours
// so time.ParseDuration accepts them: "7d" → "168h", "2w" → "336h", "1d12h" →
// "24h12h" (ParseDuration sums repeated units). Components in units it already
// understands are left untouched, and a string with no day/week component is
// returned verbatim. This keeps the documented "7d"-style ranges working
// (sdk.md) without reimplementing duration parsing. RFC3339 timestamps contain
// no lowercase d/w, so they pass through unchanged.
func expandDayWeek(s string) string {
	return dayWeekRe.ReplaceAllStringFunc(s, func(m string) string {
		sub := dayWeekRe.FindStringSubmatch(m)
		n, err := strconv.ParseFloat(sub[1], 64)
		if err != nil {
			return m // regex guarantees a numeric sub[1]; never corrupt on surprise input
		}
		hoursPerUnit := 24.0
		if sub[2] == "w" {
			hoursPerUnit = 168.0
		}
		return strconv.FormatFloat(n*hoursPerUnit, 'f', -1, 64) + "h"
	})
}

// resolveTimeValue parses an RFC3339 timestamp or a relative duration like "1h",
// "30m", "7d" or "2w" and renders it as a ClickHouse DateTime literal (see
// formatClickHouseTime). When bucketSeconds > 0, timestamps are bucketed
// (truncated) to the nearest boundary.
//
// A value that is neither a duration nor a timestamp is rejected with an error
// (which the builder surfaces as a 400) rather than returned unchanged: passing
// a raw string to ClickHouse surfaces as an opaque DateTime parse error (#285).
//
// The output deliberately matches coerceFilterValue's format rather than RFC3339:
// a bare "…T…Z" string is rejected by DateTime64 columns.
func resolveTimeValue(val string, bucketSeconds int) (string, error) {
	// Try a relative duration first (e.g., "1h", "30m", "7d", "2w"). Go's
	// time.ParseDuration only understands units up to hours, so day/week
	// suffixes are pre-expanded to hours.
	if d, err := time.ParseDuration(expandDayWeek(val)); err == nil {
		return formatClickHouseTime(bucketTime(time.Now().UTC().Add(-d), bucketSeconds)), nil
	}
	// Try an absolute timestamp (RFC3339Nano accepts fractional and whole-second
	// input); normalise to UTC before bucketing.
	if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
		return formatClickHouseTime(bucketTime(t.UTC(), bucketSeconds)), nil
	}
	return "", fmt.Errorf("invalid time value %q: want an RFC3339 timestamp or a relative duration such as \"1h\", \"30m\", \"7d\", \"2w\"", val)
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
