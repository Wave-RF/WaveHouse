package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// DefaultMaxRows is the fallback result LIMIT applied when no explicit LIMIT is
// specified and no policy MaxRows is set — preventing an unbounded read. It is
// the default value of the operator-facing query.default_max_rows config
// knob (passed into Build), and the guard Build falls back to when that knob is
// misconfigured to 0.
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
// the schema (it must be a real, discovered column) AND authorized against
// perms, the caller's resolved column permissions. perms may be nil, which
// means "no policy" — every column is allowed (used by callers that gate access
// elsewhere, and by tests). Centralizing the authorization here, at the one
// place that already enumerates every column reference, is what keeps a denied
// column from slipping through any single clause (#223). perms also carries the
// role's row-level-security predicate and max_rows cap, which Build emits as
// part of the WHERE and LIMIT clauses it assembles — never spliced into the
// rendered SQL afterward (#322).
//
// Every identifier that reaches the SQL — columns, the table, aggregation
// aliases — is backtick-quoted via chsql.QuoteIdent, so the builder accepts any
// name ClickHouse accepts while remaining injection-safe. Values stay positional
// `?` parameters bound by the driver.
//
// Projection rules: SelectAll requests every readable column (expanded to the
// role's allow/deny set); an explicit Columns list projects exactly those (where
// "*" is a literal column name, not a wildcard); the two are mutually exclusive.
// A query with neither, and no aggregations, selects nothing → ErrEmptyProjection.
func Build(table string, q *StructuredQuery, schema *discovery.TableSchema, perms *policy.ResolvedPermissions, bucketSeconds, defaultMaxRows int) (*BuildResult, error) {
	if chsql.BindUnsafe(table) {
		return nil, fmt.Errorf("unsupported table name (contains '?'): %s", table)
	}
	if q.SelectAll && len(q.Columns) > 0 {
		return nil, ErrColumnsAndSelectAll
	}
	if q.SelectAll && len(q.Aggregations) > 0 {
		return nil, fmt.Errorf("select_all cannot be combined with aggregations")
	}

	colSet := schemaColumnSet(schema)

	// Validate + authorize every referenced column in one pass.
	if err := validateAndAuthorizeColumns(q, colSet, perms); err != nil {
		return nil, err
	}

	// Resolve the row projection (aggregations are appended separately below).
	projection, wildcard, err := resolveProjection(q, schema, perms)
	if err != nil {
		return nil, err
	}

	var params []any

	// SELECT clause: the resolved row columns (a bare "*" only for an unrestricted
	// SelectAll; every concrete name is quoted), then any aggregation expressions.
	selectParts := make([]string, 0, len(projection)+len(q.Aggregations)+1)
	if wildcard {
		selectParts = append(selectParts, "*")
	}
	for _, c := range projection {
		selectParts = append(selectParts, chsql.QuoteIdent(c))
	}
	for _, a := range q.Aggregations {
		selectParts = append(selectParts, aggregationExpr(a))
	}
	// resolveProjection returns ErrEmptyProjection for the nothing-to-select case,
	// so this should be unreachable. Guard fail-CLOSED anyway.
	if len(selectParts) == 0 {
		return nil, ErrEmptyProjection
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), chsql.QuoteIdent(table))

	// WHERE clause: the role's row-level-security predicate first (in both the
	// SQL and the positional params), then the caller's own filters — ANDed, so a
	// caller can narrow its row visibility but never widen past the policy. The
	// predicate is emitted structurally, in the same assembly as every other
	// clause; policy SQL is never spliced into rendered text afterward, which is
	// what let a crafted identifier swallow the predicate (#322).
	whereParts, whereParams, err := buildWhere(q.Filters, q.TimeRange, bucketSeconds)
	if err != nil {
		return nil, fmt.Errorf("building WHERE clause: %w", err)
	}
	if perms != nil && perms.WhereClause != "" {
		whereParts = append([]string{"(" + perms.WhereClause + ")"}, whereParts...)
		params = append(params, perms.WhereParams...)
	}
	params = append(params, whereParams...)
	if len(whereParts) > 0 {
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	// GROUP BY.
	if len(q.GroupBy) > 0 {
		groupCols := make([]string, len(q.GroupBy))
		for i, g := range q.GroupBy {
			groupCols[i] = chsql.QuoteIdent(g)
		}
		sql += " GROUP BY " + strings.Join(groupCols, ", ")
	}

	// ORDER BY.
	if len(q.OrderBy) > 0 {
		var orderParts []string
		for _, o := range q.OrderBy {
			dir := "ASC"
			if strings.ToLower(o.Dir) == "desc" {
				dir = "DESC"
			}
			orderParts = append(orderParts, fmt.Sprintf("%s %s", chsql.QuoteIdent(o.Column), dir))
		}
		sql += " ORDER BY " + strings.Join(orderParts, ", ")
	}

	// LIMIT — apply the caller's explicit limit, capped at the configured
	// default maximum (query.default_max_rows) and at the role's max_rows policy
	// cap, whichever is smaller. A misconfigured non-positive default falls back
	// to the DefaultMaxRows constant so a read can never be left unbounded or
	// clamped to LIMIT 0.
	maxRows := defaultMaxRows
	if maxRows <= 0 {
		maxRows = DefaultMaxRows
	}
	if perms != nil && perms.MaxRows > 0 && perms.MaxRows < maxRows {
		maxRows = perms.MaxRows
	}
	if q.Limit > 0 && q.Limit <= maxRows {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit)
	} else {
		sql += fmt.Sprintf(" LIMIT %d", maxRows)
	}

	return &BuildResult{SQL: sql, Params: params}, nil
}

// aggregationExpr renders a single aggregation as a SELECT expression, e.g.
// {Fn:"count", Column:"*", Alias:"n"} → "count(*) AS `n`". The function name was
// allowlisted by validateAndAuthorizeColumns; the column (unless it is the "*"
// of count(*)) and the alias are backtick-quoted, so any legal ClickHouse name
// is safe to emit.
func aggregationExpr(a Aggregation) string {
	arg := a.Column
	if arg != "*" {
		arg = chsql.QuoteIdent(arg)
	}
	expr := fmt.Sprintf("%s(%s)", a.Fn, arg)
	if a.Alias != "" {
		expr += " AS " + chsql.QuoteIdent(a.Alias)
	}
	return expr
}

// validateAndAuthorizeColumns checks, in a single pass, that every column the
// query references (1) exists in the schema — so only real, discovered columns
// reach the SQL — and (2) is permitted by the role's column allow/deny lists —
// blocking a denied column from leaking through ANY clause. This is the one
// chokepoint that enumerates every column-bearing field, so none can silently
// skip the policy check the way scattered handler checks did (#223).
//
// Identifier *escaping* is not done here — every identifier is backtick-quoted by
// chsql.QuoteIdent when it is emitted — so this pass only rejects names that are
// unknown, denied, or bind-unsafe (chsql.BindUnsafe). Note "*" in the projection
// columns is now a literal column name (validated/authorized like any other); the
// only special "*" is an aggregation's count(*) argument, which is the SQL
// all-rows token, not a column.
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
		// The alias is backtick-quoted by aggregationExpr, so any legal ClickHouse
		// name is safe against injection. The lone refusal is a '?', which would
		// break clickhouse-go's positional value binding.
		if chsql.BindUnsafe(a.Alias) {
			return fmt.Errorf("unsupported aggregation alias (contains '?'): %s", a.Alias)
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
			// Not a schema column — allow it as an aggregation-alias reference
			// (ORDER BY an aggregation's AS name). Aliases carry no column policy;
			// the aggregation that defines them was authorized above. The name is
			// backtick-quoted when emitted, so any content is safe except a '?'
			// (would break value binding, as for aliases).
			if chsql.BindUnsafe(o.Column) {
				return fmt.Errorf("unsupported order column (contains '?'): %s", o.Column)
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

// resolveProjection decides the row columns the SELECT clause projects
// (aggregations are appended separately by Build). It returns the concrete
// columns plus a wildcard flag (true ⇒ emit a bare SELECT *):
//
//   - SelectAll, unrestricted role → (nil, true): a bare SELECT * is safe because
//     the role may read every column.
//   - SelectAll, column-restricted role → (allowed columns, false): expanded to
//     exactly the role's allow/deny set so denied columns never reach the result;
//     a role entitled to no columns gets ErrNoReadableColumns (→ 403).
//   - explicit Columns → (those columns, false): already validated + authorized;
//     a literal "*" among them is quoted like any other name.
//   - no columns but aggregations present → (nil, false): an aggregation-only
//     query projects no row columns.
//   - nothing at all → ErrEmptyProjection (→ 200 [], a request for no data).
func resolveProjection(q *StructuredQuery, schema *discovery.TableSchema, perms *policy.ResolvedPermissions) (cols []string, wildcard bool, err error) {
	switch {
	case q.SelectAll:
		if !perms.RestrictsColumns() {
			return nil, true, nil
		}
		allowed := perms.AllowedProjection(schema.ColumnNames())
		if len(allowed) == 0 {
			return nil, false, ErrNoReadableColumns
		}
		return allowed, false, nil
	case len(q.Columns) > 0:
		return q.Columns, false, nil
	case len(q.Aggregations) > 0:
		return nil, false, nil
	default:
		return nil, false, ErrEmptyProjection
	}
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
		col := chsql.QuoteIdent(timeRange.Column)
		sinceTime, err := resolveTimeValue(timeRange.Since, bucketSeconds)
		if err != nil {
			return nil, nil, fmt.Errorf("time_range since: %w", err)
		}
		parts = append(parts, fmt.Sprintf("%s >= ?", col))
		params = append(params, sinceTime)

		if timeRange.Until != "" {
			untilTime, err := resolveTimeValue(timeRange.Until, bucketSeconds)
			if err != nil {
				return nil, nil, fmt.Errorf("time_range until: %w", err)
			}
			parts = append(parts, fmt.Sprintf("%s <= ?", col))
			params = append(params, untilTime)
		}
	}

	return parts, params, nil
}

func filterToSQL(f Filter) (string, []any, error) {
	col := chsql.QuoteIdent(f.Column)
	val := coerceFilterValue(f.Value)
	switch strings.ToLower(f.Op) {
	case "eq":
		return col + " = ?", []any{val}, nil
	case "neq":
		return col + " != ?", []any{val}, nil
	case "gt":
		return col + " > ?", []any{val}, nil
	case "gte":
		return col + " >= ?", []any{val}, nil
	case "lt":
		return col + " < ?", []any{val}, nil
	case "lte":
		return col + " <= ?", []any{val}, nil
	case "like":
		return col + " LIKE ?", []any{val}, nil
	case "in":
		if vals, ok := f.Value.([]any); ok && len(vals) > 0 {
			placeholders := strings.Repeat("?,", len(vals))
			placeholders = placeholders[:len(placeholders)-1]
			return fmt.Sprintf("%s IN (%s)", col, placeholders), vals, nil
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

// validateColumn confirms a column is real (present in the discovered schema)
// and bind-safe. It does NOT restrict the character set: any name ClickHouse
// allows is accepted, because the name is backtick-quoted by chsql.QuoteIdent
// when it reaches the SQL. Schema membership is the injection boundary for column
// references — only a name ClickHouse already reported can be used.
func validateColumn(col string, validCols map[string]bool) error {
	if !validCols[col] {
		return fmt.Errorf("unknown column: %s", col)
	}
	if chsql.BindUnsafe(col) {
		return fmt.Errorf("unsupported column name (contains '?'): %s", col)
	}
	return nil
}

func isValidAggFn(fn string) bool {
	// ASCII-only before folding: strings.ToLower's Unicode fold maps İ (U+0130)
	// to 'i', so "mİn" would pass the allowlist yet reach ClickHouse verbatim as
	// an unknown function — a 500 where a 400 belongs. Every allowlisted name is
	// ASCII, so any non-ASCII byte disqualifies outright.
	for i := 0; i < len(fn); i++ {
		if fn[i] >= utf8.RuneSelf {
			return false
		}
	}
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
