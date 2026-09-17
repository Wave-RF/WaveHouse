package query

import (
	"encoding/json"
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

// BuildResult holds the generated SQL and its bound values. The SQL carries
// positional `?` placeholders, one per entry in Params, in left-to-right
// order. NamedParams turns that pair into the named-parameter form
// ClickHouse's HTTP interface takes.
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
// `?` parameters, which NamedParams turns into ClickHouse named parameters.
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
	// Build's one caller resolves for "select", so Select is non-nil here. A
	// mis-resolved grant never reaches this line, but by THREE different deniers
	// depending on the query's shape — naming only one would leave the other two
	// looking unguarded:
	//
	//   - names columns    → validateAndAuthorizeColumns → IsColumnAllowed
	//   - select_all only  → resolveProjection → RestrictsColumns/AllowedProjection
	//   - aggregations only → validateAndAuthorizeColumns → IsAggregationAllowed
	//
	// All three fail closed on a nil Select; all three are pinned by
	// TestBuild_InsertResolvedGrantIsRejected. The bare read is the backstop if
	// that ordering changes — it would panic rather than skip the filter. Do NOT
	// add a `perms.Select != nil` guard: it would emit an unfiltered query, the
	// silent widening the pointer shape exists to prevent.
	if perms != nil && perms.Select.WhereClause != "" {
		whereParts = append([]string{"(" + perms.Select.WhereClause + ")"}, whereParts...)
		params = append(params, perms.Select.WhereParams...)
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
	if perms != nil && perms.Select.MaxRows > 0 && perms.Select.MaxRows < maxRows {
		maxRows = perms.Select.MaxRows
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
		if !perms.IsColumnAllowed(col, false) {
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
		// shift the positional-to-named parameter rewrite (see NamedParams).
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
			// (would shift the parameter rewrite, as for aliases).
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
	// The value is bound, never rendered, so it reaches ClickHouse as the
	// caller wrote it — including a timestamp. WaveHouse used to rewrite an
	// RFC3339 value into ClickHouse's own spelling here; ClickHouse has
	// accepted the RFC3339 spelling itself since 26.5 (cast_string_to_date_
	// time_mode defaults to best_effort), and every surface that hands a
	// caller a timestamp to filter on — /v1/query, the SSE wire — already
	// emits ClickHouse's spelling, which is what the rewrite existed for.
	val := f.Value
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
		// One placeholder for the whole list, bound as an Array(String) — not
		// one placeholder per element. Both spellings answer identically, but
		// ClickHouse's HTTP interface caps a request at 999 query-string
		// fields (measured on 26.6.3.62: 999 ok, 1000 → "Too many form
		// fields"), so a per-element binding would fail an `in` list that the
		// 1 MiB request cap otherwise allows. One field per list raises the
		// ceiling to the ~64 KiB per-field cap instead.
		if vals, ok := f.Value.([]any); ok && len(vals) > 0 {
			return col + " IN ?", []any{vals}, nil
		}
		return "", nil, fmt.Errorf("invalid value for 'in' operator")
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", f.Op)
	}
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
// The output is ClickHouse's own DateTime spelling rather than RFC3339 so the
// bound value is unambiguous on every server version.
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

// ─── ClickHouse named-parameter binding ─────────────────────────────────────

// NamedParams rewrites the positional `?` placeholders in the built SQL into
// ClickHouse named parameters and renders each bound value as the text
// ClickHouse will read it back from. It returns the rewritten SQL and the
// values for `param_p0` … `param_pN-1`, positionally.
//
// Every scalar binds as `{pN:String}` and every list as `{pN:Array(String)}`
// — one rule for the whole product, shared with the row filters the type
// layer compiles. String is not a weaker binding than the column's own type:
// ClickHouse parses the parameter against the column on both sides of the
// comparison, so `UInt8 = {p:String}` with "256" is false and with "1.5" is a
// type error, matching what the server answers for the same literal (AUDIT
// §C.1, measured against 26.3 and 26.6).
//
// The rewrite is a left-to-right scan for `?`, which is exact for this SQL and
// only for this SQL: Build never renders a value or a string literal, and
// chsql.BindUnsafe rejects a `?` in any identifier it quotes — the same
// invariant positional binding relied on.
func (r *BuildResult) NamedParams() (string, []string, error) {
	if len(r.Params) == 0 {
		if strings.Contains(r.SQL, "?") {
			return "", nil, fmt.Errorf("query has placeholders but no bound values")
		}
		return r.SQL, nil, nil
	}

	params := make([]string, 0, len(r.Params))
	var b strings.Builder
	b.Grow(len(r.SQL) + len(r.Params)*12)

	rest := r.SQL
	for i, v := range r.Params {
		q := strings.IndexByte(rest, '?')
		if q < 0 {
			return "", nil, fmt.Errorf("query has %d bound values but only %d placeholders", len(r.Params), i)
		}
		text, kind, err := chParamValue(v)
		if err != nil {
			return "", nil, err
		}
		b.WriteString(rest[:q])
		fmt.Fprintf(&b, "{p%d:%s}", i, kind)
		params = append(params, text)
		rest = rest[q+1:]
	}
	if strings.Contains(rest, "?") {
		return "", nil, fmt.Errorf("query has more placeholders than the %d bound values", len(r.Params))
	}
	b.WriteString(rest)
	return b.String(), params, nil
}

// chParamValue renders one bound value as its ClickHouse query-parameter text
// and names the parameter type to declare it as.
func chParamValue(v any) (text, kind string, err error) {
	if vals, ok := v.([]any); ok {
		lit, err := chArrayLiteral(vals)
		if err != nil {
			return "", "", err
		}
		return lit, "Array(String)", nil
	}
	raw, err := chScalarText(v)
	if err != nil {
		return "", "", err
	}
	return chsql.EscapeStringParam(raw), "String", nil
}

// chScalarText is one scalar's value as plain text, before any encoding —
// what the caller means, not what the wire needs.
func chScalarText(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case json.Number:
		// The caller's own digits, not a float64 round-trip: 12.50 stays
		// "12.50" and an integer past 2^53 keeps every digit.
		return val.String(), nil
	case bool:
		return strconv.FormatBool(val), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case int:
		return strconv.Itoa(val), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case nil:
		// `col = NULL` is never true in SQL, so the driver quietly answered
		// "no rows"; an empty String parameter would instead compare against
		// the empty string, which is a different question. Refuse it (→ 400)
		// rather than answer a question the caller did not ask.
		return "", fmt.Errorf("filter value must not be null")
	default:
		return "", fmt.Errorf("unsupported filter value type %T", v)
	}
}

// chArrayLiteral renders a list as the `['a','b']` text an Array(String)
// query parameter is parsed from. A nested list has no place inside an `in`
// list, and the elements take quoteCHElement's encoding INSTEAD of
// chsql.EscapeStringParam's, not on top of it.
func chArrayLiteral(vals []any) (string, error) {
	var b strings.Builder
	b.WriteByte('[')
	for i, v := range vals {
		if _, isList := v.([]any); isList {
			return "", fmt.Errorf("nested list in an 'in' value")
		}
		text, err := chScalarText(v)
		if err != nil {
			return "", err
		}
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(quoteCHElement(text))
	}
	b.WriteByte(']')
	return b.String(), nil
}

// quoteCHElement wraps one already-rendered value as a single-quoted element
// of an Array(String) parameter literal. That literal is read as a quoted
// value rather than an escaped field — measured on 26.6.3.62, a raw tab or
// newline inside the quotes round-trips untouched — so only the quote and the
// backslash need encoding, and chsql.EscapeStringParam's encoding must NOT be
// applied on top of it.
func quoteCHElement(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' || s[i] == '\'' {
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	b.WriteByte('\'')
	return b.String()
}
