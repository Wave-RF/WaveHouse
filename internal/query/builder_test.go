package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSchema() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "clicks",
		Columns: []discovery.Column{
			{Name: "page", Type: "String"},
			{Name: "button", Type: "String"},
			{Name: "count", Type: "UInt64"},
			{Name: "ts", Type: "DateTime"},
			{Name: "org_id", Type: "String"},
		},
	}
}

func TestBuild_SimpleSelect(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page", "count"}, Limit: 10}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Equal(t, "SELECT `page`, `count` FROM `clicks` LIMIT 10", result.SQL)
	assert.Empty(t, result.Params)
}

func TestBuild_SelectStar(t *testing.T) {
	t.Parallel()
	// SelectAll (not omitted columns) is what produces a full-row read.
	result, err := Build("clicks", &StructuredQuery{SelectAll: true}, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `clicks` LIMIT 10000", result.SQL)
}

// TestBuild_EmptyProjection: a query naming no columns, no aggregations, and no
// SelectAll selects nothing → ErrEmptyProjection (handler maps it to 200 []).
// Omitting columns is fail-closed — you get nothing unless you ask, so a hidden
// column can never leak by simply leaving columns out.
func TestBuild_EmptyProjection(t *testing.T) {
	t.Parallel()
	cases := map[string]*StructuredQuery{
		"empty struct":          {},
		"empty columns slice":   {Columns: Columns{}},
		"filter but no project": {Filters: []Filter{{Column: "page", Op: "eq", Value: "x"}}},
		"limit only":            {Limit: 5},
	}
	for name, sq := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
			require.ErrorIs(t, err, ErrEmptyProjection)
		})
	}
}

// TestBuild_ColumnsAndSelectAll: setting both an explicit list and SelectAll is
// ambiguous → ErrColumnsAndSelectAll (handler maps it to 400).
func TestBuild_ColumnsAndSelectAll(t *testing.T) {
	t.Parallel()
	_, err := Build("clicks", &StructuredQuery{Columns: Columns{"page"}, SelectAll: true}, testSchema(), nil, 0, DefaultMaxRows)
	require.ErrorIs(t, err, ErrColumnsAndSelectAll)
}

// TestBuild_LiteralStarColumn: "*" in columns is a literal column name now, not a
// wildcard. It is quoted and must exist in the schema like any other column.
func TestBuild_LiteralStarColumn(t *testing.T) {
	t.Parallel()
	// Not in the schema → unknown column.
	_, err := Build("clicks", &StructuredQuery{Columns: Columns{"*"}}, testSchema(), nil, 0, DefaultMaxRows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")

	// Present in the schema → selected as the quoted literal `*`, never a wildcard.
	starSchema := &discovery.TableSchema{Name: "t", Columns: []discovery.Column{{Name: "*", Type: "String"}}}
	result, err := Build("t", &StructuredQuery{Columns: Columns{"*"}}, starSchema, nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Equal(t, "SELECT `*` FROM `t` LIMIT 10000", result.SQL)
}

func TestBuild_WithAggregation(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "total"}},
		GroupBy:      []string{"page"},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "count(*) AS `total`")
	assert.Contains(t, result.SQL, "GROUP BY `page`")
}

func TestBuild_AllFilterOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		op   string
		want string
	}{
		{"eq", "`page` = ?"},
		{"neq", "`page` != ?"},
		{"gt", "`page` > ?"},
		{"gte", "`page` >= ?"},
		{"lt", "`page` < ?"},
		{"lte", "`page` <= ?"},
		{"like", "`page` LIKE ?"},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{
				Columns: []string{"page"},
				Filters: []Filter{{Column: "page", Op: tt.op, Value: "test"}},
			}
			result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
			require.NoError(t, err)
			assert.Contains(t, result.SQL, tt.want)
		})
	}
}

func TestBuild_InFilter(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		Filters: []Filter{{Column: "page", Op: "in", Value: []any{"/home", "/about"}}},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "`page` IN (?,?)")
	assert.Len(t, result.Params, 2)
}

func TestBuild_OrderBy(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page", "count"},
		OrderBy: []OrderClause{{Column: "count", Dir: "desc"}, {Column: "page", Dir: "asc"}},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ORDER BY `count` DESC, `page` ASC")
}

func TestBuild_UnknownColumn(t *testing.T) {
	t.Parallel()
	_, err := Build("clicks", &StructuredQuery{Columns: []string{"nonexistent"}}, testSchema(), nil, 0, DefaultMaxRows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestBuild_InvalidAggFn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "drop_table", Column: "count"}}}
	_, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported aggregation")
}

func TestBuild_TimeRange(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns:   []string{"page"},
		TimeRange: &TimeRange{Column: "ts", Since: "2024-01-01T00:00:00Z"},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "`ts` >= ?")
	assert.Len(t, result.Params, 1)
}

func TestInjectPermissionFilters_WithWhere(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks` WHERE page = ?", Params: []any{"/home"}}
	InjectPermissionFilters(result, "org_id = ?", []any{"org-1"})
	assert.Contains(t, result.SQL, "(org_id = ?)")
	assert.Equal(t, []any{"org-1", "/home"}, result.Params)
}

func TestInjectPermissionFilters_WithoutWhere(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks` ORDER BY page"}
	InjectPermissionFilters(result, "org_id = ?", []any{"org-1"})
	assert.Contains(t, result.SQL, "WHERE org_id = ?")
	assert.Contains(t, result.SQL, "ORDER BY page")
}

func TestInjectPermissionFilters_Empty(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks`"}
	InjectPermissionFilters(result, "", nil)
	assert.Equal(t, "SELECT * FROM `clicks`", result.SQL)
}

func TestApplyMaxRows_NoLimit(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks`"}
	ApplyMaxRows(result, 100)
	assert.Contains(t, result.SQL, "LIMIT 100")
}

func TestApplyMaxRows_HigherExisting(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks` LIMIT 500"}
	ApplyMaxRows(result, 100)
	assert.Contains(t, result.SQL, "LIMIT 100")
}

func TestApplyMaxRows_LowerExisting(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks` LIMIT 50"}
	ApplyMaxRows(result, 100)
	assert.Contains(t, result.SQL, "LIMIT 50")
}

func TestApplyMaxRows_Zero(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM `clicks`"}
	ApplyMaxRows(result, 0)
	assert.NotContains(t, result.SQL, "LIMIT")
}

func TestIsValidAggFn(t *testing.T) {
	t.Parallel()
	for _, fn := range []string{"count", "sum", "avg", "min", "max", "uniq", "median"} {
		assert.True(t, isValidAggFn(fn), fn)
	}
	assert.False(t, isValidAggFn("drop_table"))
	assert.False(t, isValidAggFn(""))
}

func TestBuild_DefaultMaxRows_Applied(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}} // Limit: 0.
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, fmt.Sprintf("LIMIT %d", DefaultMaxRows))
}

func TestBuild_LimitExceedsDefaultMaxRows_Capped(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}, Limit: DefaultMaxRows + 1}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, fmt.Sprintf("LIMIT %d", DefaultMaxRows))
	assert.NotContains(t, result.SQL, fmt.Sprintf("LIMIT %d", DefaultMaxRows+1))
}

func TestBuild_LimitWithinRange_Respected(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}, Limit: 50}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "LIMIT 50")
}

// TestBuild_ConfigurableDefaultMaxRows pins that the default LIMIT is the value
// the caller passes (the query_limits.default_max_rows knob), both as the
// no-limit fallback and as the ceiling an over-large request is clamped to —
// and that a non-positive value falls back to the DefaultMaxRows constant so a
// read is never left unbounded or clamped to LIMIT 0.
func TestBuild_ConfigurableDefaultMaxRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		limit          int
		defaultMaxRows int
		wantLimit      int
	}{
		{name: "custom default applied when no limit", limit: 0, defaultMaxRows: 250, wantLimit: 250},
		{name: "request capped at custom default", limit: 999, defaultMaxRows: 250, wantLimit: 250},
		{name: "request under custom default respected", limit: 100, defaultMaxRows: 250, wantLimit: 100},
		{name: "custom default above the constant is honored", limit: 0, defaultMaxRows: 50000, wantLimit: 50000},
		{name: "zero falls back to the constant", limit: 0, defaultMaxRows: 0, wantLimit: DefaultMaxRows},
		{name: "negative falls back to the constant", limit: 0, defaultMaxRows: -1, wantLimit: DefaultMaxRows},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{Columns: []string{"page"}, Limit: tt.limit}
			result, err := Build("clicks", sq, testSchema(), nil, 0, tt.defaultMaxRows)
			require.NoError(t, err)
			assert.Contains(t, result.SQL, fmt.Sprintf("LIMIT %d", tt.wantLimit))
		})
	}
}

func TestBuild_InvalidFilterColumn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		Filters: []Filter{{Column: "nonexistent", Op: "eq", Value: "x"}},
	}
	_, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestBuild_InvalidGroupByColumn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		GroupBy: []string{"nonexistent"},
	}
	_, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestResolveTimeValue_RelativeDuration(t *testing.T) {
	t.Parallel()
	result, err := resolveTimeValue("1h", 0)
	require.NoError(t, err)
	assert.NotEmpty(t, result)
	assert.NotEqual(t, "1h", result, "relative duration should resolve to a timestamp")

	assert.NotContains(t, result, "T", "must not emit the RFC3339 T separator")
	assert.NotContains(t, result, "Z", "must not emit the RFC3339 Z zone suffix")
}

func TestResolveTimeValue_RFC3339(t *testing.T) {
	t.Parallel()

	result, err := resolveTimeValue("2024-01-01T00:00:00Z", 0)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01 00:00:00", result)
}

func TestResolveTimeValue_WithBucketing(t *testing.T) {
	t.Parallel()
	// With 60s buckets, a time at :30 should truncate to :00.
	result, err := resolveTimeValue("2024-01-01T12:34:30Z", 60)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01 12:34:00", result)
}

func TestExpandDayWeek(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"7d":    "168h", // documented day suffix (#285)
		"1d":    "24h",
		"2w":    "336h",
		"1w":    "168h",
		"0.5d":  "12h",    // fractional magnitude
		"1d12h": "24h12h", // ParseDuration sums repeated units
		"1h":    "1h",     // already a unit ParseDuration handles — untouched
		"30m":   "30m",
		"":      "", // no component — verbatim
	}
	for in, want := range cases {
		assert.Equal(t, want, expandDayWeek(in), "expandDayWeek(%q)", in)
	}
}

func TestResolveTimeValue_DayWeekSuffix(t *testing.T) {
	t.Parallel()
	// "7d"/"2w" are documented (sdk.md) but not Go durations; they must resolve
	// to a real ClickHouse DateTime, not fall through as the raw string (#285).
	for _, in := range []string{"7d", "2w", "1d12h"} {
		result, err := resolveTimeValue(in, 0)
		require.NoError(t, err, "resolveTimeValue(%q)", in)
		assert.NotEqual(t, in, result, "%q must resolve, not pass through raw", in)
		assert.NotContains(t, result, "T")
		assert.NotContains(t, result, "Z")
	}

	// "7d" must resolve to the same instant as its hour-equivalent "168h".
	// Bucket to the day so the sub-millisecond gap between the two time.Now()
	// reads can't make the comparison flaky.
	day := 86400
	d7, err := resolveTimeValue("7d", day)
	require.NoError(t, err)
	h168, err := resolveTimeValue("168h", day)
	require.NoError(t, err)
	assert.Equal(t, h168, d7, `"7d" and "168h" should resolve to the same bucketed time`)
}

func TestResolveTimeValue_Invalid(t *testing.T) {
	t.Parallel()
	// Neither a duration nor a timestamp: must fail closed (→ 400) instead of
	// reaching ClickHouse as a raw literal (#285).
	for _, in := range []string{"7dd", "banana", "7 days", "168", "7D"} {
		_, err := resolveTimeValue(in, 0)
		require.Error(t, err, "resolveTimeValue(%q) should error", in)
	}
}

func TestBucketTime_ZeroBucket(t *testing.T) {
	t.Parallel()
	ts, _ := time.Parse(time.RFC3339, "2024-01-01T12:34:56Z")
	got := bucketTime(ts, 0)
	assert.Equal(t, ts, got, "zero bucket should not truncate")
}

func TestFindInsertPoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sql  string
		want int
	}{
		{"SELECT * FROM t GROUP BY x", 15},
		{"SELECT * FROM t ORDER BY x", 15},
		{"SELECT * FROM t LIMIT 10", 15},
		{"SELECT * FROM t", 15}, // len("SELECT * FROM t") == 15
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, findInsertPoint(tt.sql))
		})
	}
}

func TestCoerceFilterValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   any
		wantTyp string
		wantVal any
	}{
		{"RFC3339", "2026-04-02T16:02:07Z", "string", "2026-04-02 16:02:07"},
		{"RFC3339Nano", "2026-04-02T16:02:07.666Z", "string", "2026-04-02 16:02:07.666"},
		{"RFC3339Nano_short", "2026-04-02T16:02:07.15Z", "string", "2026-04-02 16:02:07.15"},
		{"plain_string", "hello", "string", "hello"},
		{"number", 42, "int", 42},
		{"nil", nil, "<nil>", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := coerceFilterValue(tt.input)
			assert.Equal(t, tt.wantTyp, fmt.Sprintf("%T", got))
			if tt.wantVal != nil {
				assert.Equal(t, tt.wantVal, got)
			}
		})
	}
}

func TestBuild_FilterWithTimestampValue(t *testing.T) {
	t.Parallel()
	schema := &discovery.TableSchema{
		Name: "events",
		Columns: []discovery.Column{
			{Name: "received_timestamp", Type: "DateTime64(3)"},
			{Name: "event_id", Type: "String"},
		},
	}
	sq := &StructuredQuery{
		SelectAll: true,
		Filters: []Filter{
			{Column: "received_timestamp", Op: "lt", Value: "2026-04-02T16:02:07.666Z"},
		},
		OrderBy: []OrderClause{{Column: "received_timestamp", Dir: "desc"}},
		Limit:   3,
	}
	result, err := Build("events", sq, schema, nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "`received_timestamp` < ?")
	require.Len(t, result.Params, 1)
	strVal, isString := result.Params[0].(string)
	assert.True(t, isString, "timestamp filter value should be coerced to formatted string, got %T", result.Params[0])
	assert.Equal(t, "2026-04-02 16:02:07.666", strVal)
}

func TestBuild_TableNameWithBacktick(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}, Limit: 10}
	result, err := Build("my`table", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	// ClickHouse's canonical escaping (per SHOW CREATE) is backslash, not
	// backtick-doubling: an embedded ` becomes \`. The column is quoted too.
	assert.Equal(t, "SELECT `page` FROM `my\\`table` LIMIT 10", result.SQL)
}

func TestBuild_InvalidColumns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sq      *StructuredQuery
		wantErr string
	}{
		{
			name: "invalid aggregation column",
			sq: &StructuredQuery{
				Aggregations: []Aggregation{{Fn: "sum", Column: "nonexistent", Alias: "total"}},
			},
			wantErr: "unknown column",
		},
		{
			name: "bind-unsafe order by column",
			sq: &StructuredQuery{
				Columns: []string{"page"},
				// A non-schema order column is allowed as an alias reference and
				// backtick-quoted; only a '?' (which clickhouse-go's binder would
				// miscount) is rejected.
				OrderBy: []OrderClause{{Column: "we?ird", Dir: "asc"}},
			},
			wantErr: "unsupported order column",
		},
		{
			name: "invalid time range column",
			sq: &StructuredQuery{
				Columns:   []string{"page"},
				TimeRange: &TimeRange{Column: "nonexistent", Since: "2024-01-01T00:00:00Z"},
			},
			wantErr: "unknown column",
		},
		{
			// Valid column, unparseable duration: must fail closed at Build
			// (→ 400) rather than reach ClickHouse as a raw literal (#285).
			name: "invalid time range since duration",
			sq: &StructuredQuery{
				Columns:   []string{"page"},
				TimeRange: &TimeRange{Column: "ts", Since: "banana"},
			},
			wantErr: "invalid time value",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build("clicks", tt.sq, testSchema(), nil, 0, DefaultMaxRows)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestBuild_TimeRange_SinceOnly(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns:   []string{"page"},
		TimeRange: &TimeRange{Column: "ts", Since: "2024-01-01T00:00:00Z", Until: ""},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "`ts` >= ?")
	assert.NotContains(t, result.SQL, "`ts` <= ?")
}

func TestBuild_TimeRange_ClickHouseDateTimeFormat(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		TimeRange: &TimeRange{
			Column: "ts",
			Since:  "2024-01-01T00:00:00Z",
			Until:  "2024-01-02T03:04:05Z",
		},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	require.Len(t, result.Params, 2)
	assert.Equal(t, "2024-01-01 00:00:00", result.Params[0])
	assert.Equal(t, "2024-01-02 03:04:05", result.Params[1])
	for _, p := range result.Params {
		s, ok := p.(string)
		require.True(t, ok, "time_range bound must be a formatted string param")
		assert.NotContains(t, s, "T", "must not bind an RFC3339 T-separated string (#238)")
	}
}

func TestBuild_FilterUnsupportedOp(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		// Unsupported operations should gracefully be ignored by filterToSQL
		Filters: []Filter{{Column: "page", Op: "magic", Value: "val"}},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestBuild_FilterInOp_InvalidValueType(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		// 'in' operator requires an array ([]any) value. A scalar string shouldn't panic.
		Filters: []Filter{{Column: "page", Op: "in", Value: "not-an-array"}},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// ─── Column-allowlist authorization (#223 and its sibling fail-opens) ──────────

// TestBuild_AuthorizesEveryClause is the core regression for the vulnerability
// family: a denied column must be rejected no matter WHICH clause it appears in.
// The original bug only checked the SELECT projection and aggregations, leaving
// filters, group_by, order_by, and time_range able to reference (and thereby
// leak — e.g. GROUP BY a denied column enumerates its distinct values) a column
// the role cannot read. Build now authorizes every column reference.
func TestBuild_AuthorizesEveryClause(t *testing.T) {
	t.Parallel()
	// viewer may read page/ts/count; org_id (a tenant key) is denied.
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"page", "ts", "count"}}
	const denied = "org_id"

	tests := []struct {
		name string
		sq   *StructuredQuery
	}{
		{"projection column", &StructuredQuery{Columns: []string{denied}}},
		{"aggregation argument", &StructuredQuery{Aggregations: []Aggregation{{Fn: "max", Column: denied, Alias: "m"}}}},
		{"filter column", &StructuredQuery{Columns: []string{"page"}, Filters: []Filter{{Column: denied, Op: "eq", Value: "x"}}}},
		{"group_by column", &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n"}}, GroupBy: []string{denied}}},
		{"order_by column", &StructuredQuery{Columns: []string{"page"}, OrderBy: []OrderClause{{Column: denied, Dir: "asc"}}}},
		{"time_range column", &StructuredQuery{Columns: []string{"page"}, TimeRange: &TimeRange{Column: denied, Since: "1h"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build("clicks", tt.sq, testSchema(), perms, 0, DefaultMaxRows)
			var fce *ForbiddenColumnError
			require.ErrorAs(t, err, &fce, "denied column in %s must be rejected", tt.name)
			assert.Equal(t, denied, fce.Column)
		})
	}
}

// TestBuild_AllowsAuthorizedColumnsInEveryClause is the positive counterpart:
// allowed columns in every clause build successfully.
func TestBuild_AllowsAuthorizedColumnsInEveryClause(t *testing.T) {
	t.Parallel()
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"page", "ts", "count"}}
	sq := &StructuredQuery{
		Columns:   []string{"page"},
		Filters:   []Filter{{Column: "count", Op: "gt", Value: 1}},
		GroupBy:   []string{"page"},
		OrderBy:   []OrderClause{{Column: "ts", Dir: "asc"}},
		TimeRange: &TimeRange{Column: "ts", Since: "1h"},
	}
	_, err := Build("clicks", sq, testSchema(), perms, 0, DefaultMaxRows)
	require.NoError(t, err)
}

// TestBuild_SelectAllProjection covers SelectAll resolution: an unrestricted role
// (or no policy) keeps a bare "*", a column-restricted role expands to exactly its
// allowed columns in schema order (deny always subtracted), and a role entitled to
// no columns fails closed — so SelectAll can never reach ClickHouse as a bare *
// that returns denied columns (#223).
func TestBuild_SelectAllProjection(t *testing.T) {
	t.Parallel()
	// testSchema order: page, button, count, ts, org_id.
	tests := []struct {
		name     string
		perms    *policy.ResolvedPermissions
		wantErr  error
		selectIs string
	}{
		{
			name:     "nil perms keeps SELECT *",
			perms:    nil,
			selectIs: "*",
		},
		{
			name:     "unrestricted (wildcard allow) keeps SELECT *",
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}},
			selectIs: "*",
		},
		{
			name:     "restricted expands to allowed projection (schema order)",
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"count", "page"}},
			selectIs: "`page`, `count`",
		},
		{
			name:     "deny-list with empty allow expands and drops denied",
			perms:    &policy.ResolvedPermissions{Allowed: true, DenyColumns: []string{"org_id"}},
			selectIs: "`page`, `button`, `count`, `ts`",
		},
		{
			name:     "deny-list with wildcard allow drops denied",
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}, DenyColumns: []string{"org_id", "button"}},
			selectIs: "`page`, `count`, `ts`",
		},
		{
			name:    "restricted with zero readable columns fails closed",
			perms:   &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"nonexistent"}},
			wantErr: ErrNoReadableColumns,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := Build("clicks", &StructuredQuery{SelectAll: true}, testSchema(), tt.perms, 0, DefaultMaxRows)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "SELECT "+tt.selectIs+" FROM `clicks` LIMIT 10000", result.SQL,
				"restricted SelectAll must never reach ClickHouse as a bare *")
			assert.NotContains(t, result.SQL, "org_id", "denied column must not appear when denied")
		})
	}
}

// TestBuild_ForbiddenAggregation maps a denied aggregation function to a typed
// error (→ HTTP 403), distinct from an unsupported function (→ 400).
func TestBuild_ForbiddenAggregation(t *testing.T) {
	t.Parallel()
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"count"}, DeniedAggregations: []string{"sum"}}
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "sum", Column: "count", Alias: "total"}}}
	_, err := Build("clicks", sq, testSchema(), perms, 0, DefaultMaxRows)
	var fae *ForbiddenAggregationError
	require.ErrorAs(t, err, &fae)
	assert.Equal(t, "sum", fae.Fn)
}

// TestBuild_OrderByAliasSkipsColumnPolicy confirms ORDER BY an aggregation alias
// (not a schema column) is not column-policy-checked — the aggregation that
// defines the alias was already authorized — so a legitimate "order by the count"
// query is not wrongly rejected for a column-restricted role.
func TestBuild_OrderByAliasSkipsColumnPolicy(t *testing.T) {
	t.Parallel()
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"page"}}
	sq := &StructuredQuery{
		Columns:      []string{"page"},
		Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n"}},
		GroupBy:      []string{"page"},
		OrderBy:      []OrderClause{{Column: "n", Dir: "desc"}},
	}
	result, err := Build("clicks", sq, testSchema(), perms, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ORDER BY `n` DESC")
}

// TestBuild_CountStarWithoutReadableColumns documents that count(*) is permitted
// even when the role can read no concrete columns: it exposes cardinality, not
// column values, and is governed by aggregation policy + row-level filters.
func TestBuild_CountStarWithoutReadableColumns(t *testing.T) {
	t.Parallel()
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"nonexistent"}}
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n"}}}
	result, err := Build("clicks", sq, testSchema(), perms, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "count(*) AS `n`")
}

// TestBuild_AggregationAliasQuotedAndContained is the successor to the old
// strict-alias rejection test. Aliases are now permissive identifiers (ClickHouse
// allows arbitrary quoted alias names), so an injection-shaped alias is no longer
// rejected — it is backtick-quoted, which neutralizes it: the crafted SQL becomes
// an inert (weird) column label rather than syntax. Real containment against a
// live server is proven by TestIntegration_AliasInjectionContained; here we pin
// that Build succeeds and emits the alias as a single quoted identifier whose
// inner backticks are backslash-escaped, so it cannot break out.
func TestBuild_AggregationAliasQuotedAndContained(t *testing.T) {
	t.Parallel()
	aliases := []string{
		"total",
		"n FROM secrets --", // reparent attempt
		"n; DROP TABLE clicks",
		"n, (SELECT 1)",     // subquery
		"n` FROM secrets `", // backtick break
		"1n",                // leading digit
		"naïve total",       // space + unicode
	}
	for _, alias := range aliases {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: alias}}}
			result, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
			require.NoError(t, err)
			assert.Contains(t, result.SQL, "AS "+chsql.QuoteIdent(alias),
				"alias must be emitted as one backtick-quoted, escaped token")
		})
	}
}

// TestBuild_RejectsBindUnsafeAlias keeps the one alias rejection that remains: a
// '?' would be miscounted by clickhouse-go's positional value binder.
func TestBuild_RejectsBindUnsafeAlias(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "we?ird"}}}
	_, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported aggregation alias")
}

// ─── Permissive identifier handling (ClickHouse allows arbitrary quoted names) ─
//
// ClickHouse identifiers — table names, column names, AND aliases — may contain
// arbitrary characters when quoted (dots, spaces, unicode, keywords, embedded
// backticks/backslashes). Customers point WaveHouse at existing schemas that use
// such names, so the builder must accept any name the schema actually contains —
// not just names matching the safe-identifier regex.
//
// These unit tests assert ACCEPTANCE only (the builder no longer rejects a legal
// column/alias). That the resulting SQL is correctly ESCAPED and round-trips —
// including the backslash/backtick cases and injection containment — is proven
// against a real ClickHouse in tests/integration/identifier_roundtrip_test.go.
// The escaping mechanism (server-side {name:Identifier} params vs a centralized
// client-side quoter) is deliberately not asserted here so these stay valid
// whichever is chosen.

// weirdColumnSchema returns a schema whose weird column names are all legal
// ClickHouse identifiers but would be rejected by a strict identifier regex.
// "id" is a normal anchor so each clause can isolate one weird column.
func weirdColumnSchema() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "weird",
		Columns: []discovery.Column{
			{Name: "id", Type: "String"},
			{Name: "user.id", Type: "String"},
			{Name: "2024", Type: "String"},
			{Name: "gross $", Type: "String"},
			{Name: "naïve", Type: "String"},
			{Name: "tick`col", Type: "String"},
			{Name: `back\slash`, Type: "String"},
		},
	}
}

// TestBuild_PermissiveColumnNames_AcceptedInEveryClause asserts a legal-in-
// ClickHouse column that the safe-identifier regex rejects is still accepted no
// matter which clause references it — projection, filter, group_by, order_by,
// aggregation argument, or time_range. order_by is included deliberately: it has
// its own identifier check distinct from validateColumn and must be relaxed too.
func TestBuild_PermissiveColumnNames_AcceptedInEveryClause(t *testing.T) {
	t.Parallel()
	schema := weirdColumnSchema()
	weird := []string{"user.id", "2024", "gross $", "naïve", "tick`col", `back\slash`}
	for _, col := range weird {
		t.Run(col, func(t *testing.T) {
			t.Parallel()
			clauses := map[string]*StructuredQuery{
				"projection":  {Columns: []string{col}},
				"filter":      {Columns: []string{"id"}, Filters: []Filter{{Column: col, Op: "eq", Value: "x"}}},
				"group_by":    {Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n"}}, GroupBy: []string{col}},
				"order_by":    {Columns: []string{"id"}, OrderBy: []OrderClause{{Column: col, Dir: "asc"}}},
				"aggregation": {Aggregations: []Aggregation{{Fn: "max", Column: col, Alias: "m"}}},
				"time_range":  {Columns: []string{"id"}, TimeRange: &TimeRange{Column: col, Since: "1h"}},
			}
			for clause, sq := range clauses {
				_, err := Build("weird", sq, schema, nil, 0, DefaultMaxRows)
				require.NoErrorf(t, err, "%s clause must accept legal ClickHouse column %q", clause, col)
			}
		})
	}
}

// TestBuild_PermissiveAliases_Accepted asserts aliases — which ClickHouse defines
// as identifiers ("Aliases should comply with the identifiers syntax") — are
// accepted permissively, including injection-shaped ones. Containment (that a
// crafted alias cannot break out of the AS position) is an escaping property
// proven by TestIntegration_AliasInjectionContained, not a rejection.
func TestBuild_PermissiveAliases_Accepted(t *testing.T) {
	t.Parallel()
	aliases := []string{
		"total events",       // space
		"naïve",              // unicode
		"n) FROM secrets --", // injection-shaped — must be contained, not rejected
		"x` , (SELECT 1) `y", // backtick break — contained
	}
	for _, alias := range aliases {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: alias}}}
			_, err := Build("clicks", sq, testSchema(), nil, 0, DefaultMaxRows)
			require.NoErrorf(t, err, "alias %q must be accepted — containment is an escaping concern, not a rejection", alias)
		})
	}
}
