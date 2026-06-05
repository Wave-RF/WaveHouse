package query

import (
	"fmt"
	"testing"
	"time"

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
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Equal(t, "SELECT page, count FROM `clicks` LIMIT 10", result.SQL)
	assert.Empty(t, result.Params)
}

func TestBuild_SelectStar(t *testing.T) {
	t.Parallel()
	result, err := Build("clicks", &StructuredQuery{}, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `clicks` LIMIT 10000", result.SQL)
}

func TestBuild_WithAggregation(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "total"}},
		GroupBy:      []string{"page"},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "count(*) AS total")
	assert.Contains(t, result.SQL, "GROUP BY page")
}

func TestBuild_AllFilterOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		op   string
		want string
	}{
		{"eq", "page = ?"},
		{"neq", "page != ?"},
		{"gt", "page > ?"},
		{"gte", "page >= ?"},
		{"lt", "page < ?"},
		{"lte", "page <= ?"},
		{"like", "page LIKE ?"},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{
				Columns: []string{"page"},
				Filters: []Filter{{Column: "page", Op: tt.op, Value: "test"}},
			}
			result, err := Build("clicks", sq, testSchema(), nil, 0)
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
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "page IN (?,?)")
	assert.Len(t, result.Params, 2)
}

func TestBuild_OrderBy(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page", "count"},
		OrderBy: []OrderClause{{Column: "count", Dir: "desc"}, {Column: "page", Dir: "asc"}},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ORDER BY count DESC, page ASC")
}

func TestBuild_UnknownColumn(t *testing.T) {
	t.Parallel()
	_, err := Build("clicks", &StructuredQuery{Columns: []string{"nonexistent"}}, testSchema(), nil, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestBuild_InvalidAggFn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "drop_table", Column: "count"}}}
	_, err := Build("clicks", sq, testSchema(), nil, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported aggregation")
}

func TestBuild_TimeRange(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns:   []string{"page"},
		TimeRange: &TimeRange{Column: "ts", Since: "2024-01-01T00:00:00Z"},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ts >= ?")
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
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, fmt.Sprintf("LIMIT %d", DefaultMaxRows))
}

func TestBuild_LimitExceedsDefaultMaxRows_Capped(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}, Limit: DefaultMaxRows + 1}
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, fmt.Sprintf("LIMIT %d", DefaultMaxRows))
	assert.NotContains(t, result.SQL, fmt.Sprintf("LIMIT %d", DefaultMaxRows+1))
}

func TestBuild_LimitWithinRange_Respected(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}, Limit: 50}
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "LIMIT 50")
}

func TestBuild_InvalidFilterColumn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		Filters: []Filter{{Column: "nonexistent", Op: "eq", Value: "x"}},
	}
	_, err := Build("clicks", sq, testSchema(), nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestBuild_InvalidGroupByColumn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		GroupBy: []string{"nonexistent"},
	}
	_, err := Build("clicks", sq, testSchema(), nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestResolveTimeValue_RelativeDuration(t *testing.T) {
	t.Parallel()
	result := resolveTimeValue("1h", 0)
	assert.NotEmpty(t, result)
	assert.NotEqual(t, "1h", result, "relative duration should resolve to a timestamp")
}

func TestResolveTimeValue_RFC3339(t *testing.T) {
	t.Parallel()
	result := resolveTimeValue("2024-01-01T00:00:00Z", 0)
	assert.Equal(t, "2024-01-01T00:00:00Z", result)
}

func TestResolveTimeValue_WithBucketing(t *testing.T) {
	t.Parallel()
	// With 60s buckets, a time at :30 should truncate to :00.
	result := resolveTimeValue("2024-01-01T12:34:30Z", 60)
	assert.Equal(t, "2024-01-01T12:34:00Z", result)
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
		Filters: []Filter{
			{Column: "received_timestamp", Op: "lt", Value: "2026-04-02T16:02:07.666Z"},
		},
		OrderBy: []OrderClause{{Column: "received_timestamp", Dir: "desc"}},
		Limit:   3,
	}
	result, err := Build("events", sq, schema, nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "received_timestamp < ?")
	require.Len(t, result.Params, 1)
	strVal, isString := result.Params[0].(string)
	assert.True(t, isString, "timestamp filter value should be coerced to formatted string, got %T", result.Params[0])
	assert.Equal(t, "2026-04-02 16:02:07.666", strVal)
}

func TestBuild_TableNameWithBacktick(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Columns: []string{"page"}, Limit: 10}
	result, err := Build("my`table", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Equal(t, "SELECT page FROM `my``table` LIMIT 10", result.SQL)
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
			name: "invalid order by column",
			sq: &StructuredQuery{
				Columns: []string{"page"},
				// Aliases are allowed, but they must still be valid identifiers
				OrderBy: []OrderClause{{Column: "invalid;--", Dir: "asc"}},
			},
			wantErr: "invalid order column",
		},
		{
			name: "invalid time range column",
			sq: &StructuredQuery{
				Columns:   []string{"page"},
				TimeRange: &TimeRange{Column: "nonexistent", Since: "2024-01-01T00:00:00Z"},
			},
			wantErr: "unknown column",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build("clicks", tt.sq, testSchema(), nil, 0)
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
	result, err := Build("clicks", sq, testSchema(), nil, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ts >= ?")
	assert.NotContains(t, result.SQL, "ts <= ?")
}

func TestBuild_FilterUnsupportedOp(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns: []string{"page"},
		// Unsupported operations should gracefully be ignored by filterToSQL
		Filters: []Filter{{Column: "page", Op: "magic", Value: "val"}},
	}
	result, err := Build("clicks", sq, testSchema(), nil, 0)
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
	result, err := Build("clicks", sq, testSchema(), nil, 0)
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
			_, err := Build("clicks", tt.sq, testSchema(), perms, 0)
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
	_, err := Build("clicks", sq, testSchema(), perms, 0)
	require.NoError(t, err)
}

// TestBuild_WildcardProjection covers SELECT * resolution: unrestricted roles
// keep "*", restricted roles expand to exactly their allowed columns (in schema
// order), and the omitted-columns and explicit-"*" inputs behave identically —
// closing both the #223 omitted-columns bypass and its explicit-"*" sibling.
func TestBuild_WildcardProjection(t *testing.T) {
	t.Parallel()
	// testSchema order: page, button, count, ts, org_id.
	tests := []struct {
		name     string
		columns  []string
		perms    *policy.ResolvedPermissions
		wantErr  error
		selectIs string
	}{
		{
			name:     "nil perms keeps SELECT *",
			columns:  nil,
			perms:    nil,
			selectIs: "*",
		},
		{
			name:     "unrestricted (wildcard allow) omitted columns keeps SELECT *",
			columns:  nil,
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}},
			selectIs: "*",
		},
		{
			name:     "restricted omitted columns expands to allowed projection (schema order)",
			columns:  nil,
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"count", "page"}},
			selectIs: "page, count",
		},
		{
			name:     "restricted explicit-star expands identically (sibling bypass)",
			columns:  []string{"*"},
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"count", "page"}},
			selectIs: "page, count",
		},
		{
			name:     "deny-list with empty allow expands and drops denied (sibling bypass)",
			columns:  []string{"*"},
			perms:    &policy.ResolvedPermissions{Allowed: true, DenyColumns: []string{"org_id"}},
			selectIs: "page, button, count, ts",
		},
		{
			name:     "deny-list with wildcard allow drops denied",
			columns:  nil,
			perms:    &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"*"}, DenyColumns: []string{"org_id", "button"}},
			selectIs: "page, count, ts",
		},
		{
			name:    "restricted with zero readable columns fails closed",
			columns: nil,
			perms:   &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"nonexistent"}},
			wantErr: ErrNoReadableColumns,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := Build("clicks", &StructuredQuery{Columns: tt.columns}, testSchema(), tt.perms, 0)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "SELECT "+tt.selectIs+" FROM `clicks` LIMIT 10000", result.SQL,
				"restricted SELECT * must never reach ClickHouse as a bare *")
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
	_, err := Build("clicks", sq, testSchema(), perms, 0)
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
	result, err := Build("clicks", sq, testSchema(), perms, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ORDER BY n DESC")
}

// TestBuild_CountStarWithoutReadableColumns documents that count(*) is permitted
// even when the role can read no concrete columns: it exposes cardinality, not
// column values, and is governed by aggregation policy + row-level filters.
func TestBuild_CountStarWithoutReadableColumns(t *testing.T) {
	t.Parallel()
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"nonexistent"}}
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n"}}}
	result, err := Build("clicks", sq, testSchema(), perms, 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "count(*) AS n")
}

// TestBuild_RejectsInjectionViaAggregationAlias is a red-team regression for a
// SQL-injection vector adjacent to #223: the aggregation alias is interpolated
// into the SELECT list verbatim ("fn(col) AS <alias>"), so an alias that isn't a
// bare identifier — one that reparents the query with a comment, breaks the
// statement, or opens a subquery — must be rejected before it reaches ClickHouse.
func TestBuild_RejectsInjectionViaAggregationAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		alias string
		valid bool
	}{
		{"plain identifier", "total", true},
		{"identifier with underscore and digits", "total_2", true},
		{"empty alias is allowed (no AS emitted)", "", true},
		{"reparent via comment", "n FROM secrets --", false},
		{"statement break", "n; DROP TABLE clicks", false},
		{"subquery", "n, (SELECT 1)", false},
		{"backtick break", "n` FROM secrets `", false},
		{"leading digit", "1n", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: tt.alias}}}
			result, err := Build("clicks", sq, testSchema(), nil, 0)
			if tt.valid {
				require.NoError(t, err)
				if tt.alias != "" {
					assert.Contains(t, result.SQL, "AS "+tt.alias)
				}
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid aggregation alias")
				assert.Nil(t, result)
			}
		})
	}
}
