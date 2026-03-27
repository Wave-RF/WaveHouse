package query

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
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
	result, err := Build("clicks", sq, testSchema(), 0)
	require.NoError(t, err)
	assert.Equal(t, "SELECT page, count FROM clicks LIMIT 10", result.SQL)
	assert.Empty(t, result.Params)
}

func TestBuild_SelectStar(t *testing.T) {
	t.Parallel()
	result, err := Build("clicks", &StructuredQuery{}, testSchema(), 0)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks", result.SQL)
}

func TestBuild_WithAggregation(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "total"}},
		GroupBy:      []string{"page"},
	}
	result, err := Build("clicks", sq, testSchema(), 0)
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
			result, err := Build("clicks", sq, testSchema(), 0)
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
	result, err := Build("clicks", sq, testSchema(), 0)
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
	result, err := Build("clicks", sq, testSchema(), 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ORDER BY count DESC, page ASC")
}

func TestBuild_UnknownColumn(t *testing.T) {
	t.Parallel()
	_, err := Build("clicks", &StructuredQuery{Columns: []string{"nonexistent"}}, testSchema(), 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestBuild_InvalidAggFn(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{Aggregations: []Aggregation{{Fn: "drop_table", Column: "count"}}}
	_, err := Build("clicks", sq, testSchema(), 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported aggregation")
}

func TestBuild_TimeRange(t *testing.T) {
	t.Parallel()
	sq := &StructuredQuery{
		Columns:   []string{"page"},
		TimeRange: &TimeRange{Column: "ts", Since: "2024-01-01T00:00:00Z"},
	}
	result, err := Build("clicks", sq, testSchema(), 0)
	require.NoError(t, err)
	assert.Contains(t, result.SQL, "ts >= ?")
	assert.Len(t, result.Params, 1)
}

func TestInjectPermissionFilters_WithWhere(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks WHERE page = ?", Params: []any{"/home"}}
	InjectPermissionFilters(result, "org_id = ?", []any{"org-1"})
	assert.Contains(t, result.SQL, "(org_id = ?)")
	assert.Equal(t, []any{"org-1", "/home"}, result.Params)
}

func TestInjectPermissionFilters_WithoutWhere(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks ORDER BY page"}
	InjectPermissionFilters(result, "org_id = ?", []any{"org-1"})
	assert.Contains(t, result.SQL, "WHERE org_id = ?")
	assert.Contains(t, result.SQL, "ORDER BY page")
}

func TestInjectPermissionFilters_Empty(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks"}
	InjectPermissionFilters(result, "", nil)
	assert.Equal(t, "SELECT * FROM clicks", result.SQL)
}

func TestApplyMaxRows_NoLimit(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks"}
	ApplyMaxRows(result, 100)
	assert.Contains(t, result.SQL, "LIMIT 100")
}

func TestApplyMaxRows_HigherExisting(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks LIMIT 500"}
	ApplyMaxRows(result, 100)
	assert.Contains(t, result.SQL, "LIMIT 100")
}

func TestApplyMaxRows_LowerExisting(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks LIMIT 50"}
	ApplyMaxRows(result, 100)
	assert.Contains(t, result.SQL, "LIMIT 50")
}

func TestApplyMaxRows_Zero(t *testing.T) {
	t.Parallel()
	result := &BuildResult{SQL: "SELECT * FROM clicks"}
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
