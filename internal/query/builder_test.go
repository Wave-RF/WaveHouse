package query

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
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
	// One placeholder for the whole list, bound as one external table.
	assert.Contains(t, result.SQL, "`page` IN ?")
	require.Len(t, result.Params, 1)
	assert.Equal(t, listParam{Values: []any{"/home", "/about"}}, result.Params[0])

	b, err := result.Bind()
	require.NoError(t, err)
	assert.Contains(t, b.SQL, "`page` IN (SELECT v FROM _p0)")
	assert.Empty(t, b.Params)
	assert.Equal(t, []Table{{Name: "_p0", Data: rowBinary("/home", "/about")}}, b.Tables)
}

// rowBinary is the RowBinary a Table carries for vals.
func rowBinary(vals ...string) []byte {
	var b []byte
	for _, v := range vals {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return b
}

// bindTexts binds res and returns its SQL and parameter values, failing on a
// table: a test of scalar binding.
func bindTexts(t *testing.T, res *BuildResult) (string, []string) {
	t.Helper()
	b, err := res.Bind()
	require.NoError(t, err)
	require.Empty(t, b.Tables)
	return b.SQL, paramValues(b)
}

// paramValues are b's parameter values in placeholder order, nil for none.
func paramValues(b *Bound) []string {
	var out []string
	for _, p := range b.Params {
		out = append(out, p.Value)
	}
	return out
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

// permsWithFilter returns resolved permissions carrying a row-filter predicate
// on org_id (a String column), resolved by policy.Evaluate itself.
func permsWithFilter() *policy.ResolvedPermissions {
	return permsFiltering(map[string]policy.Filter{"org_id": {Eq: new("org-1")}}, nil)
}

// permsFiltering resolves a read grant carrying filter for role "r" on clicks.
func permsFiltering(filter map[string]policy.Filter, claims map[string]any) *policy.ResolvedPermissions {
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"r": {Select: &policy.SelectPermissions{Filter: filter}}},
	}}
	return policy.Evaluate(p, "r", "clicks", "select", claims)
}

// TestBuild_PolicyPredicate pins the structural emission of the row-level-
// security predicate (#322): Build places it first — in both the WHERE clause
// and the positional params — ANDed ahead of the caller's own filters, and
// before any ORDER BY / LIMIT, so a caller can narrow but never widen row
// visibility.
func TestBuild_PolicyPredicate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sq         *StructuredQuery
		wantSQL    string
		wantParams []any
	}{
		{
			"ANDed ahead of caller filters",
			&StructuredQuery{Columns: []string{"page"}, Filters: []Filter{{Column: "page", Op: "eq", Value: "/home"}}},
			"SELECT `page` FROM `clicks` WHERE (`org_id` = ?) AND `page` = ? LIMIT 10000",
			[]any{"org-1", "/home"},
		},
		{
			"emitted before ORDER BY and LIMIT when the caller has no filters",
			&StructuredQuery{Columns: []string{"page"}, OrderBy: []OrderClause{{Column: "page", Dir: "asc"}}},
			"SELECT `page` FROM `clicks` WHERE (`org_id` = ?) ORDER BY `page` ASC LIMIT 10000",
			[]any{"org-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := Build("clicks", tt.sq, testSchema(), permsWithFilter(), 0, DefaultMaxRows)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, result.SQL)
			assert.Equal(t, tt.wantParams, result.Params)
		})
	}
}

// TestBuild_PolicyPredicate_SurvivesCraftedIdentifiers pins the row filter
// against the identifier family that deleted it when the predicate was spliced
// into rendered SQL (#322, found on #457): an alias or ORDER BY alias-reference
// carrying a SQL clause keyword — including "lımıt", whose dotless ı (U+0131)
// uppercases to ASCII I and slipped past the interim regex guard's (?i) simple
// case folding — is accepted, stays contained in its backtick-quoted
// identifier, and the predicate survives.
func TestBuild_PolicyPredicate_SurvivesCraftedIdentifiers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sq   *StructuredQuery
	}{
		{"WHERE in an alias", &StructuredQuery{
			Aggregations: []Aggregation{{Fn: "groupArray", Column: "page", Alias: "e WHERE z"}},
		}},
		{"dotless-i lımıt in an alias", &StructuredQuery{
			Aggregations: []Aggregation{{Fn: "groupArray", Column: "page", Alias: "e lımıt z"}},
		}},
		{"legitimate keyword-bearing alias", &StructuredQuery{
			Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "Total order by region"}},
		}},
		{"keyword in an ORDER BY alias-reference", &StructuredQuery{
			Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n WHERE 1 = 1"}},
			OrderBy:      []OrderClause{{Column: "n WHERE 1 = 1", Dir: "desc"}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := Build("clicks", tt.sq, testSchema(), permsWithFilter(), 0, DefaultMaxRows)
			require.NoError(t, err)
			assert.Contains(t, result.SQL, " WHERE (`org_id` = ?)", "row filter must survive the crafted identifier")
			assert.Equal(t, []any{"org-1"}, result.Params)
		})
	}
}

// TestBuild_PolicyPredicate_IntegerColumnsBindThroughTheStrictCast pins the
// query path's half of the integer-claim rule: a policy claim compared against
// an integer column (any width, Nullable or LowCardinality) renders as
// chsql.StrictInt over ONE {pN:String} parameter, on every operator and on
// each element of an _in list, while every other column keeps the plain
// {pN:String} form. The typelayer renders the same expression for the stream
// and the insert check.
func TestBuild_PolicyPredicate_IntegerColumnsBindThroughTheStrictCast(t *testing.T) {
	t.Parallel()
	schema := &discovery.TableSchema{Name: "clicks", Columns: []discovery.Column{
		{Name: "page", Type: "String"},
		{Name: "u64", Type: "UInt64"},
		{Name: "i8", Type: "Int8"},
		{Name: "nu256", Type: "Nullable(UInt256)"},
		{Name: "lci32", Type: "LowCardinality(Nullable(Int32))"},
		{Name: "org_id", Type: "String"},
		{Name: "amount", Type: "Decimal(18, 4)"},
		{Name: "flag", Type: "Bool"},
	}}
	e := func(p, typ string) string {
		c := "accurateCastOrNull({" + p + ":String}, '" + typ + "')"
		return "if(toString(" + c + ") = {" + p + ":String}, " + c + ", NULL)"
	}
	tests := []struct {
		name       string
		column     string
		filter     policy.Filter
		claims     map[string]any
		wantWhere  string
		wantParams []string
	}{
		{
			"eq on UInt64", "u64",
			policy.Filter{Eq: new("{{ jwt.t }}")},
			map[string]any{"t": "5"},
			"`u64` = " + e("p0", "UInt64"),
			[]string{"5"},
		},
		{
			"neq on Int8", "i8",
			policy.Filter{Neq: new("-3")},
			nil,
			"`i8` != " + e("p0", "Int8"),
			[]string{"-3"},
		},
		{
			"gt on Nullable(UInt256) casts to the bare type", "nu256",
			policy.Filter{Gt: new("7")},
			nil,
			"`nu256` > " + e("p0", "UInt256"),
			[]string{"7"},
		},
		{
			"lt on LowCardinality(Nullable(Int32)) casts to the bare type", "lci32",
			policy.Filter{Lt: new("9")},
			nil,
			"`lci32` < " + e("p0", "Int32"),
			[]string{"9"},
		},
		{
			"in on UInt64 casts each element", "u64",
			policy.Filter{In: new("{{ jwt.ts }}")},
			map[string]any{"ts": []any{"5", "18446744073709551621", "007"}},
			"`u64` IN (" + e("p0", "UInt64") + "," + e("p1", "UInt64") + "," + e("p2", "UInt64") + ")",
			[]string{"5", "18446744073709551621", "007"},
		},
		{
			"a claim needing escape is encoded once", "u64",
			policy.Filter{Eq: new("{{ jwt.t }}")},
			map[string]any{"t": `a\b`},
			"`u64` = " + e("p0", "UInt64"),
			[]string{`a\\b`},
		},
		{
			"eq on String keeps the plain form", "org_id",
			policy.Filter{Eq: new("acme")},
			nil,
			"`org_id` = {p0:String}",
			[]string{"acme"},
		},
		{
			"in on String keeps the plain form", "org_id",
			policy.Filter{In: new("{{ jwt.ts }}")},
			map[string]any{"ts": []any{"a", "b"}},
			"`org_id` IN ({p0:String},{p1:String})",
			[]string{"a", "b"},
		},
		{
			"Decimal keeps the plain form", "amount",
			policy.Filter{Gt: new("1.50")},
			nil,
			"`amount` > {p0:String}",
			[]string{"1.50"},
		},
		{
			"Bool keeps the plain form", "flag",
			policy.Filter{Eq: new("true")},
			nil,
			"`flag` = {p0:String}",
			[]string{"true"},
		},
		{
			"an unresolvable claim still fails closed", "u64",
			policy.Filter{Eq: new("{{ jwt.absent }}")},
			nil,
			"1 = 0", nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			perms := permsFiltering(map[string]policy.Filter{tt.column: tt.filter}, tt.claims)
			res, err := Build("clicks", &StructuredQuery{Columns: []string{"page"}}, schema, perms, 0, DefaultMaxRows)
			require.NoError(t, err)
			sql, params := bindTexts(t, res)
			assert.Equal(t, "SELECT `page` FROM `clicks` WHERE ("+tt.wantWhere+") LIMIT 10000", sql)
			assert.Equal(t, tt.wantParams, params)
		})
	}
}

// TestBuild_PolicyPredicate_CallerFiltersKeepThePlainForm: the strict cast is
// for policy claims only. A caller's own filter on an integer column binds as
// a plain {pN:String}, and its `in` list as a table converted with
// accurateCastOrNull — it can only narrow what the policy already admits.
func TestBuild_PolicyPredicate_CallerFiltersKeepThePlainForm(t *testing.T) {
	t.Parallel()
	perms := permsFiltering(map[string]policy.Filter{"count": {Eq: new("5")}}, nil)
	sq := &StructuredQuery{Columns: []string{"page"}, Filters: []Filter{
		{Column: "count", Op: "gt", Value: json.Number("1")},
		{Column: "count", Op: "in", Value: []any{json.Number("1"), json.Number("2")}},
	}}
	res, err := Build("clicks", sq, testSchema(), perms, 0, DefaultMaxRows)
	require.NoError(t, err)
	b, err := res.Bind()
	require.NoError(t, err)
	assert.Equal(t, "SELECT `page` FROM `clicks` WHERE (`count` = "+chsql.StrictInt("p0", "UInt64")+
		") AND `count` > {p1:String} AND `count` IN (SELECT accurateCastOrNull(v, 'UInt64') FROM _p2) LIMIT 10000", b.SQL)
	assert.Equal(t, []string{"5", "1"}, paramValues(b))
	assert.Equal(t, []Table{{Name: "_p2", Data: rowBinary("1", "2")}}, b.Tables)
}

// TestBuild_PolicyMaxRows pins the role's max_rows cap folded into Build's LIMIT
// computation (#322): the emitted LIMIT is min(caller limit, default cap, policy
// cap), with non-positive values meaning "no cap from that source". Includes a
// schema whose column uppercases to a different byte length ("ıı"), which made
// the deleted post-hoc ApplyMaxRows mis-index and silently drop the cap.
func TestBuild_PolicyMaxRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		column  string
		maxRows int
		limit   int
		want    int
	}{
		{"policy cap below default", "page", 100, 0, 100},
		{"caller limit below policy cap", "page", 100, 7, 7},
		{"caller limit above policy cap", "page", 100, 500, 100},
		{"policy cap above default is inert", "page", DefaultMaxRows + 500, 0, DefaultMaxRows},
		{"no policy cap", "page", 0, 0, DefaultMaxRows},
		{"cap survives a length-changing unicode column", "ıı", 100, 0, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			schema := &discovery.TableSchema{Name: "clicks", Columns: []discovery.Column{{Name: tt.column, Type: "String"}}}
			perms := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{MaxRows: tt.maxRows}}
			sq := &StructuredQuery{Columns: []string{tt.column}, Limit: tt.limit}
			result, err := Build("clicks", sq, schema, perms, 0, DefaultMaxRows)
			require.NoError(t, err)
			assert.True(t, strings.HasSuffix(result.SQL, fmt.Sprintf(" LIMIT %d", tt.want)),
				"want LIMIT %d, got %q", tt.want, result.SQL)
		})
	}
}

func TestIsValidAggFn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		fn   string
		want bool
	}{
		{"count", "count", true},
		{"sum", "sum", true},
		{"avg", "avg", true},
		{"min", "min", true},
		{"max", "max", true},
		{"uniq", "uniq", true},
		{"median", "median", true},
		{"uppercase COUNT", "COUNT", true},
		{"mixed-case Min", "Min", true},
		{"unknown function", "drop_table", false},
		{"empty name", "", false},
		// Unicode case folding must not stand in for the ASCII-exact match:
		// strings.ToLower maps İ (U+0130) to 'i', so "mİn" would otherwise
		// pass the allowlist and be emitted verbatim — ClickHouse then answers
		// "unknown function" (a 500) where this rejection's 400 belongs.
		{"unicode lookalike of min", "mİn", false},
		{"unicode lookalike of uniq", "unİq", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isValidAggFn(tt.fn))
		})
	}
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
// the caller passes (the query.default_max_rows knob), both as the
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
	// RFC 3339 in UTC: the Z is what makes the column's zone irrelevant.
	_, err = time.Parse(time.RFC3339Nano, result)
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(result, "Z"), result)
}

func TestResolveTimeValue_RFC3339(t *testing.T) {
	t.Parallel()

	result, err := resolveTimeValue("2024-01-01T00:00:00Z", 0)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01T00:00:00Z", result)

	// An offset is normalised to the same instant in UTC; a fraction is kept.
	result, err = resolveTimeValue("2024-01-01T09:00:00.25+09:00", 0)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01T00:00:00.25Z", result)
}

func TestResolveTimeValue_WithBucketing(t *testing.T) {
	t.Parallel()
	// With 60s buckets, a time at :30 should truncate to :00.
	result, err := resolveTimeValue("2024-01-01T12:34:30Z", 60)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01T12:34:00Z", result)
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
	// to an instant, not fall through as the raw string (#285).
	for _, in := range []string{"7d", "2w", "1d12h"} {
		result, err := resolveTimeValue(in, 0)
		require.NoError(t, err, "resolveTimeValue(%q)", in)
		assert.NotEqual(t, in, result, "%q must resolve, not pass through raw", in)
		_, err = time.Parse(time.RFC3339Nano, result)
		require.NoError(t, err, "resolveTimeValue(%q) = %q", in, result)
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

// TestConversionFor pins how a column's type, as system.columns spells it,
// picks the conversion ClickHouse applies to a bound String.
func TestConversionFor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ  string
		want conversion
	}{
		{"DateTime", conversion{kind: parseTime, scale: 8}},
		{"DateTime('Asia/Tokyo')", conversion{kind: parseTime, scale: 8, zone: "Asia/Tokyo"}},
		{"DateTime64(3)", conversion{kind: parseTime, scale: 8}},
		{"DateTime64(3, 'America/New_York')", conversion{kind: parseTime, scale: 8, zone: "America/New_York"}},
		{"DateTime64(9, 'UTC')", conversion{kind: parseTime, scale: 9, zone: "UTC"}},
		{"Nullable(DateTime64(6, 'Etc/GMT+5'))", conversion{kind: parseTime, scale: 8, zone: "Etc/GMT+5"}},
		{"LowCardinality(Nullable(DateTime('America/Argentina/Buenos_Aires')))", conversion{kind: parseTime, scale: 8, zone: "America/Argentina/Buenos_Aires"}},
		{"Date", conversion{kind: parseDate}},
		{"Nullable(Date32)", conversion{kind: parseDate32}},
		{"String", conversion{kind: asString}},
		{"LowCardinality(String)", conversion{kind: asString}},
		{"", conversion{kind: asString}},
		{"UInt64", conversion{kind: castTo, typ: "UInt64"}},
		{"LowCardinality(Nullable(Int32))", conversion{kind: castTo, typ: "Int32"}},
		{"Decimal(18, 4)", conversion{kind: castTo, typ: "Decimal(18, 4)"}},
		{"Enum8('a' = 1, 'b' = 2)", conversion{kind: castTo, typ: "Enum8('a' = 1, 'b' = 2)"}},
		{"Array(DateTime)", conversion{kind: castTo, typ: "Array(DateTime)"}},
		// A DateTime type it cannot read leaves ClickHouse to refuse what it
		// cannot compare, rather than guess the zone.
		{"DateTime64(x)", conversion{kind: asString}},
		{"DateTime('a\\'b')", conversion{kind: asString}},
		{"DateTime64(3, 'UTC', 1)", conversion{kind: asString}},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, conversionFor(tt.typ))
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

	sql, params := bindTexts(t, result)
	assert.Contains(t, sql, "`received_timestamp` < parseDateTime64BestEffort({p0:String}, 8)")
	assert.Equal(t, []string{"2026-04-02T16:02:07.666Z"}, params, "the value reaches ClickHouse as written")
}

// TestBuild_TimestampValuesParseInClickHouse: a filter value on a Date or
// DateTime column — wrapped or not, scalar or inside an `in` list — reaches
// ClickHouse as written and is parsed there, in the column's declared zone
// when it has one. The same text on any other column is compared as it is,
// and like never converts.
func TestBuild_TimestampValuesParseInClickHouse(t *testing.T) {
	t.Parallel()
	schema := &discovery.TableSchema{Name: "events", Columns: []discovery.Column{
		{Name: "dt", Type: "DateTime"},
		{Name: "dtz", Type: "DateTime('Europe/Berlin')"},
		{Name: "dt64", Type: "Nullable(DateTime64(3, 'UTC'))"},
		{Name: "dt9", Type: "DateTime64(9, 'Asia/Tokyo')"},
		{Name: "lc", Type: "LowCardinality(Nullable(DateTime))"},
		{Name: "label", Type: "String"},
		{Name: "day", Type: "Date"},
		{Name: "day32", Type: "Date32"},
	}}
	const rfc = "2026-04-02T16:02:07.666Z"
	tests := []struct {
		column, op string
		value      any
		wantWhere  string
		wantParams []string
		wantTable  []byte
	}{
		{"dt", "eq", rfc, "`dt` = parseDateTime64BestEffort({p0:String}, 8)", []string{rfc}, nil},
		{"dtz", "gte", rfc, "`dtz` >= parseDateTime64BestEffort({p0:String}, 8, 'Europe/Berlin')", []string{rfc}, nil},
		{"dt64", "neq", rfc, "`dt64` != parseDateTime64BestEffort({p0:String}, 8, 'UTC')", []string{rfc}, nil},
		{"dt9", "lt", rfc, "`dt9` < parseDateTime64BestEffort({p0:String}, 9, 'Asia/Tokyo')", []string{rfc}, nil},
		{"lc", "lte", rfc, "`lc` <= parseDateTime64BestEffort({p0:String}, 8)", []string{rfc}, nil},
		{"dtz", "eq", "2026-04-02 16:02:07", "`dtz` = parseDateTime64BestEffort({p0:String}, 8, 'Europe/Berlin')", []string{"2026-04-02 16:02:07"}, nil},
		{"dt", "gt", json.Number("1782014400"), "`dt` > parseDateTime64BestEffort({p0:String}, 8)", []string{"1782014400"}, nil},
		{"day", "eq", rfc, "`day` = toDate(parseDateTime64BestEffort({p0:String}, 8))", []string{rfc}, nil},
		{"day32", "eq", "1950-01-01", "`day32` = toDate32(parseDateTime64BestEffort({p0:String}, 8))", []string{"1950-01-01"}, nil},
		{"label", "eq", rfc, "`label` = {p0:String}", []string{rfc}, nil},
		{"dt", "like", "2026-%", "`dt` LIKE {p0:String}", []string{"2026-%"}, nil},
		{
			"dt64", "in",
			[]any{rfc, "2026-04-02 16:02:07"},
			"`dt64` IN (SELECT parseDateTime64BestEffort(v, 8, 'UTC') FROM _p0)", nil,
			rowBinary(rfc, "2026-04-02 16:02:07"),
		},
		{"day", "in", []any{"2026-04-02"}, "`day` IN (SELECT toDate(parseDateTime64BestEffort(v, 8)) FROM _p0)", nil, rowBinary("2026-04-02")},
		{"label", "in", []any{rfc}, "`label` IN (SELECT v FROM _p0)", nil, rowBinary(rfc)},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s %s %v", tt.column, tt.op, tt.value), func(t *testing.T) {
			t.Parallel()
			sq := &StructuredQuery{Columns: []string{"label"}, Filters: []Filter{{Column: tt.column, Op: tt.op, Value: tt.value}}}
			result, err := Build("events", sq, schema, nil, 0, DefaultMaxRows)
			require.NoError(t, err)
			b, err := result.Bind()
			require.NoError(t, err)
			assert.Equal(t, "SELECT `label` FROM `events` WHERE "+tt.wantWhere+" LIMIT 10000", b.SQL)
			assert.Equal(t, tt.wantParams, paramValues(b))
			if tt.wantTable == nil {
				assert.Empty(t, b.Tables)
			} else {
				assert.Equal(t, []Table{{Name: "_p0", Data: tt.wantTable}}, b.Tables)
			}
		})
	}
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
				// backtick-quoted; only a '?' (which the positional-to-named rewrite
				// would miscount) is rejected.
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

// TestBuild_TimeRange_BoundsAreRFC3339: time_range bounds are instants in
// UTC, bound through the column's conversion, so the column's zone cannot
// shift them (#238 was a T-separated bound ClickHouse refused).
func TestBuild_TimeRange_BoundsAreRFC3339(t *testing.T) {
	t.Parallel()
	schema := &discovery.TableSchema{Name: "clicks", Columns: []discovery.Column{
		{Name: "page", Type: "String"},
		{Name: "ts", Type: "DateTime('Asia/Tokyo')"},
	}}
	sq := &StructuredQuery{
		Columns: []string{"page"},
		TimeRange: &TimeRange{
			Column: "ts",
			Since:  "2024-01-01T09:00:00+09:00",
			Until:  "2024-01-02T03:04:05Z",
		},
	}
	result, err := Build("clicks", sq, schema, nil, 0, DefaultMaxRows)
	require.NoError(t, err)
	sql, params := bindTexts(t, result)
	assert.Equal(t, "SELECT `page` FROM `clicks` WHERE `ts` >= parseDateTime64BestEffort({p0:String}, 8, 'Asia/Tokyo')"+
		" AND `ts` <= parseDateTime64BestEffort({p1:String}, 8, 'Asia/Tokyo') LIMIT 10000", sql)
	assert.Equal(t, []string{"2024-01-01T00:00:00Z", "2024-01-02T03:04:05Z"}, params)
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
	perms := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"page", "ts", "count"}}}
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
	perms := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"page", "ts", "count"}}}
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
			perms:    &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"*"}}},
			selectIs: "*",
		},
		{
			name:     "restricted expands to allowed projection (schema order)",
			perms:    &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"count", "page"}}},
			selectIs: "`page`, `count`",
		},
		{
			name:     "deny-list with empty allow expands and drops denied",
			perms:    &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{DenyColumns: []string{"org_id"}}},
			selectIs: "`page`, `button`, `count`, `ts`",
		},
		{
			name:     "deny-list with wildcard allow drops denied",
			perms:    &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"*"}, DenyColumns: []string{"org_id", "button"}}},
			selectIs: "`page`, `count`, `ts`",
		},
		{
			name:    "restricted with zero readable columns fails closed",
			perms:   &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"nonexistent"}}},
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
	perms := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"count"}, DeniedAggregations: []string{"sum"}}}
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
	perms := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"page"}}}
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
	perms := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{AllowColumns: []string{"nonexistent"}}}
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
// '?' would be miscounted by the positional-to-named parameter rewrite.
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

// TestBuild_InsertResolvedGrantIsRejected: a grant resolved for the wrong
// operation never produces a query, by whichever denier the query's shape
// reaches first. Each shape asserts its OWN error — a bare require.Error would
// pass just as happily if all three collapsed onto one denier, which is exactly
// the claim this test exists to check.
//
// Note the three do not divide into "names columns" vs "skips
// validateAndAuthorizeColumns": aggregation-only ENTERS that function and is
// denied inside it, in the aggregation loop. Only select_all traverses it
// without reaching an accessor.
func TestBuild_InsertResolvedGrantIsRejected(t *testing.T) {
	t.Parallel()
	insertResolved := &policy.ResolvedPermissions{Allowed: true, Insert: &policy.ResolvedInsert{}}
	for name, tc := range map[string]struct {
		q      *StructuredQuery
		assert func(t *testing.T, err error)
	}{
		"names a column → IsColumnAllowed": {
			q: &StructuredQuery{Columns: []string{"page"}},
			assert: func(t *testing.T, err error) {
				var e *ForbiddenColumnError
				assert.ErrorAs(t, err, &e)
			},
		},
		"select_all → AllowedProjection": {
			q: &StructuredQuery{SelectAll: true},
			assert: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, ErrNoReadableColumns)
			},
		},
		"aggregation only → IsAggregationAllowed": {
			q: &StructuredQuery{Aggregations: []Aggregation{{Fn: "count", Column: "*", Alias: "n"}}},
			assert: func(t *testing.T, err error) {
				var e *ForbiddenAggregationError
				assert.ErrorAs(t, err, &e)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			res, err := Build("clicks", tc.q, testSchema(), insertResolved, 0, DefaultMaxRows)
			require.Error(t, err, "a grant resolved for the wrong operation must not build a query")
			assert.Nil(t, res)
			tc.assert(t, err)
		})
	}

	// The control: the same query with a select-resolved grant builds.
	selectResolved := &policy.ResolvedPermissions{Allowed: true, Select: &policy.ResolvedSelect{}}
	res, err := Build("clicks", &StructuredQuery{Columns: []string{"page"}}, testSchema(), selectResolved, 0, DefaultMaxRows)
	require.NoError(t, err)
	assert.NotNil(t, res)
}

// TestBind pins the ClickHouse binding rule: every positional `?` becomes a
// named parameter or an external table, every scalar binds as String and
// every list as a table, in the order the WHERE assembly emitted them.
func TestBind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		params     []any
		wantSQL    string
		wantParams []Param
		wantTables []Table
	}{
		{
			name:    "no parameters",
			sql:     "SELECT `page` FROM `clicks` LIMIT 10",
			wantSQL: "SELECT `page` FROM `clicks` LIMIT 10",
		},
		{
			name:       "policy predicate keeps its leading position",
			sql:        "SELECT `page` FROM `clicks` WHERE (`org_id` = ?) AND `page` = ? LIMIT 100",
			params:     []any{"org-1", "/home"},
			wantSQL:    "SELECT `page` FROM `clicks` WHERE (`org_id` = {p0:String}) AND `page` = {p1:String} LIMIT 100",
			wantParams: []Param{{"p0", "org-1"}, {"p1", "/home"}},
		},
		{
			name:       "list binds as one table, named after its position",
			sql:        "SELECT * FROM `t` WHERE `a` = ? AND `page` IN ? AND `b` = ? LIMIT 10",
			params:     []any{"x", listParam{Values: []any{"/a", "/b"}}, "y"},
			wantSQL:    "SELECT * FROM `t` WHERE `a` = {p0:String} AND `page` IN (SELECT v FROM _p1) AND `b` = {p2:String} LIMIT 10",
			wantParams: []Param{{"p0", "x"}, {"p2", "y"}},
			wantTables: []Table{{Name: "_p1", Data: rowBinary("/a", "/b")}},
		},
		{
			name:       "a list on a typed column is cast element by element",
			sql:        "SELECT * FROM `t` WHERE `e` IN ? LIMIT 10",
			params:     []any{listParam{Values: []any{"it's"}, Conv: conversionFor("Enum8('it\\'s' = 1)")}},
			wantSQL:    "SELECT * FROM `t` WHERE `e` IN (SELECT accurateCastOrNull(v, 'Enum8(\\'it\\\\\\'s\\' = 1)') FROM _p0) LIMIT 10",
			wantTables: []Table{{Name: "_p0", Data: rowBinary("it's")}},
		},
		{
			name:       "a timestamp parses in ClickHouse",
			sql:        "SELECT * FROM `t` WHERE `ts` > ? LIMIT 10",
			params:     []any{conversionFor("DateTime('Asia/Tokyo')").scalar("2026-06-21T04:00:00.5Z")},
			wantSQL:    "SELECT * FROM `t` WHERE `ts` > parseDateTime64BestEffort({p0:String}, 8, 'Asia/Tokyo') LIMIT 10",
			wantParams: []Param{{"p0", "2026-06-21T04:00:00.5Z"}},
		},
		{
			name:   "numbers keep the caller's own digits",
			sql:    "SELECT * FROM `t` WHERE `a` = ? AND `b` = ? AND `c` = ? LIMIT 10",
			params: []any{json.Number("12.50"), json.Number("9007199254740993"), true},
			wantSQL: "SELECT * FROM `t` WHERE `a` = {p0:String} AND `b` = {p1:String} " +
				"AND `c` = {p2:String} LIMIT 10",
			wantParams: []Param{{"p0", "12.50"}, {"p1", "9007199254740993"}, {"p2", "true"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := (&BuildResult{SQL: tt.sql, Params: tt.params}).Bind()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, b.SQL)
			assert.Equal(t, tt.wantParams, b.Params)
			assert.Equal(t, tt.wantTables, b.Tables)
		})
	}
}

// TestBind_Encoding pins how a value travels. A scalar {p:String} is read by
// ClickHouse's escaped-text reader, so a raw backslash is taken as the start
// of an escape sequence ("a\b" came back holding a backspace, measured on
// 26.6.3.62) and a raw tab ends the field outright (code 457, measured again
// on 26.8.15.10); chsql.EscapeStringParam encodes both. An `in` list's
// elements travel as RowBinary, each one's bytes as written behind its
// length, so nothing in one can end it or reach the SQL.
func TestBind_Encoding(t *testing.T) {
	t.Parallel()
	scalars := []struct {
		name      string
		value     any
		wantParam string
	}{
		{"plain", "hello", "hello"},
		{"single quote needs nothing", "it's", "it's"},
		{"backslash", `a\b`, `a\\b`},
		{"windows path", `C:\Users\x`, `C:\\Users\\x`},
		{"tab", "a\tb", `a\tb`},
		{"newline", "a\nb", `a\nb`},
		{"carriage return", "a\rb", `a\rb`},
		{"a literal backslash-n", `a\nb`, `a\\nb`},
		{"like pattern", "%foo%", "%foo%"},
	}
	for _, tt := range scalars {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, params := bindTexts(t, &BuildResult{SQL: "SELECT ?", Params: []any{tt.value}})
			assert.Equal(t, []string{tt.wantParam}, params)
		})
	}

	t.Run("list elements are their own bytes", func(t *testing.T) {
		t.Parallel()
		vals := []string{`it's`, `a\b`, "a\tb", "a\nb", `']) OR 1=1 --`, "", "\x00z", strings.Repeat("x", 300)}
		list := make([]any, len(vals))
		for i, v := range vals {
			list[i] = v
		}
		b, err := (&BuildResult{SQL: "SELECT * FROM t WHERE c IN ?", Params: []any{listParam{Values: list}}}).Bind()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM t WHERE c IN (SELECT v FROM _p0)", b.SQL)
		require.Len(t, b.Tables, 1)
		// Decode the RowBinary back: varint length, then the bytes.
		var got []string
		for data := b.Tables[0].Data; len(data) > 0; {
			n, w := binary.Uvarint(data)
			require.Positive(t, w)
			end := w + int(n) //nolint:gosec // G115: a test value's length, far below MaxInt
			got = append(got, string(data[w:end]))
			data = data[end:]
		}
		assert.Equal(t, vals, got)
	})
}

// TestBind_Rejects covers the values and shapes that have no honest binding.
// A JSON null is the notable one: it used to bind as `col = NULL` (never
// true), where an empty String parameter would compare against the empty
// string — a different question, so it is refused (→ 400).
func TestBind_Rejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		params  []any
		wantErr string
	}{
		{"null value", "SELECT ?", []any{nil}, "must not be null"},
		{"null in a list", "SELECT ?", []any{listParam{Values: []any{"a", nil}}}, "must not be null"},
		{"null timestamp", "SELECT ?", []any{conversionFor("DateTime").scalar(nil)}, "must not be null"},
		{"object value", "SELECT ?", []any{map[string]any{"k": "v"}}, "unsupported filter value type"},
		{"list as a scalar", "SELECT ?", []any{[]any{"a"}}, "unsupported filter value type"},
		{"nested list", "SELECT ?", []any{listParam{Values: []any{[]any{"a"}}}}, "nested list"},
		{"more values than placeholders", "SELECT 1", []any{"a"}, "only 0 placeholders"},
		{"more placeholders than values", "SELECT ?, ?", []any{"a"}, "more placeholders"},
		{"placeholder with no values", "SELECT ?", nil, "no bound values"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := (&BuildResult{SQL: tt.sql, Params: tt.params}).Bind()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
