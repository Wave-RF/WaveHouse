package pipes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindParams_AllSupplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE page = {{page}} AND count > {{min_count}}",
		Parameters: []ParamDef{
			{Name: "page", Type: "string", Required: true},
			{Name: "min_count", Type: "number", Required: true},
		},
	}
	sql, params, err := BindParams(q, map[string]any{"page": "/home", "min_count": 10})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks WHERE page = '/home' AND count > 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_MissingRequired(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks WHERE page = {{page}}",
		Parameters: []ParamDef{{Name: "page", Type: "string", Required: true}},
	}
	_, _, err := BindParams(q, map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: page")
}

func TestBindParams_DefaultApplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks LIMIT {{limit}}",
		Parameters: []ParamDef{{Name: "limit", Type: "number", Default: 100}},
	}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 100", sql)
	assert.Nil(t, params)
}

func TestBindParams_MultipleOccurrences(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE a = {{val}} OR b = {{val}}",
		Parameters: []ParamDef{{Name: "val", Type: "string", Required: true}},
	}
	sql, params, err := BindParams(q, map[string]any{"val": "x"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'x' OR b = 'x'", sql)
	assert.Nil(t, params)
}

func TestBindParams_NoParameters(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT count(*) FROM clicks"}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT count(*) FROM clicks", sql)
	assert.Empty(t, params)
}

func TestBindParams_OptionalWithDefault_Supplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks LIMIT {{limit}}",
		Parameters: []ParamDef{{Name: "limit", Type: "number", Default: 100}},
	}
	sql, params, err := BindParams(q, map[string]any{"limit": 50})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 50", sql)
	assert.Nil(t, params)
}

func TestBindParams_PlaceholderNotInSQL(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks",
		Parameters: []ParamDef{{Name: "unused", Type: "string"}},
	}
	sql, params, err := BindParams(q, map[string]any{"unused": "val"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks", sql)
	assert.Empty(t, params, "unused param should not generate positional args")
}

func TestBindParams_InlineDefault_NoFormalParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT page, count() FROM clicks GROUP BY page LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT page, count() FROM clicks GROUP BY page LIMIT 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_InlineDefault_SuppliedOverrides(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{"limit": float64(5)})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 5", sql)
	assert.Nil(t, params)
}

func TestBindParams_InlineNoDefault_MissingRequired(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE page = {{page}}",
	}
	_, _, err := BindParams(q, map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: page")
}

func TestBindParams_InlineMultipleParams(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE country = {{country:US}} LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks WHERE country = 'US' LIMIT 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_StringEscaping(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE name = {{name}}",
		Parameters: []ParamDef{{Name: "name", Type: "string", Required: true}},
	}
	sql, _, err := BindParams(q, map[string]any{"name": "O'Brien"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE name = 'O''Brien'", sql)
}

func TestBindParams_BooleanParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE active = {{active}}",
		Parameters: []ParamDef{{Name: "active", Type: "boolean", Required: true}},
	}
	sql, _, err := BindParams(q, map[string]any{"active": true})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE active = 1", sql)
}

func TestBindParams_NilParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE col = {{val}}",
		Parameters: []ParamDef{{Name: "val", Type: "string", Default: nil}},
	}
	sql, _, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE col = NULL", sql)
}

func TestFormatParamValue_OK(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil is NULL", nil, "NULL"},
		{"string quoted", "hello", "'hello'"},
		{"numeric string bare", "100", "100"},
		{"string escapes quote", "O'Brien", "'O''Brien'"},
		{"string escapes backslash", `a\b`, `'a\\b'`},
		{"whole float as int", float64(42), "42"},
		{"fractional float", float64(42.5), "42.5"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"int", 7, "7"},
		{"array of strings -> IN list", []any{"a", "b"}, "('a', 'b')"},
		{"array of numbers", []any{float64(1), float64(2), float64(3)}, "(1, 2, 3)"},
		{"array escapes each element", []any{"x' OR 1=1"}, "('x'' OR 1=1')"},
		{"nested array recurses", []any{[]any{"a"}, "b"}, "(('a'), 'b')"},
		{"single-element array", []any{"only"}, "('only')"},
		// Only canonical numeric literals render bare; Go-only spellings that
		// ClickHouse can't parse are quoted instead of emitted as invalid bare SQL.
		{"underscore number is quoted not bare", "1_000", "'1_000'"},
		{"non-finite is quoted not bare", "Inf", "'Inf'"},
		{"hex float is quoted not bare", "0x1p-2", "'0x1p-2'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := formatParamValue(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFormatParamValue_Rejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      any
		wantErr string
	}{
		{"empty array", []any{}, "array parameter must not be empty"},
		{"object", map[string]any{"k": "v"}, "unsupported parameter type object"},
		{"array containing object", []any{map[string]any{"k": "v"}}, "unsupported parameter type object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := formatParamValue(tc.in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestBindParams_ArrayInClause(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE id IN {{ids}}",
		Parameters: []ParamDef{{Name: "ids", Type: "array", Required: true}},
	}
	sql, params, err := BindParams(q, map[string]any{"ids": []any{"a", "b", "c"}})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE id IN ('a', 'b', 'c')", sql)
	assert.Nil(t, params)
}

func TestBindParams_ArrayWorksWithoutDeclaredType(t *testing.T) {
	t.Parallel()
	// An undeclared (inline) parameter still renders an array safely.
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE id IN {{ids}}"}
	sql, _, err := BindParams(q, map[string]any{"ids": []any{float64(1), float64(2)}})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE id IN (1, 2)", sql)
}

func TestBindParams_ArrayMixedNumericAndStringElements(t *testing.T) {
	t.Parallel()
	// Array elements carry no declared element type, so each leaf decides bare vs
	// quoted on its own: a numeric-looking string renders bare, a non-numeric one
	// is quoted (and escaped).
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE id IN {{ids}}"}
	sql, _, err := BindParams(q, map[string]any{"ids": []any{"100", "abc"}})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE id IN (100, 'abc')", sql)
}

// TestBindParams_ArrayNeutralizesInjection is the #317 regression: an array
// element's inner single quote can no longer break out of its literal. The
// UNION payload survives only as inert text inside a quoted list element.
func TestBindParams_ArrayNeutralizesInjection(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT secret FROM t WHERE id IN {{ids}}"}
	sql, _, err := BindParams(q, map[string]any{
		"ids": []any{"' UNION SELECT pw FROM users -- "},
	})
	require.NoError(t, err)
	assert.Equal(t, "SELECT secret FROM t WHERE id IN (''' UNION SELECT pw FROM users -- ')", sql)
}

func TestBindParams_ObjectRejected(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE col = {{p}}"}
	_, _, err := BindParams(q, map[string]any{"p": map[string]any{"k": "v"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `parameter "p"`)
	assert.Contains(t, err.Error(), "unsupported parameter type object")
}

func TestBindParams_EmptyArrayRejected(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE id IN {{ids}}"}
	_, _, err := BindParams(q, map[string]any{"ids": []any{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "array parameter must not be empty")
}

func TestStatic_PipeAndPipes(t *testing.T) {
	t.Parallel()
	a := &NamedQuery{Name: "a", SQL: "SELECT 1"}
	b := &NamedQuery{Name: "b", SQL: "SELECT 2"}
	src := Static(a, b)
	assert.Same(t, a, src.Pipe("a"))
	assert.Nil(t, src.Pipe("missing"))
	assert.Len(t, src.Pipes(), 2)
	assert.Empty(t, Static().Pipes())
}
