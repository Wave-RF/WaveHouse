package pipes

import (
	"encoding/json"
	"errors"
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
	sql, params, err := BindParams(q, map[string]any{"page": "/home", "min_count": 10}, nil)
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
	_, _, err := BindParams(q, map[string]any{}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: page")
}

func TestBindParams_DefaultApplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks LIMIT {{limit}}",
		Parameters: []ParamDef{{Name: "limit", Type: "number", Default: 100}},
	}
	sql, params, err := BindParams(q, map[string]any{}, nil)
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
	sql, params, err := BindParams(q, map[string]any{"val": "x"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'x' OR b = 'x'", sql)
	assert.Nil(t, params)
}

func TestBindParams_NoParameters(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT count(*) FROM clicks"}
	sql, params, err := BindParams(q, map[string]any{}, nil)
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
	sql, params, err := BindParams(q, map[string]any{"limit": 50}, nil)
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
	sql, params, err := BindParams(q, map[string]any{"unused": "val"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks", sql)
	assert.Empty(t, params, "unused param should not generate positional args")
}

func TestBindParams_InlineDefault_NoFormalParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT page, count() FROM clicks GROUP BY page LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT page, count() FROM clicks GROUP BY page LIMIT 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_InlineDefault_SuppliedOverrides(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{"limit": float64(5)}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 5", sql)
	assert.Nil(t, params)
}

func TestBindParams_InlineNoDefault_MissingRequired(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE page = {{page}}",
	}
	_, _, err := BindParams(q, map[string]any{}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: page")
}

func TestBindParams_InlineMultipleParams(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE country = {{country:US}} LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{}, nil)
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
	sql, _, err := BindParams(q, map[string]any{"name": "O'Brien"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE name = 'O''Brien'", sql)
}

func TestBindParams_BooleanParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE active = {{active}}",
		Parameters: []ParamDef{{Name: "active", Type: "boolean", Required: true}},
	}
	sql, _, err := BindParams(q, map[string]any{"active": true}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE active = 1", sql)
}

func TestBindParams_NilParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE col = {{val}}",
		Parameters: []ParamDef{{Name: "val", Type: "string", Default: nil}},
	}
	sql, _, err := BindParams(q, map[string]any{}, nil)
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
	sql, params, err := BindParams(q, map[string]any{"ids": []any{"a", "b", "c"}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE id IN ('a', 'b', 'c')", sql)
	assert.Nil(t, params)
}

func TestBindParams_ArrayWorksWithoutDeclaredType(t *testing.T) {
	t.Parallel()
	// An undeclared (inline) parameter still renders an array safely.
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE id IN {{ids}}"}
	sql, _, err := BindParams(q, map[string]any{"ids": []any{float64(1), float64(2)}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE id IN (1, 2)", sql)
}

func TestBindParams_ArrayMixedNumericAndStringElements(t *testing.T) {
	t.Parallel()
	// Array elements carry no declared element type, so each leaf decides bare vs
	// quoted on its own: a numeric-looking string renders bare, a non-numeric one
	// is quoted (and escaped).
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE id IN {{ids}}"}
	sql, _, err := BindParams(q, map[string]any{"ids": []any{"100", "abc"}}, nil)
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
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT secret FROM t WHERE id IN (''' UNION SELECT pw FROM users -- ')", sql)
}

func TestBindParams_ObjectRejected(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE col = {{p}}"}
	_, _, err := BindParams(q, map[string]any{"p": map[string]any{"k": "v"}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `parameter "p"`)
	assert.Contains(t, err.Error(), "unsupported parameter type object")
}

func TestBindParams_EmptyArrayRejected(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE id IN {{ids}}"}
	_, _, err := BindParams(q, map[string]any{"ids": []any{}}, nil)
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

// TestBindParams_ClaimBindsQuoted: a {{ jwt.<path> }} reference binds from the
// token beside ordinary request parameters, as a quoted literal.
func TestBindParams_ClaimBindsQuoted(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM orders WHERE tenant_id = {{ jwt.app_metadata.tenant_id }} AND ts >= {{start}}",
		Parameters: []ParamDef{{Name: "start", Type: "string", Required: true}},
	}
	claims := map[string]any{"app_metadata": map[string]any{"tenant_id": "acme"}}
	sql, params, err := BindParams(q, map[string]any{"start": "2026-01-01"}, claims)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM orders WHERE tenant_id = 'acme' AND ts >= '2026-01-01'", sql)
	assert.Nil(t, params)
}

func TestBindParams_ClaimArrayBindsAsSet(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM orders WHERE tenant_id IN {{ jwt.app_metadata.tenant_ids }}"}
	claims := map[string]any{"app_metadata": map[string]any{"tenant_ids": []any{"acme", "globex"}}}
	sql, _, err := BindParams(q, nil, claims)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM orders WHERE tenant_id IN ('acme', 'globex')", sql)
}

// TestBindParams_ClaimStaysQuoted: a claim binds quoted whatever it spells,
// the way a policy filter binds it (a string parameter), where a request
// parameter renders a numeric-looking string bare. A numeric claim binds in
// canonical form.
func TestBindParams_ClaimStaysQuoted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		claim any
		want  string
	}{
		{"numeric string", "42", "SELECT 1 WHERE a = '42' AND b = 42"},
		{"json number", json.Number("42"), "SELECT 1 WHERE a = '42' AND b = 42"},
		{"float spelling canonicalized", json.Number("1.0"), "SELECT 1 WHERE a = '1' AND b = 42"},
		{"boolean", true, "SELECT 1 WHERE a = 'true' AND b = 42"},
		{"present but empty", "", "SELECT 1 WHERE a = '' AND b = 42"},
		{"escaped", `o'reilly\`, `SELECT 1 WHERE a = 'o''reilly\\' AND b = 42`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q := &NamedQuery{SQL: "SELECT 1 WHERE a = {{jwt.v}} AND b = {{v}}"}
			sql, _, err := BindParams(q, map[string]any{"v": "42"}, map[string]any{"v": tt.claim})
			require.NoError(t, err)
			assert.Equal(t, tt.want, sql)
		})
	}
}

// TestBindParams_ClaimUnresolvedFailsClosed: a reference the token cannot bind
// is *UnresolvedClaimError naming the path, never a stand-in literal.
func TestBindParams_ClaimUnresolvedFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		claims map[string]any
	}{
		{"no token", nil},
		{"absent", map[string]any{"other": "x"}},
		{"null", map[string]any{"tenant_ids": nil}},
		{"object", map[string]any{"tenant_ids": map[string]any{"k": "v"}}},
		{"empty array", map[string]any{"tenant_ids": []any{}}},
		{"non-scalar element", map[string]any{"tenant_ids": []any{"a", []any{"b"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q := &NamedQuery{SQL: "SELECT * FROM t WHERE tenant_id IN {{ jwt.tenant_ids }}"}
			_, _, err := BindParams(q, map[string]any{"tenant_ids": []any{"evil"}}, tt.claims)
			var unresolved *UnresolvedClaimError
			require.ErrorAs(t, err, &unresolved)
			assert.Equal(t, "tenant_ids", unresolved.Path)
			assert.Equal(t, `claim "tenant_ids" cannot be resolved from the token`, err.Error())
		})
	}
}

// TestBindParams_ClaimNeverReadsRequest: no request parameter, however named,
// stands in for a claim — the boundary the feature exists for.
func TestBindParams_ClaimNeverReadsRequest(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE tenant_id = {{ jwt.tenant_id }}"}
	supplied := map[string]any{"jwt.tenant_id": "evil", "jwt": map[string]any{"tenant_id": "evil"}, "tenant_id": "evil"}

	_, _, err := BindParams(q, supplied, nil)
	var unresolved *UnresolvedClaimError
	require.ErrorAs(t, err, &unresolved)

	sql, _, err := BindParams(q, supplied, map[string]any{"tenant_id": "acme"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE tenant_id = 'acme'", sql)
}

// TestBindParams_BoundTextNotRescanned: every placeholder resolves in one pass
// over the template, so a bound value that spells a placeholder is inert text,
// in either direction.
func TestBindParams_BoundTextNotRescanned(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE a = {{ jwt.a }} AND b = {{b}}"}
	sql, _, err := BindParams(q, map[string]any{"b": "{{ jwt.a }}", "limit": 5}, map[string]any{"a": "{{limit}}"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = '{{limit}}' AND b = '{{ jwt.a }}'", sql)
}

func TestBindParams_ClaimSpacingForms(t *testing.T) {
	t.Parallel()
	for _, tmpl := range []string{"{{jwt.a}}", "{{ jwt.a }}", "{{  jwt.a  }}"} {
		q := &NamedQuery{SQL: "SELECT " + tmpl}
		sql, _, err := BindParams(q, nil, map[string]any{"a": "x"})
		require.NoError(t, err, tmpl)
		assert.Equal(t, "SELECT 'x'", sql, tmpl)
	}
}

// TestBindParams_ValueCannotEndComment: a bound value's newline, carriage
// return or `*` is written as its escape, so it binds the same value but can
// neither end a `--` comment a placeholder sits in nor close or open a
// `/* */` one — for a claim or a parameter.
func TestBindParams_ValueCannotEndComment(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT 1 FROM t WHERE a IN {{ jwt.ids }} /* AND b = {{ jwt.region }} */ -- AND c = {{c}}"}
	claims := map[string]any{"ids": []any{"1"}, "region": "*/ OR 1=1 /*"}
	sql, _, err := BindParams(q, map[string]any{"c": "y\r\nOR 1=1"}, claims)
	require.NoError(t, err)
	assert.Equal(t, `SELECT 1 FROM t WHERE a IN ('1') /* AND b = '\x2A/ OR 1=1 /\x2A' */ -- AND c = 'y\r\nOR 1=1'`, sql)
	assert.NotContains(t, sql, "\n")
	// A backslash ahead of the star is doubled and the star still escaped.
	assert.Equal(t, `'\\\x2A/'`, quoteLiteral(`\*/`))
}

// TestBindParams_NegativeNumberCannotStartComment: a negative bare number
// renders with a leading space, so bound after a `-` in the template it is
// arithmetic, never the `--` that would comment out the rest of the line and
// the claim predicate with it.
func TestBindParams_NegativeNumberCannotStartComment(t *testing.T) {
	t.Parallel()
	for name, min := range map[string]any{"numeric string": "-5", "float64": float64(-5), "int": -5, "fraction": -2.5} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			q := &NamedQuery{SQL: "SELECT 1 FROM t WHERE n > 0-{{min}} AND tenant_id IN {{ jwt.ids }}"}
			sql, _, err := BindParams(q, map[string]any{"min": min}, map[string]any{"ids": []any{"acme"}})
			require.NoError(t, err)
			assert.NotContains(t, sql, "--")
			assert.Contains(t, sql, "n > 0- -")
			assert.Contains(t, sql, "tenant_id IN ('acme')")
		})
	}
	sql, _, err := BindParams(&NamedQuery{SQL: "SELECT {{n}}"}, map[string]any{"n": 5}, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 5", sql, "a non-negative number renders as before")
}

func TestNamedQuery_Claims(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT {{a}} WHERE t IN {{ jwt.app_metadata.tenant_ids }} AND u = {{jwt.sub}} AND t2 IN {{ jwt.app_metadata.tenant_ids }}"}
	assert.Equal(t, []string{"app_metadata.tenant_ids", "sub"}, q.Claims())
	assert.Nil(t, (&NamedQuery{SQL: "SELECT {{a}}"}).Claims())
	// A claim-shaped fragment inside a parameter's inline default binds the
	// parameter, so it is not a claim the pipe binds: Claims and BindParams
	// share one scan.
	inDefault := &NamedQuery{SQL: "SELECT * FROM t WHERE tenant_id = {{t:{{ jwt.tenant_id }}  -- }}"}
	assert.Nil(t, inDefault.Claims())
	sql, _, err := BindParams(inDefault, map[string]any{"t": "req"}, map[string]any{"tenant_id": "tok"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE tenant_id = 'req'  -- }}", sql)
}

// TestNamedQuery_Skeleton: every placeholder becomes an empty string
// literal, so the statement's verb is readable where a `{{` would hide it
// from a classifier.
func TestNamedQuery_Skeleton(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "EXECUTE AS {{ jwt.sub }} INSERT INTO audit (msg) SELECT {{msg:x}} WHERE t IN {{jwt.tenant_ids}}"}
	assert.Equal(t, "EXECUTE AS '' INSERT INTO audit (msg) SELECT '' WHERE t IN ''", q.Skeleton())
	assert.Equal(t, "SELECT 1", (&NamedQuery{SQL: "SELECT 1"}).Skeleton())
}

func TestNamedQuery_CheckTemplate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		q    NamedQuery
		want string // substring of the error; empty for a template that passes
	}{
		{"no placeholders", NamedQuery{SQL: "SELECT 1"}, ""},
		{"parameters and claims", NamedQuery{SQL: "SELECT {{a}}, {{b:1}} WHERE t IN {{ jwt.app_metadata.tenant_ids }}", Parameters: []ParamDef{{Name: "a"}}}, ""},
		// ClickHouse's own `{{` — format()'s brace escape, a string literal —
		// starts like no placeholder and is left to ClickHouse.
		{"format brace escape", NamedQuery{SQL: `SELECT format('{{"day": "{}"}}', toString(day)) AS j FROM t WHERE d = {{d}}`}, ""},
		{"brace string literal", NamedQuery{SQL: "SELECT '{{' AS open, '{{ }}' AS pair"}, ""},
		// Where ClickHouse reads text, the binder would still bind.
		{"placeholder in a line comment", NamedQuery{SQL: "SELECT 1 FROM t -- WHERE tenant_id = {{ jwt.tenant_id }}"}, `placeholder "{{ jwt.tenant_id }}" inside a comment`},
		{"placeholder in a commented-out line", NamedQuery{SQL: "SELECT 1 FROM t\n-- WHERE a = {{a}}\nWHERE b = {{b}}"}, `placeholder "{{a}}" inside a comment`},
		{"placeholder in a block comment", NamedQuery{SQL: "SELECT /* {{a}} */ 1"}, `placeholder "{{a}}" inside a comment`},
		{"placeholder in a hash comment", NamedQuery{SQL: "SELECT 1 # {{a}}"}, `placeholder "{{a}}" inside a comment`},
		{"placeholder in quotes", NamedQuery{SQL: "SELECT 1 WHERE order_id = '{{order_id}}'"}, `placeholder "{{order_id}}" inside quotes`},
		{"placeholder inside a longer string", NamedQuery{SQL: "SELECT 1 WHERE name LIKE 'a {{n}} b'"}, `placeholder "{{n}}" inside quotes`},
		{"claim in quotes", NamedQuery{SQL: "SELECT 1 WHERE t = '{{ jwt.tenant_id }}'"}, `placeholder "{{ jwt.tenant_id }}" inside quotes`},
		{"placeholder in a quoted identifier", NamedQuery{SQL: "SELECT \"{{col}}\" FROM t"}, `placeholder "{{col}}" inside quotes`},
		{"placeholder in a backtick identifier", NamedQuery{SQL: "SELECT `{{col}}` FROM t"}, `placeholder "{{col}}" inside quotes`},
		{"placeholder in a heredoc", NamedQuery{SQL: "SELECT $$ {{a}} $$"}, `placeholder "{{a}}" inside quotes`},
		{"placeholder after a closed string", NamedQuery{SQL: "SELECT 1 WHERE a = 'x' AND b = {{b}}"}, ""},
		{"placeholder after a comment line", NamedQuery{SQL: "-- totals\nSELECT {{a}}"}, ""},
		{"quote in an inline default opens nothing", NamedQuery{SQL: "SELECT {{d:it's}} WHERE b = {{b}}"}, ""},
		{"inline placeholder named jwt", NamedQuery{SQL: "SELECT {{jwt}}"}, `placeholder "{{jwt}}" names the reserved parameter "jwt"`},
		{"inline default named jwt", NamedQuery{SQL: "SELECT {{jwt:x}}"}, `placeholder "{{jwt:x}}" names the reserved parameter "jwt"`},
		{"claim path outside the grammar", NamedQuery{SQL: "SELECT 1 WHERE t = {{ jwt.tenant-id }}"}, `unrecognized placeholder "{{ jwt.tenant-id }}"`},
		{"prefix without a path", NamedQuery{SQL: "SELECT {{ jwt }}"}, `unrecognized placeholder "{{ jwt }}"`},
		{"spaced parameter placeholder", NamedQuery{SQL: "SELECT 1 LIMIT {{ limit }}"}, `unrecognized placeholder "{{ limit }}"`},
		{"unterminated", NamedQuery{SQL: "SELECT {{ jwt.a"}, `unrecognized placeholder "{{ jwt.a"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.q.CheckTemplate()
			if tt.want == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestUnresolvedClaimError_Is(t *testing.T) {
	t.Parallel()
	err := error(&UnresolvedClaimError{Path: "sub"})
	var target *UnresolvedClaimError
	assert.True(t, errors.As(err, &target))
}
