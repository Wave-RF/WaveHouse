package pipes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDummyBind_TypedFormalParams(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE page = {{page}} AND count > {{min_count}}",
		Parameters: []ParamDef{
			{Name: "page", Type: "string", Required: true},
			{Name: "min_count", Type: "number", Required: true},
		},
	}
	sql, err := DummyBind(q)
	require.NoError(t, err)
	// String dummy is quoted, number dummy is bare — type-correct so the SQL stays
	// analyzable; the concrete values are irrelevant to which tables are read.
	assert.Equal(t, "SELECT * FROM clicks WHERE page = 'x' AND count > 0", sql)
}

func TestDummyBind_BooleanFormalParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE active = {{flag}}",
		Parameters: []ParamDef{{Name: "flag", Type: "boolean", Required: true}},
	}
	sql, err := DummyBind(q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE active = 0", sql)
}

func TestDummyBind_BareInlineParamGetsDummy(t *testing.T) {
	t.Parallel()
	// {{c}} has no formal definition and no inline default — DummyBind must still
	// cover it so BindParams doesn't fail "missing required parameter".
	q := &NamedQuery{SQL: "SELECT * FROM t WHERE c = {{c}}"}
	sql, err := DummyBind(q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE c = 'x'", sql)
}

func TestDummyBind_InlineDefaultPreserved(t *testing.T) {
	t.Parallel()
	// An inline default is the author's own valid value — DummyBind leaves it for
	// BindParams rather than overriding it with a dummy.
	q := &NamedQuery{SQL: "SELECT * FROM t LIMIT {{lim:10}}"}
	sql, err := DummyBind(q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t LIMIT 10", sql)
}

func TestDummyBind_NoParams(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT 1"}
	sql, err := DummyBind(q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", sql)
}
