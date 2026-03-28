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
	assert.Equal(t, "SELECT * FROM clicks WHERE page = ? AND count > ?", sql)
	assert.Equal(t, []any{"/home", 10}, params)
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
	assert.Equal(t, "SELECT * FROM clicks LIMIT ?", sql)
	assert.Equal(t, []any{100}, params)
}

func TestBindParams_MultipleOccurrences(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE a = {{val}} OR b = {{val}}",
		Parameters: []ParamDef{{Name: "val", Type: "string", Required: true}},
	}
	sql, params, err := BindParams(q, map[string]any{"val": "x"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = ? OR b = ?", sql)
	assert.Equal(t, []any{"x", "x"}, params)
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
	assert.Equal(t, "SELECT * FROM clicks LIMIT ?", sql)
	assert.Equal(t, []any{50}, params)
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

func TestMemoryStore_GetListPut(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore(
		&NamedQuery{Name: "q1", SQL: "SELECT 1"},
		&NamedQuery{Name: "q2", SQL: "SELECT 2"},
	)

	// Get.
	q := store.Get("q1")
	require.NotNil(t, q)
	assert.Equal(t, "SELECT 1", q.SQL)

	assert.Nil(t, store.Get("missing"))

	// List.
	all := store.List()
	assert.Len(t, all, 2)

	// Put (memory store doesn't have KV, so Put won't work without kv).
	// Verify cached state directly.
	store.mu.Lock()
	store.cached["q3"] = &NamedQuery{Name: "q3", SQL: "SELECT 3"}
	store.mu.Unlock()

	assert.NotNil(t, store.Get("q3"))
	assert.Len(t, store.List(), 3)
}

func TestMemoryStore_Empty(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	assert.Empty(t, store.List())
	assert.Nil(t, store.Get("anything"))
}
