package stream

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
)

func TestFilterColumns_NilPerms(t *testing.T) {
	t.Parallel()
	data := map[string]any{"a": 1, "b": 2}
	assert.Equal(t, data, filterColumns(data, nil), "nil perms returns the original data")
}

func TestFilterColumns_NilData(t *testing.T) {
	t.Parallel()
	assert.Nil(t, filterColumns(nil, &policy.ResolvedPermissions{Allowed: true}))
}

func TestFilterColumns_AllowList(t *testing.T) {
	t.Parallel()
	data := map[string]any{"page": "/home", "secret": "xyz", "button": "signup"}
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"page", "button"}}
	result := filterColumns(data, perms)

	assert.Equal(t, "/home", result["page"])
	assert.Equal(t, "signup", result["button"])
	assert.NotContains(t, result, "secret")
}

func TestFilterColumns_DoesNotMutateOriginal(t *testing.T) {
	t.Parallel()
	data := map[string]any{"a": 1, "b": 2, "c": 3}
	perms := &policy.ResolvedPermissions{Allowed: true, AllowColumns: []string{"a"}}
	_ = filterColumns(data, perms)

	assert.Contains(t, data, "a")
	assert.Contains(t, data, "b")
	assert.Contains(t, data, "c")
}

func TestFilterColumns_DenyList(t *testing.T) {
	t.Parallel()
	data := map[string]any{"page": "/home", "secret_col": "xyz"}
	perms := &policy.ResolvedPermissions{Allowed: true, DenyColumns: []string{"secret_col"}}
	result := filterColumns(data, perms)

	assert.Contains(t, result, "page")
	assert.NotContains(t, result, "secret_col")
}
