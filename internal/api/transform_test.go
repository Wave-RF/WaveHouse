package api

import (
	"encoding/json"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformForClient_ValidEventMessage(t *testing.T) {
	t.Parallel()
	raw := `{"table_name":"clicks","received_timestamp":"2024-01-01T00:00:00Z","data":{"page":"/home"}}`
	out, err := transformForClient([]byte(raw))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(out, &result))
	assert.Equal(t, "clicks", result["table_name"])
	assert.Equal(t, "2024-01-01T00:00:00Z", result["received_timestamp"])
	data, ok := result["data"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "/home", data["page"])
}

func TestTransformForClient_NonEventMessage(t *testing.T) {
	t.Parallel()
	raw := `{"some":"arbitrary","json":true}`
	out, err := transformForClient([]byte(raw))
	require.NoError(t, err)
	// Should pass through unchanged.
	assert.JSONEq(t, raw, string(out))
}

func TestTransformForClient_InvalidJSON(t *testing.T) {
	t.Parallel()
	out, err := transformForClient([]byte("not json"))
	require.NoError(t, err)
	// Should pass through unchanged.
	assert.Equal(t, "not json", string(out))
}

func TestFilterEventColumns_NilPerms(t *testing.T) {
	t.Parallel()
	data := map[string]any{"a": 1, "b": 2}
	result := filterEventColumns(data, nil)
	assert.Equal(t, data, result, "nil perms should return original data")
}

func TestFilterEventColumns_NilData(t *testing.T) {
	t.Parallel()
	perms := &policy.ResolvedPermissions{Allowed: true}
	result := filterEventColumns(nil, perms)
	assert.Nil(t, result)
}

func TestFilterEventColumns_Allowed(t *testing.T) {
	t.Parallel()
	data := map[string]any{"page": "/home", "secret": "xyz", "button": "signup"}
	perms := &policy.ResolvedPermissions{
		Allowed:      true,
		AllowColumns: []string{"page", "button"},
	}
	result := filterEventColumns(data, perms)

	assert.Equal(t, "/home", result["page"])
	assert.Equal(t, "signup", result["button"])
	assert.NotContains(t, result, "secret")
}

func TestFilterEventColumns_DoesNotMutateOriginal(t *testing.T) {
	t.Parallel()
	data := map[string]any{"a": 1, "b": 2, "c": 3}
	perms := &policy.ResolvedPermissions{
		Allowed:      true,
		AllowColumns: []string{"a"},
	}
	_ = filterEventColumns(data, perms)

	// Original data must still have all keys.
	assert.Contains(t, data, "a")
	assert.Contains(t, data, "b")
	assert.Contains(t, data, "c")
}

func TestFilterEventColumns_DenyList(t *testing.T) {
	t.Parallel()
	data := map[string]any{"page": "/home", "secret_col": "xyz"}
	perms := &policy.ResolvedPermissions{
		Allowed:     true,
		DenyColumns: []string{"secret_col"},
	}
	result := filterEventColumns(data, perms)

	assert.Contains(t, result, "page")
	assert.NotContains(t, result, "secret_col")
}
