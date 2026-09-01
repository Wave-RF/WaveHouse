package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyHandler_Get_NilPolicy(t *testing.T) {
	t.Parallel()
	store := policy.Static(nil)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/policy", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"tables":{}}`, w.Body.String())
}

func TestPolicyHandler_Get_WithPolicy(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page", "count"}},
				},
			},
		},
	}
	store := policy.Static(p)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/policy", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var got policy.Policy
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Contains(t, got.Tables, "clicks")
}
