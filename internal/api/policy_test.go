package api

import (
	"bytes"
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
	store := policy.NewMemoryStore(nil)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/policy", nil)
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
	store := policy.NewMemoryStore(p)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/policy", nil)
	h.Get(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	var got policy.Policy
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Contains(t, got.Tables, "clicks")
}

func TestPolicyHandler_Validate_Valid(t *testing.T) {
	t.Parallel()
	store := policy.NewMemoryStore(nil)
	h := NewPolicyHandler(store)

	p := policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	body, _ := json.Marshal(p)
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/policy/validate", bytes.NewReader(body))
	h.Validate(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"valid":true}`, w.Body.String())
}

func TestPolicyHandler_Validate_InvalidJSON(t *testing.T) {
	t.Parallel()
	store := policy.NewMemoryStore(nil)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/policy/validate", bytes.NewReader([]byte(`not json`)))
	h.Validate(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
}

func TestPolicyHandler_Put_InvalidJSON(t *testing.T) {
	t.Parallel()
	store := policy.NewMemoryStore(nil)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/v1/policy", bytes.NewReader([]byte(`{bad}`)))
	h.Put(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
}

func TestPolicyHandler_Validate_InvalidPolicy(t *testing.T) {
	t.Parallel()
	store := policy.NewMemoryStore(nil)
	h := NewPolicyHandler(store)

	p := policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {MaxRows: -1},
				},
			},
		},
	}
	body, _ := json.Marshal(p)
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/policy/validate", bytes.NewReader(body))
	h.Validate(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "max_rows")
	assertJSONErrorResponse(t, w)
}
