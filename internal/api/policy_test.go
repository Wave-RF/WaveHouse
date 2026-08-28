package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
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

func TestPolicyHandler_Validate_Valid(t *testing.T) {
	t.Parallel()
	store := policy.Static(nil)
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
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ops/policy/validate", bytes.NewReader(body))
	h.Validate(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"valid":true}`, w.Body.String())
}

func TestPolicyHandler_Validate_InvalidJSON(t *testing.T) {
	t.Parallel()
	store := policy.Static(nil)
	h := NewPolicyHandler(store)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ops/policy/validate", bytes.NewReader([]byte(`not json`)))
	h.Validate(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	testutil.AssertJSONErrorResponse(t, w)
}

// TestPolicyHandler_RequestBodyCap pins the control-plane body cap on the admin
// policy decoder (#315) — Validate. An oversized body returns
// 413, not 400 "invalid json". maxRequestBytes is tiny so we don't allocate
// 1 MiB per run.
func TestPolicyHandler_RequestBodyCap(t *testing.T) {
	t.Parallel()

	// A valid policy whose JSON comfortably exceeds the tiny test cap.
	p := policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {Select: map[string]policy.RolePermissions{
				"viewer": {AllowColumns: []string{"page", "button", "count"}},
			}},
		},
	}
	body, err := json.Marshal(p)
	require.NoError(t, err)

	const testCap = 16
	require.Greater(t, len(body), testCap, "test body must exceed the cap")

	tests := []struct {
		name   string
		method string
		path   string
		call   func(*PolicyHandler, http.ResponseWriter, *http.Request)
	}{
		{"validate", http.MethodPost, "/v1/ops/policy/validate", (*PolicyHandler).Validate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := NewPolicyHandler(policy.Static(nil))
			h.maxRequestBytes = testCap
			w := httptest.NewRecorder()
			r := httptest.NewRequestWithContext(context.Background(), tt.method, tt.path, bytes.NewReader(body))
			tt.call(h, w, r)

			assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code, "oversized body must 413")
			assert.Contains(t, w.Body.String(), "request body exceeded")
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestPolicyHandler_Validate_InvalidPolicy(t *testing.T) {
	t.Parallel()
	store := policy.Static(nil)
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
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ops/policy/validate", bytes.NewReader(body))
	h.Validate(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "max_rows")
	testutil.AssertJSONErrorResponse(t, w)
}
