package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSE_ApplyStreamPolicy_NoPolicy(t *testing.T) {
	t.Parallel()
	h := &SSEHandler{Hub: NewHub()}

	evt := ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Data:              map[string]any{"page": "/home", "count": float64(1)},
	}
	raw, _ := json.Marshal(evt)

	out := h.applyStreamPolicy(raw, "viewer", nil)
	require.NotNil(t, out)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	assert.Equal(t, "clicks", got["table_name"])
	data := got["data"].(map[string]any)
	assert.Equal(t, "/home", data["page"])
	assert.Equal(t, float64(1), data["count"])
}

func TestSSE_ApplyStreamPolicy_FiltersColumns(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	h := &SSEHandler{
		Hub:         NewHub(),
		PolicyStore: policy.NewMemoryStore(p),
	}

	evt := ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Data:              map[string]any{"page": "/home", "count": float64(1), "secret": "hidden"},
	}
	raw, _ := json.Marshal(evt)

	out := h.applyStreamPolicy(raw, "viewer", nil)
	require.NotNil(t, out)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	data := got["data"].(map[string]any)
	assert.Equal(t, "/home", data["page"])
	assert.NotContains(t, data, "count")
	assert.NotContains(t, data, "secret")
}

func TestSSE_ApplyStreamPolicy_ForbiddenTable(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"admin": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	h := &SSEHandler{
		Hub:         NewHub(),
		PolicyStore: policy.NewMemoryStore(p),
	}

	evt := ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Data:              map[string]any{"page": "/home"},
	}
	raw, _ := json.Marshal(evt)

	// "viewer" has no access — should return nil (skip).
	out := h.applyStreamPolicy(raw, "viewer", nil)
	assert.Nil(t, out)
}

func TestSSE_ApplyStreamPolicy_NonEventJSON(t *testing.T) {
	t.Parallel()
	h := &SSEHandler{Hub: NewHub()}

	// Non-EventMessage JSON — should be passed through.
	raw := []byte(`{"custom":"data","value":42}`)
	out := h.applyStreamPolicy(raw, "", nil)
	require.NotNil(t, out)
	assert.JSONEq(t, `{"custom":"data","value":42}`, string(out))
}

func TestSSE_ApplyStreamPolicy_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := &SSEHandler{Hub: NewHub()}

	out := h.applyStreamPolicy([]byte(`not json`), "", nil)
	assert.Nil(t, out)
}

// WS handler has same applyStreamPolicy logic — test parity.
func TestWS_ApplyStreamPolicy_FiltersColumns(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"events": {
				Select: map[string]policy.RolePermissions{
					"user": {AllowColumns: []string{"name"}},
				},
			},
		},
	}
	h := &WSHandler{
		Hub:         NewHub(),
		PolicyStore: policy.NewMemoryStore(p),
	}

	evt := ingest.EventMessage{
		TableName:         "events",
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Data:              map[string]any{"name": "click", "internal_id": "abc"},
	}
	raw, _ := json.Marshal(evt)

	out := h.applyStreamPolicy(raw, "user", nil, "events")
	require.NotNil(t, out)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	// WS wraps in table envelope.
	assert.Equal(t, "events", got["table"])
	inner := got["data"].(map[string]any)
	data := inner["data"].(map[string]any)
	assert.Equal(t, "click", data["name"])
	assert.NotContains(t, data, "internal_id")
}

func TestWS_ApplyStreamPolicy_NoPolicy(t *testing.T) {
	t.Parallel()
	h := &WSHandler{Hub: NewHub()}

	evt := ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Data:              map[string]any{"page": "/home"},
	}
	raw, _ := json.Marshal(evt)

	out := h.applyStreamPolicy(raw, "", nil, "clicks")
	require.NotNil(t, out)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	// WS wraps in table envelope.
	assert.Equal(t, "clicks", got["table"])
	inner := got["data"].(map[string]any)
	assert.Equal(t, "clicks", inner["table_name"])
}

// TestSSE_RejectsMissingOrInvalidTable verifies that the SSE handler returns
// 400 when the ?table= parameter is missing or contains characters that could
// be interpreted as NATS wildcards (regression guard for the fix that landed
// alongside the wildcard fan-out removal in #100).
func TestSSE_RejectsMissingOrInvalidTable(t *testing.T) {
	t.Parallel()
	h := &SSEHandler{Hub: NewHub()}

	cases := []struct {
		name    string
		table   string
		errBody string
	}{
		{"missing", "", "missing required query parameter: table"},
		{"empty after url decode", "", "missing required query parameter: table"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := "/v1/stream/sse"
			if tc.table != "" {
				target += "?table=" + url.QueryEscape(tc.table)
			}
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
			w := httptest.NewRecorder()
			h.Handle(w, req)
			testutil.AssertJSONContains(t, w, http.StatusBadRequest, map[string]any{"error": tc.errBody})
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestSSE_AcceptsSafeTableName(t *testing.T) {
	t.Parallel()
	h := &SSEHandler{Hub: NewHub()}

	// Use a request context that's already cancelled so the handler exits
	// the live-stream select loop immediately instead of blocking the test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream/sse?table=clicks", nil)
	w := httptest.NewRecorder()
	h.Handle(w, req)
	// Past the validation gate — header set to text/event-stream, not the
	// 400-path application/json.
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
}

func TestWS_RejectsInvalidTableOnQuery(t *testing.T) {
	t.Parallel()
	h := &WSHandler{Hub: NewHub()}

	cases := []string{""}
	for _, tbl := range cases {
		t.Run(tbl, func(t *testing.T) {
			t.Parallel()
			target := "/v1/stream/ws?table=" + url.QueryEscape(tbl)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
			w := httptest.NewRecorder()
			h.Handle(w, req)
			// Validation runs before websocket.Accept, so we get a plain 400
			// rather than an upgrade-attempt error.
			testutil.AssertJSONContains(t, w, http.StatusBadRequest, map[string]any{"error": "invalid table name"})
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestWS_AcceptsUnsafeTableOnQuery(t *testing.T) {
	t.Parallel()
	h := &WSHandler{Hub: NewHub()}

	cases := []string{">", "*", "ingest.>", "clicks ", "clicks.subpath"}
	for _, tbl := range cases {
		t.Run(tbl, func(t *testing.T) {
			t.Parallel()
			target := "/v1/stream/ws?table=" + url.QueryEscape(tbl)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
			w := httptest.NewRecorder()
			h.Handle(w, req)
			// Validation runs before websocket.Accept, so we get a plain 426 and not a 400
			// (an upgrade-attempt error)
			testutil.AssertBodyContains(t, w, http.StatusUpgradeRequired, "WebSocket protocol violation")
		})
	}
}

func TestExtractEventTimestamp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "valid timestamp",
			data: `{"table_name":"clicks","received_timestamp":"2024-01-01T00:00:00Z","data":{}}`,
			want: "2024-01-01T00:00:00Z",
		},
		{
			name: "nano timestamp",
			data: `{"table_name":"clicks","received_timestamp":"2024-01-01T00:00:00.123456789Z","data":{}}`,
			want: "2024-01-01T00:00:00.123456789Z",
		},
		{
			name: "no timestamp",
			data: `{"custom":"data"}`,
			want: "",
		},
		{
			name: "invalid json",
			data: `not json`,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractEventTimestamp([]byte(tt.data))
			assert.Equal(t, tt.want, got)
		})
	}
}
