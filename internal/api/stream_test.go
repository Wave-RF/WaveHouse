package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	h := &StreamHandler{Hub: NewHub()}

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
	h := &StreamHandler{
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
	h := &StreamHandler{
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
	h := &StreamHandler{Hub: NewHub()}

	// Non-EventMessage JSON — should be passed through.
	raw := []byte(`{"custom":"data","value":42}`)
	out := h.applyStreamPolicy(raw, "", nil)
	require.NotNil(t, out)
	assert.JSONEq(t, `{"custom":"data","value":42}`, string(out))
}

func TestSSE_ApplyStreamPolicy_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := &StreamHandler{Hub: NewHub()}

	out := h.applyStreamPolicy([]byte(`not json`), "", nil)
	assert.Nil(t, out)
}

func TestSSE_RejectsMissingOrInvalidTable(t *testing.T) {
	t.Parallel()
	h := &StreamHandler{Hub: NewHub()}

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
			target := "/v1/stream"
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
	h := &StreamHandler{Hub: NewHub()}

	// Use a request context that's already cancelled so the handler exits
	// the live-stream select loop immediately instead of blocking the test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
	w := httptest.NewRecorder()
	h.Handle(w, req)
	// Past the validation gate — header set to text/event-stream, not the
	// 400-path application/json.
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	// X-Accel-Buffering: no disables nginx-class proxy buffering so SSE events
	// flush in real time (the client never sees it — nginx strips it).
	assert.Equal(t, "no", w.Header().Get("X-Accel-Buffering"))
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

func TestSSE_EmitsHeartbeatsWhenIdle(t *testing.T) {
	t.Parallel()
	// A short interval keeps the test fast; production tuning lives in config.
	// One bucket so the (sole) idle connection is pushed on every tick.
	hb := NewHeartbeater(1, 20*time.Millisecond)
	go hb.Run(t.Context())

	h := &StreamHandler{Hub: NewHub(), Heartbeater: hb}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
	w := httptest.NewRecorder()

	// Handle blocks in its select loop until the context is cancelled. Run it in
	// a goroutine, let a few heartbeat intervals elapse on an otherwise-idle
	// stream, then cancel and join before reading the buffer — so the test never
	// touches w concurrently with the handler.
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(w, req)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	body := w.Body.String()
	assert.Contains(t, body, ": connected\n\n", "handshake comment should be sent on open")
	assert.GreaterOrEqual(t, strings.Count(body, ": heartbeat\n\n"), 1,
		"an idle stream should emit at least one heartbeat comment; body=%q", body)
}
