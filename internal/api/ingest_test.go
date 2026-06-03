package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRegistry() *discovery.SchemaRegistry {
	return discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{
			Name: "clicks",
			Columns: []discovery.Column{
				{Name: "page", Type: "String"},
				{Name: "button", Type: "String", HasDefault: true},
				{Name: "count", Type: "UInt64", HasDefault: true},
				{Name: "event_id", Type: "String", HasDefault: true},
				{Name: "org_id", Type: "String", HasDefault: true},
			},
		},
	})
}

func ingestRequest(t *testing.T, table string, body any) *http.Request {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)

	return httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ingest?table="+url.QueryEscape(table), bytes.NewReader(data))
}

func TestIngest_ValidPayload(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "count": 1})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]bool
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp["ok"])

	msg := pub.LastMessage()
	require.NotNil(t, msg)
	assert.Equal(t, "ingest.clicks", msg.Subject)
}

func TestIngest_MissingTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{
			name: "no query string at all",
			url:  "/v1/ingest",
		},
		{
			name: "trailing slash without query",
			url:  "/v1/ingest/",
		},
		{
			name: "empty query symbol",
			url:  "/v1/ingest?",
		},
		{
			name: "table parameter provided but empty",
			url:  "/v1/ingest?table=",
		},
		{
			name: "completely wrong query parameter",
			url:  "/v1/ingest?not_the_right_param=clicks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

			req := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				tt.url,
				bytes.NewReader([]byte(`{}`)),
			)

			w := httptest.NewRecorder()
			h.Handle(w, req)

			// Assertions remain identical for all error cases
			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "missing table")
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestIngest_UnknownTable(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "nonexistent", map[string]any{"x": 1})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "unknown table")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_InvalidJSON(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ingest?table=clicks", bytes.NewReader([]byte("not json")))

	w := httptest.NewRecorder()
	h.Handle(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_SchemaValidation_UnknownField(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "nonexistent_field": 42})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "error")
}

func TestIngest_Dedup_FirstTime(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.IDField = "event_id"

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "event_id": "evt-1"})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "should have published")
}

func TestIngest_Dedup_Duplicate(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.IDField = "event_id"

	// First call.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "event_id": "dup-1"})
	w := httptest.NewRecorder()
	h.Handle(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Second call — duplicate.
	req = ingestRequest(t, "clicks", map[string]any{"page": "/home", "event_id": "dup-1"})
	w = httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]bool
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp["duplicate"])
	// Publisher should only have 1 message (first call).
	assert.Len(t, pub.Messages, 1)
}

func TestIngest_PublishError_503(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{Err: errors.New("maximum bytes exceeded")}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "30", w.Header().Get("Retry-After"))
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_PublishError_500(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{Err: errors.New("some other error")}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "publish failed")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_Policy_Forbidden(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {},
				},
				// No insert permissions for viewer.
			},
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	ctx := auth.WithRole(req.Context(), "viewer")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "forbidden")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_Policy_ColumnDenied(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"writer": {
						AllowColumns: []string{"page"},
					},
				},
			},
		},
	})

	// Try to insert 'button' which is not in AllowColumns.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "button": "signup"})
	ctx := auth.WithRole(req.Context(), "writer")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "not allowed for insert")
}

func TestIngest_Policy_CheckClause_Mismatch(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"user": {
						Check: map[string]policy.Filter{
							"org_id": {Eq: &orgTemplate},
						},
					},
				},
			},
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": "wrong-org"})
	claims := jwt.MapClaims{"org_id": "correct-org"}
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, claims)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "check failed")
}

func TestIngest_Policy_CheckClause_Match(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"user": {
						Check: map[string]policy.Filter{
							"org_id": {Eq: &orgTemplate},
						},
					},
				},
			},
		},
	})

	// org_id in body matches JWT claim — should pass.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": "my-org"})
	claims := jwt.MapClaims{"org_id": "my-org"}
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, claims)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "should have published")
}

func TestIngest_Policy_CheckClause_AutoInject(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"user": {
						Check: map[string]policy.Filter{
							"org_id": {Eq: &orgTemplate},
						},
					},
				},
			},
		},
	})

	// org_id NOT in body — should be auto-injected from JWT claim.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	claims := jwt.MapClaims{"org_id": "injected-org"}
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, claims)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// Verify the published message has org_id injected.
	msg := pub.LastMessage()
	require.NotNil(t, msg)
	assert.Contains(t, string(msg.Data), "injected-org")
}

func TestIngest_Dedup_MissingIDField(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.IDField = "event_id"

	// Payload does NOT include event_id — should skip dedup and publish.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "should have published even without dedup ID")
}

func TestIngest_Policy_DenyColumns(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"writer": {
						DenyColumns: []string{"count"},
					},
				},
			},
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "count": 42})
	ctx := auth.WithRole(req.Context(), "writer")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "not allowed for insert")
}

func TestIngest_AdminRole_NoPolicy(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {},
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	ctx := auth.WithRole(req.Context(), "admin")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// ── NDJSON batch ingest ──────────────────────────────────────────

// ndjsonRequest builds a POST /v1/ingest request with an NDJSON body. Lines are
// joined with "\n" verbatim, so callers can pass blank or malformed lines.
func ndjsonRequest(t *testing.T, table string, lines ...string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/v1/ingest?table="+url.QueryEscape(table),
		strings.NewReader(strings.Join(lines, "\n")),
	)
	req.Header.Set("Content-Type", "application/x-ndjson")
	return req
}

// jsonLine marshals obj to a compact single-line JSON string for NDJSON bodies.
func jsonLine(t *testing.T, obj any) string {
	t.Helper()
	b, err := json.Marshal(obj)
	require.NoError(t, err)
	return string(b)
}

func decodeNDJSONResult(t *testing.T, w *httptest.ResponseRecorder) ndjsonResult {
	t.Helper()
	var resp ndjsonResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

func TestIngest_NDJSON_AllValid(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a", "count": 1}),
		jsonLine(t, map[string]any{"page": "/b", "count": 2}),
		jsonLine(t, map[string]any{"page": "/c", "count": 3}),
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 3, resp.Succeeded)
	assert.Equal(t, 0, resp.Failed)
	assert.Equal(t, 0, resp.Duplicates)
	assert.Empty(t, resp.Errors)
	assert.Len(t, pub.Messages, 3)
}

func TestIngest_NDJSON_PartialFailure_Validation(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a"}),
		jsonLine(t, map[string]any{"page": "/b", "nonexistent_field": 42}), // unknown column
		jsonLine(t, map[string]any{"page": "/c"}),
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Errors, 1)
	assert.Equal(t, 2, resp.Errors[0].Line)
	// The two good rows are still published; the bad one is not.
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_NDJSON_MalformedLine(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a"}),
		"{ this is not valid json",
		jsonLine(t, map[string]any{"page": "/c"}),
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Errors, 1)
	assert.Equal(t, 2, resp.Errors[0].Line)
	assert.Contains(t, resp.Errors[0].Error, "invalid json")
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_NDJSON_BlankLinesSkipped(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// Leading, interior, and whitespace-only lines are all skipped; only real
	// records are counted.
	req := ndjsonRequest(t, "clicks",
		"",
		jsonLine(t, map[string]any{"page": "/a"}),
		"   ",
		"",
		jsonLine(t, map[string]any{"page": "/b"}),
		"\t",
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_NDJSON_EmptyBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		lines []string
	}{
		{name: "no lines", lines: []string{""}},
		{name: "only whitespace", lines: []string{"   ", "\t", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

			req := ndjsonRequest(t, "clicks", tt.lines...)
			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "empty ndjson body")
			testutil.AssertJSONErrorResponse(t, w)
			assert.Empty(t, pub.Messages)
		})
	}
}

func TestIngest_NDJSON_Dedup(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.IDField = "event_id"

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a", "event_id": "e1"}),
		jsonLine(t, map[string]any{"page": "/b", "event_id": "e1"}), // duplicate of e1
		jsonLine(t, map[string]any{"page": "/c", "event_id": "e2"}),
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Duplicates)
	assert.Equal(t, 0, resp.Failed)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_NDJSON_Backpressure_503(t *testing.T) {
	t.Parallel()
	// Publisher rejects every publish with the backpressure sentinel; the first
	// valid record aborts the whole batch with 503 + Retry-After.
	pub := &testutil.MockPublisher{Err: errors.New("maximum bytes exceeded")}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a"}),
		jsonLine(t, map[string]any{"page": "/b"}),
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "30", w.Header().Get("Retry-After"))
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_NDJSON_PublishError_500(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{Err: errors.New("some other error")}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ndjsonRequest(t, "clicks", jsonLine(t, map[string]any{"page": "/a"}))
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "publish failed")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_NDJSON_Policy_ColumnDenied_PerLine(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"writer": {AllowColumns: []string{"page"}},
				},
			},
		},
	})

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a"}),                  // allowed
		jsonLine(t, map[string]any{"page": "/b", "button": "buy"}), // button not allowed
	)
	ctx := auth.WithRole(req.Context(), "writer")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Errors, 1)
	assert.Equal(t, 2, resp.Errors[0].Line)
	assert.Contains(t, resp.Errors[0].Error, "not allowed for insert")
	assert.Len(t, pub.Messages, 1)
}

func TestIngest_NDJSON_Policy_TableForbidden(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{"viewer": {}},
				// No insert permission for viewer.
			},
		},
	})

	req := ndjsonRequest(t, "clicks", jsonLine(t, map[string]any{"page": "/a"}))
	ctx := auth.WithRole(req.Context(), "viewer")
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	// Table-level denial happens before any record is read — whole-request 403.
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "forbidden")
	testutil.AssertJSONErrorResponse(t, w)
	assert.Empty(t, pub.Messages)
}

func TestIngest_NDJSON_Policy_CheckClause_PerLineAndAutoInject(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicyStore = policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"user": {Check: map[string]policy.Filter{"org_id": {Eq: &orgTemplate}}},
				},
			},
		},
	})

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a", "org_id": "my-org"}),    // matches claim
		jsonLine(t, map[string]any{"page": "/b", "org_id": "wrong-org"}), // mismatch → rejected
		jsonLine(t, map[string]any{"page": "/c"}),                        // org_id auto-injected
	)
	claims := jwt.MapClaims{"org_id": "my-org"}
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, claims)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Errors, 1)
	assert.Equal(t, 2, resp.Errors[0].Line)
	assert.Contains(t, resp.Errors[0].Error, "check failed")
	require.Len(t, pub.Messages, 2)
	// The auto-injected record (line 3) carries the claim-derived org_id.
	assert.Contains(t, string(pub.Messages[1].Data), "my-org")
}

func TestIngest_NDJSON_ContentTypeWithCharset(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ndjsonRequest(t, "clicks", jsonLine(t, map[string]any{"page": "/a"}))
	req.Header.Set("Content-Type", "application/x-ndjson; charset=utf-8")

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
}

func TestIngest_NDJSON_ErrorsTruncated(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	const total = maxReportedNDJSONErrors + 50
	lines := make([]string, total)
	for i := range lines {
		lines[i] = "{ not valid json"
	}
	req := ndjsonRequest(t, "clicks", lines...)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeNDJSONResult(t, w)
	assert.Equal(t, total, resp.Total)
	assert.Equal(t, total, resp.Failed)
	// Failed counts everything; the echoed errors are capped.
	assert.Len(t, resp.Errors, maxReportedNDJSONErrors)
	assert.Empty(t, pub.Messages)
}

func TestIsNDJSONContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ct   string
		want bool
	}{
		{"application/x-ndjson", true},
		{"application/x-ndjson; charset=utf-8", true},
		{"application/ndjson", true},
		{"application/jsonl", true},
		{"application/jsonlines", true},
		{"application/json", false},
		{"application/json; charset=utf-8", false},
		{"text/plain", false},
		{"", false},
		{"???not-a-media-type", false},
	}
	for _, tt := range tests {
		t.Run(tt.ct, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isNDJSONContentType(tt.ct))
		})
	}
}
