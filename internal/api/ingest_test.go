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

// checkInStore builds a policy whose insert check restricts org_id to the set
// carried by the token's `orgs` claim (an _in check) — the multi-tenant
// "a writer may only insert rows for tenants they belong to" case (#224).
func checkInStore() *policy.Store {
	orgsTemplate := "{{ jwt.orgs }}"
	return policy.NewMemoryStore(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Insert: map[string]policy.RolePermissions{
					"user": {Check: map[string]policy.Filter{"org_id": {In: &orgsTemplate}}},
				},
			},
		},
	})
}

func TestIngest_Policy_CheckIn_InSet(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = checkInStore()

	// org_id is one of the token's allowed orgs — should pass.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": "org-b"})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{"orgs": []any{"org-a", "org-b"}})
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "an in-set value should publish")
}

func TestIngest_Policy_CheckIn_NotInSet(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = checkInStore()

	// org_id is NOT one of the token's allowed orgs — forging another tenant's row.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": "org-z"})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{"orgs": []any{"org-a", "org-b"}})
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "check failed")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_Policy_CheckIn_Absent_FailsClosed(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = checkInStore()

	// org_id omitted — unlike _eq there's no single value to auto-inject, so the
	// insert is rejected (fail closed) rather than stamped with an arbitrary org.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{"orgs": []any{"org-a", "org-b"}})
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "check failed")
	testutil.AssertJSONErrorResponse(t, w)
}

// TestIngest_Policy_CheckIn_AbsentClaim_FailsClosed locks the typed-nil []any
// path behind an _in check: when the claim itself is absent, resolveInValues
// returns a typed-nil []any, which must still assert as []any in processRecord
// (entering the membership branch) so the column is rejected — never treated as a
// scalar _eq value and auto-injected. The sibling _Absent test omits the column
// with the claim present; this one drops the claim too. Guards #224 fail-closed.
func TestIngest_Policy_CheckIn_AbsentClaim_FailsClosed(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.PolicyStore = checkInStore()

	// The `orgs` claim is absent entirely, so the _in set resolves to a typed-nil
	// []any; org_id is omitted too. The insert must be rejected (fail closed), not
	// auto-injected with nil and published.
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{})
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "check failed")
	assert.Nil(t, pub.LastMessage(), "an absent _in claim must not auto-inject or publish")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_Dedup_MissingIDField(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.IDField = "event_id"

	// Payload omits event_id and require_id is off (the default): the row skips
	// dedup and is still published — the warn+counter path, not a rejection (#219).
	req := ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "should have published even without dedup ID")
}

// TestIngest_Dedup_RequireID_Rejects covers strict mode: a single insert lacking
// the id is a 400 that publishes nothing, while one carrying the id still passes.
func TestIngest_Dedup_RequireID_Rejects(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = testutil.NewMockDeduplicator()
	h.IDField = "event_id"
	h.RequireID = true

	w := httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/home"}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing dedupe id field")
	testutil.AssertJSONErrorResponse(t, w)
	assert.Nil(t, pub.LastMessage(), "must not publish a row missing the dedupe id under require_id")

	w = httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/home", "event_id": "ok-1"}))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "a record carrying the id is still accepted")
}

// TestIngest_NDJSON_RequireID_Rejects confirms strict mode is per-record: the
// missing-id line fails while the rest of the batch is published.
func TestIngest_NDJSON_RequireID_Rejects(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
	h.Dedup = testutil.NewMockDeduplicator()
	h.IDField = "event_id"
	h.RequireID = true

	req := ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/a", "event_id": "e1"}),
		jsonLine(t, map[string]any{"page": "/b"}), // missing id → rejected
		jsonLine(t, map[string]any{"page": "/c", "event_id": "e2"}),
	)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	assert.Equal(t, 0, resp.Duplicates)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Contains(t, resultAt(t, resp, 2).Error, "missing dedupe id field")
	assert.True(t, resultAt(t, resp, 3).Ok)
	assert.Len(t, pub.Messages, 2)
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

func decodeBatchResult(t *testing.T, w *httptest.ResponseRecorder) batchResult {
	t.Helper()
	var resp batchResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

// resultAt returns the per-record result with the given 1-based index, failing
// the test if it is absent (e.g. truncated away).
func resultAt(t *testing.T, resp batchResult, index int) recordResult {
	t.Helper()
	for _, r := range resp.Results {
		if r.Index == index {
			return r
		}
	}
	t.Fatalf("no result with index %d in %+v", index, resp.Results)
	return recordResult{}
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 3, resp.Succeeded)
	assert.Equal(t, 0, resp.Failed)
	assert.Equal(t, 0, resp.Duplicates)
	require.Len(t, resp.Results, 3)
	for i, rr := range resp.Results {
		assert.Equal(t, i+1, rr.Index)
		assert.True(t, rr.Ok)
		assert.Empty(t, rr.Error)
	}
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Results, 3)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.NotEmpty(t, resultAt(t, resp, 2).Error)
	assert.True(t, resultAt(t, resp, 3).Ok)
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Results, 3)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Contains(t, resultAt(t, resp, 2).Error, "invalid json")
	assert.True(t, resultAt(t, resp, 3).Ok)
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
	resp := decodeBatchResult(t, w)
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Duplicates)
	assert.Equal(t, 0, resp.Failed)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.True(t, resultAt(t, resp, 2).Duplicate)
	assert.True(t, resultAt(t, resp, 3).Ok)
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Results, 2)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Contains(t, resultAt(t, resp, 2).Error, "not allowed for insert")
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	require.Len(t, resp.Results, 3)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Contains(t, resultAt(t, resp, 2).Error, "check failed")
	assert.True(t, resultAt(t, resp, 3).Ok)
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
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
}

func TestIngest_NDJSON_ErrorsTruncated(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	const total = maxReportedResults + 50
	lines := make([]string, total)
	for i := range lines {
		lines[i] = "{ not valid json"
	}
	req := ndjsonRequest(t, "clicks", lines...)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, total, resp.Total)
	assert.Equal(t, total, resp.Failed)
	// Failed counts everything; the echoed per-record results are capped.
	assert.Len(t, resp.Results, maxReportedResults)
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

// ── Forgiving multi-format ingest (JSON array, sniffing, body cap) ──────────

// rawIngestRequest builds a POST /v1/ingest request with a verbatim body and an
// optional Content-Type (empty string → no header), so tests can exercise the
// body sniffer and malformed-input paths directly.
func rawIngestRequest(t *testing.T, table, contentType, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/v1/ingest?table="+url.QueryEscape(table),
		strings.NewReader(body),
	)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req
}

func TestIngest_JSONArray_AllValid(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// A bare JSON array with no Content-Type must be accepted as a batch.
	req := ingestRequest(t, "clicks", []map[string]any{
		{"page": "/a", "count": 1},
		{"page": "/b", "count": 2},
	})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 0, resp.Failed)
	require.Len(t, resp.Results, 2)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.True(t, resultAt(t, resp, 2).Ok)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_JSONArray_SingleElement(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// A one-element array is still a batch (returns the results envelope, not
	// the single-object {"ok":true}).
	req := ingestRequest(t, "clicks", []map[string]any{{"page": "/solo"}})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 1, resp.Succeeded)
	require.Len(t, resp.Results, 1)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.Len(t, pub.Messages, 1)
}

func TestIngest_JSONArray_WithJSONContentType(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// Content-Type: application/json must NOT force the single-object path when
	// the body is an array — the sniffer wins.
	req := rawIngestRequest(t, "clicks", "application/json", `[{"page":"/a"},{"page":"/b"}]`)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_JSONArray_PartialValidationFailure(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "clicks", []map[string]any{
		{"page": "/a"},
		{"page": "/b", "nonexistent_field": 42}, // unknown column → rejected
		{"page": "/c"},
	})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	// The bad element is reported per-record; the request itself is 200.
	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 1, resp.Failed)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.NotEmpty(t, resultAt(t, resp, 2).Error)
	assert.True(t, resultAt(t, resp, 3).Ok)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_JSONArray_ScalarElements(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// Non-object elements (number, string, nested array) are wrong-typed: the
	// decoder stays in sync, so each is a per-record error and the objects
	// around them still ingest.
	req := ingestRequest(t, "clicks", []any{
		map[string]any{"page": "/a"},
		5,
		"x",
		[]any{1, 2},
		map[string]any{"page": "/b"},
	})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 5, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Equal(t, 3, resp.Failed)
	assert.True(t, resultAt(t, resp, 1).Ok)
	assert.NotEmpty(t, resultAt(t, resp, 2).Error)
	assert.NotEmpty(t, resultAt(t, resp, 3).Error)
	assert.NotEmpty(t, resultAt(t, resp, 4).Error)
	assert.True(t, resultAt(t, resp, 5).Ok)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_JSONArray_SyntaxError_Fatal(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// A structural syntax error desyncs the decoder — the whole request fails
	// (400), unlike a per-element type error. The leading good element may have
	// already published (at-least-once on retry).
	req := rawIngestRequest(t, "clicks", "application/json", `[{"page":"/a"}, {bad]`)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	testutil.AssertJSONErrorResponse(t, w)
	assert.Len(t, pub.Messages, 1) // the leading record published before the abort
}

func TestIngest_JSONArray_Truncated_Fatal(t *testing.T) {
	t.Parallel()

	// A JSON array body cut off before its closing ']' (connection drop, partial
	// write) must fail the whole request — never report the records that did
	// arrive as a complete, successful 200 batch.
	tests := []struct {
		name string
		body string
	}{
		{name: "missing closing bracket", body: `[{"page":"/a"}`},
		{name: "trailing comma cut off", body: `[{"page":"/a"},`},
		{name: "bare open bracket", body: `[`},
		{name: "truncated mid element", body: `[{"page":"/a"},{"pa`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

			req := rawIngestRequest(t, "clicks", "application/json", tt.body)
			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "invalid json")
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestIngest_JSONArray_Empty(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// An explicit empty array is a valid, record-less batch → 200 with no rows.
	req := rawIngestRequest(t, "clicks", "application/json", `[]`)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 0, resp.Total)
	assert.Empty(t, resp.Results)
	assert.Empty(t, pub.Messages)
}

func TestIngest_Sniff_ArrayBodyBeatsNDJSONHeader(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// Body starts with '[' → array path, even though the header says NDJSON.
	req := rawIngestRequest(t, "clicks", "application/x-ndjson", `[{"page":"/a"},{"page":"/b"}]`)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, 2, resp.Succeeded)
	assert.Len(t, pub.Messages, 2)
}

func TestIngest_SingleObject_PrettyPrinted(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// A multi-line (pretty-printed) single object must not be mistaken for
	// NDJSON — it's one record on the single-object path.
	req := rawIngestRequest(t, "clicks", "application/json", "{\n  \"page\": \"/a\",\n  \"count\": 1\n}")
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]bool
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp["ok"])
	assert.Len(t, pub.Messages, 1)
}

func TestIngest_Unlabeled_ConcatenatedObjects_FirstOnly(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

	// Two concatenated objects with no NDJSON header take the single-object
	// path and ingest only the first (matching the historical behavior — send
	// application/x-ndjson to batch them).
	req := rawIngestRequest(t, "clicks", "", `{"page":"/first"}{"page":"/second"}`)
	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]bool
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp["ok"])
	require.Len(t, pub.Messages, 1)
	assert.Contains(t, string(pub.Messages[0].Data), "/first")
	assert.NotContains(t, string(pub.Messages[0].Data), "/second")
}

func TestIngest_LeadingWhitespace_Sniff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		body  string
		batch bool
	}{
		{name: "array after whitespace", body: "  \n\t [{\"page\":\"/a\"}] ", batch: true},
		{name: "object after whitespace", body: "  \n {\"page\":\"/a\"}", batch: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

			req := rawIngestRequest(t, "clicks", "application/json", tt.body)
			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			if tt.batch {
				resp := decodeBatchResult(t, w)
				assert.Equal(t, 1, resp.Total)
			} else {
				var resp map[string]bool
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
				assert.True(t, resp["ok"])
			}
			assert.Len(t, pub.Messages, 1)
		})
	}
}

func TestIngest_EmptyBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		wantMsg     string
	}{
		{name: "no content type", contentType: "", body: "", wantMsg: "empty body"},
		{name: "whitespace only", contentType: "application/json", body: "   \n\t ", wantMsg: "empty body"},
		{name: "ndjson empty", contentType: "application/x-ndjson", body: "", wantMsg: "empty ndjson body"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())

			req := rawIngestRequest(t, "clicks", tt.contentType, tt.body)
			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantMsg)
			testutil.AssertJSONErrorResponse(t, w)
			assert.Empty(t, pub.Messages)
		})
	}
}

func TestIngest_BodyCap_413(t *testing.T) {
	t.Parallel()

	const elem = `{"page":"/a"}`
	big := `{"page":"/` + strings.Repeat("a", 200) + `"}`
	tests := []struct {
		name string
		ct   string
		body string
		cap  int64
	}{
		{name: "single object", ct: "application/json", body: big, cap: 50},
		{name: "json array mid-element", ct: "application/json", body: "[" + big + "]", cap: 50},
		// Cap lands exactly between elements (just past '[' + the first element,
		// before the comma) so the overflow surfaces at arrayReader's
		// More()/Token boundary — the path that used to swallow the read error
		// and silently truncate to a partial 200 instead of 413.
		{name: "json array between elements", ct: "application/json", body: "[" + elem + "," + elem + "]", cap: int64(1 + len(elem))},
		// NDJSON over cap surfaces via the scanner's Err() (MaxBytesError).
		{name: "ndjson", ct: "application/x-ndjson", body: big, cap: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(), pub, testutil.NopLogger())
			h.maxRequestBytes = tt.cap // below the body

			req := rawIngestRequest(t, "clicks", tt.ct, tt.body)
			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
			assert.Contains(t, w.Body.String(), "request body exceeded")
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

// tsRegistry returns a registry whose events table carries the timestamp column
// shapes the #372 canonicalization path rewrites.
func tsRegistry() *discovery.SchemaRegistry {
	return discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{
			Name: "events",
			Columns: []discovery.Column{
				{Name: "name", Type: "String"},
				{Name: "ts", Type: "DateTime('UTC')", HasDefault: true},
				{Name: "ts_ms", Type: "DateTime64(3, 'UTC')", HasDefault: true},
			},
		},
	})
}

// publishedData decodes the inner data object of the last published envelope —
// what the stream fans out and the worker inserts.
func publishedData(t *testing.T, pub *testutil.MockPublisher) map[string]any {
	t.Helper()
	msg := pub.LastMessage()
	require.NotNil(t, msg)
	var evt struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(msg.Data, &evt))
	return evt.Data
}

// TestIngest_TimestampsCanonicalized is the #372 contract: whatever spelling a
// producer uses, the published payload — the one copy SSE subscribers, the
// ClickHouse insert, and the DLQ all consume — carries RFC 3339 UTC.
func TestIngest_TimestampsCanonicalized(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(tsRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "events", map[string]any{
		"name":  "e",
		"ts":    "2026-06-21 04:00:00", // zone-less ClickHouse-native form
		"ts_ms": 1782014400.5,          // Unix seconds
	})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	data := publishedData(t, pub)
	assert.Equal(t, "2026-06-21T04:00:00Z", data["ts"])
	assert.Equal(t, "2026-06-21T04:00:00.5Z", data["ts_ms"])
	assert.Equal(t, "e", data["name"], "non-timestamp columns untouched")
}

// TestIngest_TimestampGarbage_PassesThrough: fail-open — an unparseable value
// publishes verbatim; ClickHouse's own parser decides insertability (#372/#381).
func TestIngest_TimestampGarbage_PassesThrough(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(tsRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "events", map[string]any{"name": "e", "ts": "banana"})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "banana", publishedData(t, pub)["ts"], "unparseable value published verbatim")
}

// TestIngest_Batch_MixedTimestampSpellings: parseable spellings canonicalize,
// the unparseable one passes through — no record fails on its timestamp.
func TestIngest_Batch_MixedTimestampSpellings(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(tsRegistry(), pub, testutil.NopLogger())

	req := ingestRequest(t, "events", []map[string]any{
		{"name": "a", "ts": "2026-06-21T04:00:00Z"},
		{"name": "b", "ts": "banana"},
		{"name": "c", "ts": float64(1782014400)},
	})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var result struct {
		Total     int `json:"total"`
		Succeeded int `json:"succeeded"`
		Failed    int `json:"failed"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	assert.Equal(t, 3, result.Total)
	assert.Equal(t, 3, result.Succeeded)
	assert.Equal(t, 0, result.Failed)
	require.Len(t, pub.Messages, 3, "every record published")

	var spellings []string
	for _, msg := range pub.Messages {
		var evt struct {
			Data map[string]any `json:"data"`
		}
		require.NoError(t, json.Unmarshal(msg.Data, &evt))
		spellings = append(spellings, evt.Data["ts"].(string))
	}
	assert.Equal(t, []string{
		"2026-06-21T04:00:00Z", // already canonical
		"banana",               // unparseable — passed through verbatim
		"2026-06-21T04:00:00Z", // Unix seconds — canonicalized
	}, spellings)
}
