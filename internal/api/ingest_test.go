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
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRegistry(t testing.TB) *discovery.SchemaRegistry {
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
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

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ingest?table="+url.QueryEscape(table), bytes.NewReader(data))
	// Ingest requires a declared format: an undeclared Content-Type is a 415.
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestIngest_ValidPayload(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

	r := rawIngestRequest(t, "clicks", "application/json", "not json")

	w := httptest.NewRecorder()
	h.Handle(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_SchemaValidation_UnknownField(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", false }

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", false }

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"viewer": {Select: &policy.SelectPermissions{}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"writer": {Insert: &policy.InsertPermissions{AllowColumns: []string{"page"}}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
					"org_id": {Eq: &orgTemplate},
				}}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
					"org_id": {Eq: &orgTemplate},
				}}},
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

// TestIngest_Policy_CheckClause_NumericSpellingMatch: the check comparison is
// canonical on both sides (policy.CanonicalScalar), so a numeric claim and a
// numeric insert value match by value even when their JSON spellings differ —
// a claim spelled 1.0 accepts an inserted 1. The former string-form comparison
// ("1.0" != "1") rejected exactly this insert.
func TestIngest_Policy_CheckClause_NumericSpellingMatch(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	countTemplate := "{{ jwt.max_count }}"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
					"count": {Eq: &countTemplate},
				}}},
			},
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "count": 1})
	claims := jwt.MapClaims{"max_count": json.Number("1.0")}
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, claims)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, pub.LastMessage(), "should have published")
}

// TestIngest_Policy_CheckClause_StaticNumericSpelling: a check value with no
// placeholder carries no JSON type, so the comparison accepts either reading
// of it — a static `_eq: "1.0"` accepts an inserted number 1 (its canonical
// numeric reading) and an inserted string "1.0" (its spelling, the pre-PR
// behavior) alike. Without the numeric reading, the payload side is canonical
// ("1") while the static side keeps its raw spelling ("1.0") and the check
// rejects every numeric insert it was written to allow.
func TestIngest_Policy_CheckClause_StaticNumericSpelling(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		body any
	}{
		{"numeric reading", 1},
		{"literal spelling", "1.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
			staticCount := "1.0"
			h.PolicySource = policy.Static(&policy.Policy{
				Tables: map[string]policy.TablePolicy{
					"clicks": {
						"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
							"count": {Eq: &staticCount},
						}}},
					},
				},
			})

			req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "count": tt.body})
			ctx := auth.WithRole(req.Context(), "user")
			ctx = auth.WithClaims(ctx, jwt.MapClaims{})
			req = req.WithContext(ctx)

			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.NotNil(t, pub.LastMessage(), "should have published")
		})
	}
}

// TestIngest_Policy_CheckClause_StringClaimStrictEquality: the numeric
// reading is reserved for placeholder-free literals (policy.LiteralValue). A
// claim-derived required value keeps strict canonical equality, so a writer
// whose claim is the STRING "1e3" cannot insert the number 1000 — its id is
// the three-character text, and accepting the numeric reading would let it
// store a row under the tenant whose String id is "1000".
func TestIngest_Policy_CheckClause_StringClaimStrictEquality(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		body any
		want int
	}{
		{"numeric reading rejected", 1000, http.StatusForbidden},
		{"exact spelling accepted", "1e3", http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
			orgTemplate := "{{ jwt.org_id }}"
			h.PolicySource = policy.Static(&policy.Policy{
				Tables: map[string]policy.TablePolicy{
					"clicks": {
						"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
							"org_id": {Eq: &orgTemplate},
						}}},
					},
				},
			})

			req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": tt.body})
			ctx := auth.WithRole(req.Context(), "user")
			ctx = auth.WithClaims(ctx, jwt.MapClaims{"org_id": "1e3"})
			req = req.WithContext(ctx)

			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, tt.want, w.Code)
		})
	}
}

// TestIngest_Policy_CheckClause_NullValue_FailsClosed: the _eq twin of the
// _in null test. With the claim absent the required value is "", and
// CanonicalScalar(nil) also renders "" — only the hasForm guard separates
// them, so without it an explicit null in the payload would satisfy the check
// (pre-canonicalization, fmt.Sprint(nil) gave "<nil>" and this was impossible).
func TestIngest_Policy_CheckClause_NullValue_FailsClosed(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
					"org_id": {Eq: &orgTemplate},
				}}},
			},
		},
	})

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": nil})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{})
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "check failed")
	assert.Nil(t, pub.LastMessage(), "a null payload value must not satisfy an unresolvable check")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestIngest_Policy_CheckClause_AutoInject(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
					"org_id": {Eq: &orgTemplate},
				}}},
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
func checkInStore() policy.Source {
	orgsTemplate := "{{ jwt.orgs }}"
	return policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{"org_id": {In: &orgsTemplate}}}},
			},
		},
	})
}

func TestIngest_Policy_CheckIn_InSet(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = checkInStore()

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = checkInStore()

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

// TestIngest_Policy_CheckIn_NullValue_FailsClosed: an explicit null passes
// schema validation for a defaulted column, but it has no canonical scalar
// form — so it is a member of NO set, even one carrying the empty string ""
// (the regression shape: an unguarded canonicalization would render null as
// "" and match an empty-string member).
func TestIngest_Policy_CheckIn_NullValue_FailsClosed(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = checkInStore()

	req := ingestRequest(t, "clicks", map[string]any{"page": "/home", "org_id": nil})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{"orgs": []any{"org-a", ""}})
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = checkInStore()

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = checkInStore()

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", false }

	// Payload omits event_id and require_id is off: the row skips
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = testutil.NewMockDeduplicator()
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", true }

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = testutil.NewMockDeduplicator()
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", true }

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"writer": {Insert: &policy.InsertPermissions{DenyColumns: []string{"count"}}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", false }

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"writer": {Insert: &policy.InsertPermissions{AllowColumns: []string{"page"}}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"viewer": {Select: &policy.SelectPermissions{}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	orgTemplate := "{{ jwt.org_id }}"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{"org_id": {Eq: &orgTemplate}}}},
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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

// wantAcceptedTypes is the accepted-media-type list as the 415 body renders it.
// Deliberately a literal and not strings.Join(supportedContentTypes, ", "): the
// point is to pin the advertised list against the docs, and an expectation
// derived from the slice under test cannot do that.
//
// api.md's two 415 rows quote only a two-element prefix of this, so a change
// past the second entry leaves them correct — but removing or reordering
// `application/json` or `application/x-ndjson` does not. The prose sites that
// spell the whole list out, and always need editing: api.md's body/Content-Type
// table, architecture.md's "the four NDJSON spellings" count, and the ingest
// entry in CHANGELOG.md.
const wantAcceptedTypes = "application/json, application/x-ndjson, application/ndjson, application/jsonl, application/jsonlines"

// TestAcceptedTypesAreAllResolvable pins that the advertised list never grows
// beyond what the resolver accepts — an entry added to acceptedContentTypes but
// unreachable in ingestFormatOne would otherwise leave every test green.
//
// The opposite direction, accepting a type nothing advertises, is NOT pinned by
// a test and cannot be: the complement is unbounded. It is closed structurally
// instead — supportedContentTypes is derived from acceptedContentTypes, so
// adding a type to the resolver necessarily advertises it.
func TestAcceptedTypesAreAllResolvable(t *testing.T) {
	t.Parallel()
	for _, ct := range supportedContentTypes {
		_, err := resolveContentType([]string{ct})
		require.NoError(t, err, "advertised type %q must resolve", ct)
	}
	assert.Equal(t, wantAcceptedTypes, strings.Join(supportedContentTypes, ", "),
		"the advertised list must match the literal api.md quotes")
}

// TestIngestFormat: the declared Content-Type — and only it — decides the
// format. Every accepted media type maps to its family; anything else, including
// a missing header, is refused rather than guessed at.
func TestIngestFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ct   string
		want IngestFormat
		// wantErr is any 415. There is no conflict variant: this table drives
		// resolveContentType with ONE value, and disagreement needs two header
		// lines — TestIngest_DuplicateContentTypeHeaders covers that.
		wantErr bool
	}{
		{ct: "application/json", want: FormatJSON},
		{ct: "application/json; charset=utf-8", want: FormatJSON},
		{ct: "application/x-ndjson", want: FormatNDJSON},
		{ct: "application/x-ndjson; charset=utf-8", want: FormatNDJSON},
		{ct: "application/ndjson", want: FormatNDJSON},
		{ct: "application/jsonl", want: FormatNDJSON},
		{ct: "application/jsonlines", want: FormatNDJSON},
		// A malformed *parameter* still leaves a usable media type, and these
		// all worked before the header became authoritative — refusing them
		// would be a regression, not the intended tightening.
		{ct: "application/json; charset", want: FormatJSON},
		{ct: "application/json; boundary=", want: FormatJSON},
		{ct: "application/json;;", want: FormatJSON},
		{ct: `application/json; charset="unterminated`, want: FormatJSON},
		{ct: "application/x-ndjson;charset", want: FormatNDJSON},
		// Optional whitespace before the ";" is legal (RFC 9110 §8.3
		// `parameters = *( OWS ";" OWS [ parameter ] )`). ParseMediaType trims it
		// for us — the hand-rolled resolver needed its own TrimSpace here, and a
		// future editor should not restore one.
		{ct: "application/json ; charset=utf-8", want: FormatJSON},
		{ct: "application/x-ndjson ; charset=utf-8", want: FormatNDJSON},
		// A repeated parameter name is the one malformed shape ParseMediaType
		// reports WITHOUT a media type, so tolerating ErrInvalidMediaParameter
		// alone would refuse it while accepting `; charset` and `;;` — a line
		// drawn by Go's error taxonomy rather than by "parameters never decide
		// the format". ingestFormatOne re-parses the media type alone, so all of
		// these agree. Both value spellings, since Go compares them
		// case-sensitively and would otherwise split this row's fate.
		{ct: "application/json; charset=utf-8; charset=utf-16", want: FormatJSON},
		{ct: "application/json; charset=UTF-8; charset=utf-8", want: FormatJSON},
		{ct: "application/json; charset=utf-8; charset=utf-8", want: FormatJSON},
		// ...but the re-parse must not resurrect a joined declaration: a comma
		// on an unparsed line is refused before it is reached.
		{ct: "application/json; charset=a; charset=b, application/x-ndjson", wantErr: true},
		// The tightening with no joined content anywhere in
		// it: the comma guard is line-wide, so a well-formed quoted comma loses
		// its tolerance when some OTHER parameter on the line is malformed. Both
		// halves are accepted alone (both are separate rows in this table). This row
		// is what would catch a guard
		// narrowed to "a comma after the last parsed parameter" — the known-limit
		// test would not, since that case's unparsed remainder also has a comma.
		// Tracked in #563.
		{ct: `application/json; profile="a,b"; charset`, wantErr: true},
		{ct: "APPLICATION/JSON", want: FormatJSON},
		{ct: "text/plain", wantErr: true},
		{ct: "text/csv", wantErr: true},
		{ct: "", wantErr: true},
		{ct: "   ", wantErr: true},
		{ct: "???not-a-media-type", wantErr: true},
		// A quoted-string is opaque (RFC 9110 §5.6.6), so a comma inside a
		// parameter value is data. mime.ParseMediaType handles that; the row is
		// kept because hand-rolling the split is what produced three
		// over-rejections of well-formed headers, and this fails immediately if
		// anyone reintroduces one. The escaping corpus that went with the splitter
		// is gone — testing it now would only be testing the stdlib.
		{ct: `application/json; profile="a,b"`, want: FormatJSON},
		// A COMMA-JOINED value is refused, whatever it joins. Content-Type is a
		// singleton field (§8.3) and §5.3 forbids the repetition that produces the
		// joined form, so there is no list here to resolve — §8.3 warns that
		// picking a member of the pseudo-list is itself the interoperability and
		// security hazard. These leave no media type at all — "unexpected content
		// after media subtype", or "no media type" for a leading comma — so 415
		// rather than a conflict.
		{ct: "application/json, application/x-ndjson", wantErr: true},
		{ct: "application/json, application/json", wantErr: true},
		{ct: "application/json,", wantErr: true},
		{ct: ",application/json", wantErr: true},
		{ct: ",", wantErr: true},
		// Near misses, exact-match: rewriting the lookup as a prefix or substring
		// test would leave the suite green while these start ingesting.
		{ct: "application/json5", wantErr: true},
		{ct: "application/jsonlines2", wantErr: true},
		{ct: "application/ndjson-seq", wantErr: true},
		{ct: "application/x-ndjson2", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.ct, func(t *testing.T) {
			t.Parallel()
			got, err := resolveContentType([]string{tt.ct})
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsupportedContentType,
					"a single value can only fail as unsupported — a conflict needs two header lines")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestIngest_UndeclaredOrUnsupportedContentType_415: an undeclared or unreadable
// Content-Type is refused before the body is parsed, and the message names every
// type ingest reads so the caller can fix the request from the response alone.
func TestIngest_UndeclaredOrUnsupportedContentType_415(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ct   string
		// wantPrefix is the message's declared-vs-undeclared half, which api.md's
		// 415 rows quote verbatim ("no Content-Type: …" and the declared variant
		// `Content-Type "text/plain": …`). Unasserted, the whole branch could
		// collapse to the undeclared spelling — the two-unsupported-lines subtest
		// catches that mutation as well — and a
		// caller would lose the echo telling them what the server actually read.
		// The %q also matters: it renders a header with embedded quotes
		// unambiguously.
		wantPrefix string
	}{
		{"no content-type", "", "no Content-Type: "},
		{"text/plain", "text/plain", `Content-Type "text/plain": `},
		{"text/csv", "text/csv", `Content-Type "text/csv": `},
		{"malformed media type", "???not-a-media-type", `Content-Type "???not-a-media-type": `},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
			w := httptest.NewRecorder()
			h.Handle(w, rawIngestRequest(t, "clicks", tt.ct, `{"page":"/a"}`))

			assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
			testutil.AssertJSONErrorResponse(t, w)
			assert.Contains(t, jsonErrorMessage(t, w), tt.wantPrefix,
				"the 415 body must echo what was declared, or say nothing was")
			// The body must name every accepted type, in order — asserted against
			// a LITERAL, the same list api.md's 415 rows quote. An expectation
			// built from supportedContentTypes moves with the thing it is meant
			// to pin: removing an alias, or reordering the slice, changes the
			// message and the expectation together and ships green. A per-entry
			// Contains loop was weaker still, since `application/json` and
			// `application/jsonl` are SUBSTRINGS of `application/jsonlines`.
			assert.Contains(t, jsonErrorMessage(t, w), wantAcceptedTypes,
				"the 415 body must name every accepted type, in order")
			assert.Empty(t, pub.Messages, "a refused request must not publish")
		})
	}
}

// jsonErrorMessage returns the decoded "error" field. Assert against this rather
// than the raw body: the body is JSON, so a message containing quotes — the 415
// echoes the declared Content-Type through %q — appears escaped there and a
// substring check on the raw bytes tests the encoding, not the contract.
func jsonErrorMessage(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return body.Error
}

// TestIngest_ContentTypeRefusalBeatsEmptyBody: the PR's headline ordering claim
// — "checked before the body is parsed" — is what lets a caller trust that a 415
// describes their header and not their payload. Nothing pinned it: every 415 case
// sent a non-empty body, so a reordering that peeked at the body first would have
// turned these into 400s with the suite still green.
func TestIngest_ContentTypeRefusalBeatsEmptyBody(t *testing.T) {
	t.Parallel()

	for name, ct := range map[string]string{
		"absent":      "",
		"unsupported": "text/plain",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
			w := httptest.NewRecorder()
			h.Handle(w, rawIngestRequest(t, "clicks", ct, ""))

			assert.Equal(t, http.StatusUnsupportedMediaType, w.Code,
				"the header is resolved before the body is read, so this is a 415 and not an empty-body 400")
			testutil.AssertJSONErrorResponse(t, w)
			assert.Contains(t, jsonErrorMessage(t, w), "ingest requires one of")
			assert.Empty(t, pub.Messages)
		})
	}
}

// TestIngest_DeclaredNDJSON_ArrayBodyIsNotReframed: the header is authoritative.
// A JSON array sent as NDJSON is read as NDJSON — one line, not a JSON object —
// so it fails as a per-record error instead of silently being re-read as a batch.
func TestIngest_DeclaredNDJSON_ArrayBodyIsNotReframed(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	w := httptest.NewRecorder()
	h.Handle(w, rawIngestRequest(t, "clicks", "application/x-ndjson", `[{"page":"/a"},{"page":"/b"}]`))

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, 1, resp.Total, "the array is one NDJSON line, not two records")
	assert.Equal(t, 1, resp.Failed)
	assert.Empty(t, pub.Messages)
}

// ── Multi-format ingest (JSON array, arity sniffing, body cap) ─────────────

// rawIngestRequest builds a POST /v1/ingest request with a verbatim body and an
// optional Content-Type (empty string → no header), so tests can exercise the
// 415 path and malformed-input paths directly. Sniffing now decides only arity
// within the JSON family, never the family itself.
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

// TestIngest_DuplicateContentTypeHeaders: Go keeps repeated header LINES
// separately and Header.Get returns only the first, so a request declaring both
// JSON and NDJSON would otherwise take the JSON path silently — and an NDJSON
// body read as a single object ingests record one and discards the rest,
// answering 200. Same silent truncation the comma guard prevents, reached by the
// spelling curl and many proxies actually produce. Disagreement is refused;
// repeating an identical declaration still works, so the guard cannot reject a
// request that was never ambiguous.
func TestIngest_DuplicateContentTypeHeaders(t *testing.T) {
	t.Parallel()

	const ndjson = "{\"page\":\"/a\"}\n{\"page\":\"/b\"}\n"

	t.Run("disagreeing declarations are refused", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		req := rawIngestRequest(t, "clicks", "application/json", ndjson)
		req.Header.Add("Content-Type", "application/x-ndjson")

		w := httptest.NewRecorder()
		h.Handle(w, req)

		assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
		testutil.AssertJSONErrorResponse(t, w)
		// The body text is documented verbatim in api.md's 415 tables, so it is
		// contract, not phrasing — pin it here rather than let it drift silently.
		assert.Contains(t, jsonErrorMessage(t, w), `conflicting Content-Type declarations "application/json", "application/x-ndjson"`)
		assert.Empty(t, pub.Messages, "nothing may be ingested from an ambiguous declaration")
	})

	// The json-vs-ndjson case above is caught by the format comparison alone. This
	// one is caught ONLY by the error-state conjunct: both lines resolve to
	// FormatJSON (the unsupported branch returns FormatJSON with an error), so a
	// format-only guard would let it through — and an NDJSON body then takes the
	// single-object path and publishes 1 of 2 records with a 200. Pins the half of
	// the comparison a "simplification" could delete. (TestIngestFormat rows fail on
	// that mutation too; this is the handler-level statement of it.)
	t.Run("a supported and an unsupported declaration are refused", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		req := rawIngestRequest(t, "clicks", "application/json", ndjson)
		req.Header.Add("Content-Type", "text/csv")

		w := httptest.NewRecorder()
		h.Handle(w, req)

		assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
		testutil.AssertJSONErrorResponse(t, w)
		assert.Contains(t, jsonErrorMessage(t, w), `conflicting Content-Type declarations "application/json", "text/csv"`)
		assert.Empty(t, pub.Messages, "an ambiguous declaration may not publish a truncated batch")
	})

	// Two spellings of the same family are not ambiguous: the guard compares
	// resolved FORMATS, not header text, so a proxy re-adding the type with a
	// charset or a different accepted alias must not cost the request.
	t.Run("different spellings of the same format are accepted", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		req := rawIngestRequest(t, "clicks", "application/x-ndjson", ndjson)
		req.Header.Add("Content-Type", "application/ndjson; charset=utf-8")

		w := httptest.NewRecorder()
		h.Handle(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Len(t, pub.Messages, 2, "same format, different spelling — not ambiguous")
	})

	// A comma-joined value is refused, so it can never resolve to one member and
	// read an NDJSON body as a single object. Note WHY: for most of these the
	// media type reads fine — `application/json; charset=utf-8, application/x-ndjson`
	// yields "application/json". They are refused because a comma on a line that
	// did not parse cleanly may be a second declaration joined on, and the error
	// cannot tell that from a comma inside data.
	t.Run("a comma-joined value is refused, whatever it joins", func(t *testing.T) {
		t.Parallel()
		for name, ct := range map[string]string{
			"disagreeing":            `application/json, application/x-ndjson`,
			"agreeing":               `application/json, application/json`,
			"a well-formed sibling":  `application/json; p="x,y", application/json; x="`,
			"mid-quote then a comma": `application/json; foo=a"b, application/x-ndjson`,
			"joined after a param":   `application/json; charset=utf-8, application/x-ndjson`,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				pub := &testutil.MockPublisher{}
				h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
				w := httptest.NewRecorder()
				h.Handle(w, rawIngestRequest(t, "clicks", ct, ndjson))

				assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
				testutil.AssertJSONErrorResponse(t, w)
				assert.Empty(t, pub.Messages, "must not ingest record one and drop the rest behind a 200")
			})
		}
	})

	// Joining is lossy, so the two spellings answer differently in both
	// directions. Only one of them is a shortcoming:
	//
	//   - accepts joined / refuses repeated. CORRECT, not a limit. When the joined
	//     line's quotes balance it really is one media type with an odd parameter
	//     (a comma inside a quoted value is legal qdtext), and the server cannot
	//     know an intermediary built it by illegally joining two singleton lines.
	//   - refuses joined / accepts repeated. Our deliberate fail-closed: the media
	//     type still reads fine, but a comma sits on a line that did not parse
	//     cleanly and may be a joined declaration. Narrowing it is #563.
	//
	// Pinned in both directions so neither can move unnoticed.
	t.Run("joined and repeated answer differently, by construction", func(t *testing.T) {
		t.Parallel()
		for name, tc := range map[string]struct {
			joined   string
			repeated []string
			wJoined  int
			wRepeat  int
		}{
			"joined under-rejects": {
				joined:   `application/json; a=", application/x-ndjson; b="`,
				repeated: []string{`application/json; a="`, `application/x-ndjson; b="`},
				wJoined:  http.StatusOK, wRepeat: http.StatusUnsupportedMediaType,
			},
			"joined over-rejects": {
				joined:   `application/json; p="x,y", application/json; x="`,
				repeated: []string{`application/json; p="x,y"`, `application/json; x="`},
				wJoined:  http.StatusUnsupportedMediaType, wRepeat: http.StatusOK,
			},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				wJ := httptest.NewRecorder()
				NewIngestHandler(testRegistry(t), &testutil.MockPublisher{}, testutil.NopLogger()).
					Handle(wJ, rawIngestRequest(t, "clicks", tc.joined, `{"page":"/a"}`))
				assert.Equal(t, tc.wJoined, wJ.Code, "joined")

				req := rawIngestRequest(t, "clicks", "", `{"page":"/a"}`)
				for _, v := range tc.repeated {
					req.Header.Add("Content-Type", v)
				}
				wR := httptest.NewRecorder()
				NewIngestHandler(testRegistry(t), &testutil.MockPublisher{}, testutil.NopLogger()).
					Handle(wR, req)
				assert.Equal(t, tc.wRepeat, wR.Code, "repeated")
			})
		}
	})

	t.Run("a quoted comma does not split a declaration", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		w := httptest.NewRecorder()
		h.Handle(w, rawIngestRequest(t, "clicks", `application/json; profile="a,b"`, `{"page":"/a"}`))

		assert.Equal(t, http.StatusOK, w.Code, "the comma is inside a quoted value, so this parses cleanly")
		assert.Len(t, pub.Messages, 1)
	})

	t.Run("a third line that disagrees is refused", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		req := rawIngestRequest(t, "clicks", "application/json", ndjson)
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Content-Type", "application/x-ndjson")

		w := httptest.NewRecorder()
		h.Handle(w, req)

		assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
		testutil.AssertJSONErrorResponse(t, w)
		assert.Empty(t, pub.Messages, "the third declaration must be resolved like the rest")
	})

	// An empty declaration no longer buys leniency. Content-Type is a singleton
	// field (§8.3), so a second line is malformed however it is spelled; an empty
	// one yields no media type and therefore disagrees. Both orderings, because a
	// re-resolution of Header.Get alone would answer differently from the set.
	for name, empty := range map[string]string{"blank": "", "comma": ",", "spaces": "   "} {
		t.Run("an empty line disagrees ("+name+")", func(t *testing.T) {
			t.Parallel()
			for _, first := range []bool{false, true} {
				pub := &testutil.MockPublisher{}
				h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
				req := rawIngestRequest(t, "clicks", "", ndjson)
				if first {
					req.Header.Add("Content-Type", empty)
					req.Header.Add("Content-Type", "application/x-ndjson")
				} else {
					req.Header.Add("Content-Type", "application/x-ndjson")
					req.Header.Add("Content-Type", empty)
				}

				w := httptest.NewRecorder()
				h.Handle(w, req)

				assert.Equal(t, http.StatusUnsupportedMediaType, w.Code, "empty first=%v", first)
				testutil.AssertJSONErrorResponse(t, w)
				assert.Empty(t, pub.Messages)
			}
		})
	}

	// Two unsupported declarations AGREE — both resolve to (JSON, error) — so
	// this is the unsupported path, not the conflict path. The message must name
	// both, or the caller never learns the second was sent.
	t.Run("two unsupported lines name both", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		req := rawIngestRequest(t, "clicks", "text/csv", ndjson)
		req.Header.Add("Content-Type", "text/plain")

		w := httptest.NewRecorder()
		h.Handle(w, req)

		assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
		testutil.AssertJSONErrorResponse(t, w)
		msg := jsonErrorMessage(t, w)
		assert.Contains(t, msg, `"text/csv"`)
		assert.Contains(t, msg, `"text/plain"`, "a declaration the caller sent must not vanish from the message")
		assert.NotContains(t, msg, "conflicting", "agreeing-but-unsupported is not a conflict")
		assert.Empty(t, pub.Messages)
	})

	t.Run("an identical declaration repeated is not ambiguous", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
		req := rawIngestRequest(t, "clicks", "application/x-ndjson", ndjson)
		req.Header.Add("Content-Type", "application/x-ndjson")

		w := httptest.NewRecorder()
		h.Handle(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Len(t, pub.Messages, 2, "both NDJSON records ride through")
	})
}

func TestIngest_JSONArray_AllValid(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

	// A JSON array declared as application/json is read as a batch — the body's
	// first byte picks arity within the family ingestRequest declares.
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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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

func TestIngest_JSONArray_PartialValidationFailure(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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

func TestIngest_SingleObject_PrettyPrinted(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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

func TestIngest_DeclaredJSON_ConcatenatedObjects_FirstOnly(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

	// Two concatenated objects declared as application/json take the
	// single-object path and ingest only the first (matching the historical
	// behavior — send application/x-ndjson to batch them).
	req := rawIngestRequest(t, "clicks", "application/json", `{"page":"/first"}{"page":"/second"}`)
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
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
		{name: "json empty", contentType: "application/json", body: "", wantMsg: "empty body"},
		{name: "whitespace only", contentType: "application/json", body: "   \n\t ", wantMsg: "empty body"},
		{name: "ndjson empty", contentType: "application/x-ndjson", body: "", wantMsg: "empty ndjson body"},
		{name: "ndjson whitespace only", contentType: "application/jsonl", body: "  \n ", wantMsg: "empty ndjson body"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())

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
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
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
func tsRegistry(t testing.TB) *discovery.SchemaRegistry {
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
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
	h := NewIngestHandler(tsRegistry(t), pub, testutil.NopLogger())

	req := ingestRequest(t, "events", map[string]any{
		"name":  "e",
		"ts":    "2026-06-21 04:00:00", // zone-less ClickHouse-native form
		"ts_ms": 1782014400500,         // integer number = ClickHouse ticks at the column scale (ms here)
	})
	w := httptest.NewRecorder()
	h.Handle(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	data := publishedData(t, pub)
	assert.Equal(t, "2026-06-21T04:00:00Z", data["ts"])
	assert.Equal(t, "2026-06-21T04:00:00.5Z", data["ts_ms"])
	assert.Equal(t, "e", data["name"], "non-timestamp columns untouched")
}

// TestIngest_AutoInjectedLiteralTimestampCanonicalized pins the LiteralValue
// unwrap on the auto-inject path: a placeholder-free _eq check value is typed
// policy.LiteralValue for the comparison, but must enter the published data as
// a plain string — timestamp canonicalization switches on `case string`, so a
// leaked named type would silently skip the rewrite and publish the
// non-canonical spelling (the #372 fail-open that #381's row filter relies
// on). json.Marshal renders both identically, so only this assertion on the
// canonical form catches the leak.
func TestIngest_AutoInjectedLiteralTimestampCanonicalized(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(tsRegistry(t), pub, testutil.NopLogger())
	staticTS := "2026-06-21 04:00:00"
	h.PolicySource = policy.Static(&policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"events": {
				"user": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{
					"ts": {Eq: &staticTS},
				}}},
			},
		},
	})

	// ts omitted from the body — the static literal is auto-injected, then
	// canonicalized like any producer-supplied spelling.
	req := ingestRequest(t, "events", map[string]any{"name": "e"})
	ctx := auth.WithRole(req.Context(), "user")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{})
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2026-06-21T04:00:00Z", publishedData(t, pub)["ts"],
		"auto-injected literal must be canonicalized, not published in its policy spelling")
}

// TestIngest_TimestampGarbage_PassesThrough: fail-open — an unparseable value
// publishes verbatim; ClickHouse's own parser decides insertability (#372/#381).
func TestIngest_TimestampGarbage_PassesThrough(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(tsRegistry(t), pub, testutil.NopLogger())

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
	h := NewIngestHandler(tsRegistry(t), pub, testutil.NopLogger())

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

// TestIngest_Dedup_DisabledBySettings pins the hot-reloadable switch: with
// dedupe.enabled false the deduplicator is never consulted (an Err that would
// otherwise 500 is proof), the missing-id tripwire doesn't fire even in
// strict mode, and the record publishes.
func TestIngest_Dedup_DisabledBySettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body map[string]any
	}{
		{name: "with id: not consulted", body: map[string]any{"event_id": "e1", "page": "/home"}},
		{name: "without id: strict mode does not reject", body: map[string]any{"page": "/home"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			dedup := testutil.NewMockDeduplicator()
			dedup.Err = errors.New("must not be called while disabled")
			h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
			h.Dedup = dedup
			h.DedupeSettings = func(string) (bool, string, bool) { return false, "event_id", true }

			w := httptest.NewRecorder()
			h.Handle(w, ingestRequest(t, "clicks", tt.body))
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Len(t, pub.Messages, 1, "record publishes, neither deduped nor rejected")
		})
	}
}

// TestIngest_Dedup_DisabledMidReload pins the reload window: the settings
// snapshot still says enabled but the deduplicator has already been switched
// off (or not yet on). The record publishes un-deduped instead of failing.
func TestIngest_Dedup_DisabledMidReload(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	dedup.Err = dedupe.ErrDisabled
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", true }

	w := httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"event_id": "e1", "page": "/home"}))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Len(t, pub.Messages, 1, "published without idempotency, not 500")
}
