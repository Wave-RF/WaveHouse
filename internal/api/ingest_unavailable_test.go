package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// This file replaces ingest_seams_test.go. That file covered the InsertChecker
// seam — the one per-record decision point the type layer did not take over. It
// has taken it over: a check clause is now a compiled chtypes filter over the
// rows ClickHouse already accepted, so there is no Go-side comparison left to
// swap out. What survives is the group of tests about the type layer being
// absent or unable to answer, which is the fail-closed property those seam tests
// were really pinning.

// TestIngest_UnwiredTypeLayer_Refuses: a handler with no type layer must refuse
// the request, not wave it through. Nothing else on the ingest path looks at a
// value, so nil Types reading as "accept anything" would turn a wiring mistake
// into an open door — the same fail-closed direction the row evaluator takes.
func TestIngest_UnwiredTypeLayer_Refuses(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(testRegistry(t), pub, testutil.NopLogger())
	require.Nil(t, h.Types)

	w := httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/home"}))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "30", w.Header().Get("Retry-After"), "an outage is retryable; the body is unchanged")
	testutil.AssertJSONErrorResponse(t, w)
	assert.Empty(t, pub.Messages)
}

// TestIngest_UndiscoveredTable_Unavailable: a table the registry knows but the
// type layer has no compiled schema for is a 503 naming the cause, never a 400.
// The request is fine; the server cannot judge it.
func TestIngest_UndiscoveredTable_Unavailable(t *testing.T) {
	t.Parallel()
	reg := testRegistry(t)
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(reg, pub, testutil.NopLogger())
	// An engine bound to NO tables: every lookup is Unavailable.
	engineMu.Lock()
	h.Types = typelayer.TestEngine(t)
	engineMu.Unlock()

	w := httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/home"}))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, jsonErrorMessage(t, w), "not discovered")
	assert.Empty(t, pub.Messages)
}

// TestIngest_ParseErrorsPrecedeCheckErrors inverts the ordering this file's
// predecessor pinned, and is a DOCUMENTED contract change (AUDIT §A.2). Check
// clauses are now a filter over rows ClickHouse has already accepted, so a
// record that fails both reports the PARSE error, not the check. No enforcement
// is lost — nothing is published either way — but a client can no longer infer
// from a 403 that the rest of its payload was well formed.
func TestIngest_ParseErrorsPrecedeCheckErrors(t *testing.T) {
	t.Parallel()
	required := "org-allowed"
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub, testutil.NopLogger())
	h.PolicySource = policy.Static(&policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Insert: &policy.InsertPermissions{
			Check: map[string]policy.Filter{"org_id": {Eq: &required}},
		}}},
	}})

	w := httptest.NewRecorder()
	h.Handle(w, viewerIngestRequest(t, "clicks", map[string]any{
		"page":   "/a",
		"org_id": "wrong",        // fails the check clause
		"count":  "not-a-number", // and ClickHouse refuses it first
	}))

	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	_, code := errorAndCode(t, w)
	assert.Equal(t, 27, code, "ClickHouse's own refusal, with its own code")
	assert.Empty(t, pub.Messages)

	// The same record, parseable: NOW the check clause is what refuses it.
	w = httptest.NewRecorder()
	h.Handle(w, viewerIngestRequest(t, "clicks", map[string]any{
		"page":   "/a",
		"org_id": "wrong",
		"count":  1,
	}))
	assert.Equal(t, http.StatusForbidden, w.Code, "body=%s", w.Body.String())
	assert.Contains(t, jsonErrorMessage(t, w), `check failed for column "org_id"`)
	assert.Empty(t, pub.Messages)
}

// TestIngest_DedupeMarksOnlyPublishedRecords: dedupe runs AFTER validation, so a
// record ClickHouse refuses does not burn its idempotency key — the caller can
// fix the record and resend it under the same id. Marking before validation
// would swallow the corrected retry as a duplicate.
func TestIngest_DedupeMarksOnlyPublishedRecords(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := newTestIngestHandler(t, testRegistry(t), pub, testutil.NopLogger())
	h.Dedup = dedup
	h.DedupeSettings = func(string) (bool, string, bool) { return true, "event_id", false }

	// A record ClickHouse refuses (count is not a number), carrying an id.
	w := httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/a", "event_id": "evt-1", "count": "nope"}))
	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	assert.Empty(t, pub.Messages)

	// The same id, corrected: it must publish, not report a duplicate.
	w = httptest.NewRecorder()
	h.Handle(w, ingestRequest(t, "clicks", map[string]any{"page": "/a", "event_id": "evt-1", "count": 1}))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Contains(t, w.Body.String(), `"ok":true`)
	assert.Len(t, pub.Messages, 1)

	// And only NOW is the id spent.
	seen, err := dedup.CheckAndMark(context.Background(), "evt-1")
	require.NoError(t, err)
	assert.True(t, seen, "the published record's id is marked")
}

// viewerIngestRequest is ingestRequest with the "viewer" role in context, for
// the policy-gated tests.
func viewerIngestRequest(t *testing.T, table string, body map[string]any) *http.Request {
	t.Helper()
	req := ingestRequest(t, table, body)
	return req.WithContext(auth.WithRole(req.Context(), "viewer"))
}
