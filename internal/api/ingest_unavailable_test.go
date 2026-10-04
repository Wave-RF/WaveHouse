package api

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/Wave-RF/WaveHouse/internal/typelayer/typelayertest"
)

// The type layer judges every record, so there is no Go-side comparison left
// to swap out. What these tests pin is the type layer being absent or unable to
// answer for a tenant — the fail-closed property — and the ordering it imposes.

// assertTypesUnavailable pins the 503 a tenant the type layer cannot judge
// answers with: a generic message, the schema refresh's Retry-After, nothing
// published.
func assertTypesUnavailable(t *testing.T, w *httptest.ResponseRecorder, pub *testutil.MockPublisher) {
	t.Helper()
	require.Equal(t, http.StatusServiceUnavailable, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, retryAfterSchema, w.Header().Get("Retry-After"), "an outage is retryable; the body is unchanged")
	assert.Equal(t, "ingest validation is unavailable", jsonErrorMessage(t, w))
	testutil.AssertJSONErrorResponse(t, w)
	assert.Empty(t, pub.Messages)
}

// TestIngest_UnwiredTypeLayer_Refuses: a handler with no type layer must refuse
// the request, not wave it through. Nothing else on the ingest path looks at a
// value, so nil Types reading as "accept anything" would turn a wiring mistake
// into an open door — the same fail-closed direction the row evaluator takes.
func TestIngest_UnwiredTypeLayer_Refuses(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(fixedRegistry(testRegistry(t)), pub)
	require.Nil(t, h.Types)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/home"})))

	assertTypesUnavailable(t, w, pub)
}

// TestIngest_UndiscoveredTable_Unavailable: a table the registry knows but the
// type layer has no compiled schema for is a 503, never a 400 — the request is
// fine; the server cannot judge it. The cause is the operator's: it is logged,
// and the body stays generic.
func TestIngest_UndiscoveredTable_Unavailable(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelError)
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(fixedRegistry(testRegistry(t)), pub)
	h.Types = typelayertest.TestEngine(t) // bound to NO tables

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/home"})))

	assertTypesUnavailable(t, w, pub)
	assert.Contains(t, buf.String(), "not discovered", "the log carries the cause")
}

// TestIngest_UnboundTenant_RefusedBeforeTheBodyIsRead: a tenant the type layer
// has not bound — its discovery has not refreshed, or its line has no
// artifact — is refused on its own, with the schema refresh's Retry-After, and
// decided before the body is read, like a schema not discovered yet. Another
// tenant on the same engine keeps ingesting, and the body never names the
// tenant or the cause.
func TestIngest_UnboundTenant_RefusedBeforeTheBodyIsRead(t *testing.T) {
	t.Parallel()
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(100)})
	reg := testRegistry(t)
	pub := &testutil.MockPublisher{}
	h := NewIngestHandler(fixedRegistry(reg), pub)
	h.Types = typelayertest.TestEngine(t) // tenant.Default only, and no tables
	h.Types.Bind("globex", typelayertest.TestServerVersion, "UTC", reg.List())

	acme, ok := tenants.For("acme")
	require.True(t, ok)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ingest?table=clicks",
		iotest.ErrReader(errors.New("the body must not be read")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.Handle(w, req.WithContext(WithStore(req.Context(), acme)))
	assertTypesUnavailable(t, w, pub)
	assert.NotContains(t, w.Body.String(), "acme", "the body does not name the tenant")

	globex, ok := tenants.For("globex")
	require.True(t, ok)
	req = ingestRequest(t, "clicks", map[string]any{"page": "/home"})
	w = httptest.NewRecorder()
	h.Handle(w, req.WithContext(WithStore(req.Context(), globex)))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	require.Len(t, pub.Published(), 1)
	assert.Equal(t, tenant.ID("globex"), pub.Published()[0].Topic.Tenant)
}

// TestIngest_ParseErrorsPrecedeCheckErrors is a DOCUMENTED contract change.
// Check clauses are a filter over rows ClickHouse has already accepted, so a
// record that fails both reports the PARSE error, not the check. No enforcement
// is lost — nothing is published either way — but a client can no longer infer
// from a 403 that the rest of its payload was well formed.
func TestIngest_ParseErrorsPrecedeCheckErrors(t *testing.T) {
	t.Parallel()
	required := "org-allowed"
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	h.PolicySource = staticPolicy(&policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Insert: &policy.InsertPermissions{
			Check: map[string]policy.Filter{"org_id": {Eq: &required}},
		}}},
	}})

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{
		"page":   "/a",
		"org_id": "wrong",        // fails the check clause
		"count":  "not-a-number", // and ClickHouse refuses it first
	})))

	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	_, code := errorAndCode(t, w)
	assert.Equal(t, 27, code, "ClickHouse's own refusal, with its own code")
	assert.Empty(t, pub.Messages)

	// The same record, parseable: NOW the check clause is what refuses it.
	w = httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{
		"page":   "/a",
		"org_id": "wrong",
		"count":  1,
	})))
	assert.Equal(t, http.StatusForbidden, w.Code, "body=%s", w.Body.String())
	assert.Contains(t, jsonErrorMessage(t, w), `check failed for column "org_id"`)
	_, code = errorAndCode(t, w)
	assert.Zero(t, code, "a gateway rejection never carries a ClickHouse code")
	assert.Empty(t, pub.Messages)
}

// TestIngest_DedupeMarksOnlyPublishedRecords: dedupe runs AFTER validation, so a
// record ClickHouse refuses never claims its id — the caller can fix the record
// and resend it under the same id. Claiming before validation would swallow the
// corrected retry as a duplicate (or as in flight, for a lease).
func TestIngest_DedupeMarksOnlyPublishedRecords(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	h := newTestIngestHandler(t, testRegistry(t), pub)
	h.Dedup = staticDedup(dedup)
	h.DedupeSettings = func(*settings.Store, string) settings.Dedupe {
		return settings.Dedupe{Enabled: true, IDField: "event_id"}
	}
	key := dedupe.Key{Table: "clicks", ID: "evt-1"}

	// A record ClickHouse refuses (count is not a number), carrying an id.
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/a", "event_id": "evt-1", "count": "nope"})))
	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	assert.Empty(t, pub.Messages)
	assert.Zero(t, dedup.Reserves, "a refused record reserves nothing")
	assert.False(t, dedup.Pending(key))
	assert.False(t, dedup.Committed(key))

	// The same id, corrected: it must publish, not report a duplicate.
	w = httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/a", "event_id": "evt-1", "count": 1})))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Contains(t, w.Body.String(), `"ok":true`)
	assert.Len(t, pub.Messages, 1)

	// And only NOW is the id spent.
	assert.True(t, dedup.Committed(key), "the published record's id is committed")

	// In a batch, the refused sibling claims nothing either.
	w = httptest.NewRecorder()
	h.Handle(w, withTenant(ndjsonRequest(t, "clicks",
		jsonLine(t, map[string]any{"page": "/b", "event_id": "evt-2", "count": "nope"}),
		jsonLine(t, map[string]any{"page": "/c", "event_id": "evt-3"}),
	)))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.False(t, dedup.Pending(dedupe.Key{Table: "clicks", ID: "evt-2"}))
	assert.False(t, dedup.Committed(dedupe.Key{Table: "clicks", ID: "evt-2"}))
	assert.True(t, dedup.Committed(dedupe.Key{Table: "clicks", ID: "evt-3"}))
}

// viewerIngestRequest is ingestRequest with the "viewer" role in context, for
// the policy-gated tests.
func viewerIngestRequest(t *testing.T, table string, body map[string]any) *http.Request {
	t.Helper()
	req := ingestRequest(t, table, body)
	return req.WithContext(auth.WithRole(req.Context(), "viewer"))
}
