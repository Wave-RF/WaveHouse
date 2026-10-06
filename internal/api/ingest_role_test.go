package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// viewerRaw is a raw ingest request under the viewer role.
func viewerRaw(t *testing.T, table, contentType, body string) *http.Request {
	t.Helper()
	req := rawIngestRequest(t, table, contentType, body)
	return req.WithContext(auth.WithRole(req.Context(), "viewer"))
}

// viewerInsertPolicy grants the viewer role insert on clicks with ins.
func viewerInsertPolicy(ins *policy.InsertPermissions) PolicySource {
	return staticPolicy(&policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Insert: ins}},
	}})
}

// TestIngest_EqCheckOnAColumnTheRoleMayNotWrite: an `_eq` check stamps a column
// the role may not otherwise write — the server-stamped org_id beside an
// allow list of what the client sends. A record omitting it is filled with the
// required value and published with it; a record supplying that value is
// admitted; any other value fails the check, per record; the role's other
// denied columns stay denied, refusing the body.
func TestIngest_EqCheckOnAColumnTheRoleMayNotWrite(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	required := "org-42"
	h.PolicySource = viewerInsertPolicy(&policy.InsertPermissions{
		AllowColumns: []string{"page"},
		Check:        map[string]policy.Filter{"org_id": {Eq: &required}},
	})

	body := `{"page":"/home"}` + "\n" +
		`{"page":"/a","org_id":"org-42"}` + "\n" +
		`{"page":"/b","org_id":"other"}` + "\n"
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerRaw(t, "clicks", "application/x-ndjson", body)))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.True(t, resultAt(t, resp, 1).Ok, "%+v", resp.Results)
	assert.True(t, resultAt(t, resp, 2).Ok, "%+v", resp.Results)
	assert.Equal(t, recordResult{Index: 3, Error: `check failed for column "org_id"`}, resultAt(t, resp, 3),
		"a failed check is the gateway's verdict, not ClickHouse's: no exception_code")

	require.Len(t, pub.Messages, 2)
	for _, m := range pub.Messages {
		var evt ingest.EventMessage
		require.NoError(t, json.Unmarshal(m.Data, &evt))
		assert.Equal(t, []string{"page", "org_id"}, evt.Columns, "the stamped column rides in the row")
		assert.Equal(t, "org-42", publishedRow(t, m.Data)["org_id"])
	}

	denied := &testutil.MockPublisher{}
	h.Publisher = denied
	w = httptest.NewRecorder()
	h.Handle(w, withTenant(viewerRaw(t, "clicks", "application/x-ndjson", body+`{"page":"/c","button":"x"}`+"\n")))
	assert.Contains(t, requireRefused(t, w, denied, 117, 4), "button", "button is still denied")
}

// visitsRegistry is a clicks table whose computed column reads a column a role
// may be denied.
func visitsRegistry(t testing.TB) *discovery.SchemaRegistry {
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{{
		Name: "clicks",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "ip", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "''", Position: 2},
			{Name: "ip_hash", Type: "UInt64", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "cityHash64(ip)", Position: 3},
		},
	}})
}

// TestIngest_DeniedColumnAComputedColumnReads: denying a column that a
// MATERIALIZED expression reads must not take the role's ingest down. The
// denied column stays declared (MATERIALIZED with its own default), so the
// role compiles, a record without it inserts, and one naming it is refused.
func TestIngest_DeniedColumnAComputedColumnReads(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, visitsRegistry(t), pub)
	h.PolicySource = viewerInsertPolicy(&policy.InsertPermissions{DenyColumns: []string{"ip"}})

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{"page": "/home"})))
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	var evt ingest.EventMessage
	require.NoError(t, json.Unmarshal(pub.LastMessage().Data, &evt))
	assert.Equal(t, []string{"page"}, evt.Columns, "the denied column is not on the wire")

	w = httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{"page": "/a", "ip": "1.2.3.4"})))
	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	msg, code := errorAndCode(t, w)
	assert.Equal(t, 117, code)
	assert.Contains(t, msg, "ip")
	assert.Len(t, pub.Messages, 1)
}

// TestIngest_RoleShapeRefusalIsNotRetryable: a role whose projection of the
// table does not compile — here, one denied every column, which would publish
// rows with none — is a standing condition of the policy and the schema. It
// answers 500 with retryable:false and no Retry-After, so neither a client nor
// the SDK retries it, and the body names no column or ClickHouse internal.
func TestIngest_RoleShapeRefusalIsNotRetryable(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	h.PolicySource = viewerInsertPolicy(&policy.InsertPermissions{
		DenyColumns: []string{"page", "button", "count", "event_id", "org_id"},
	})

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{"page": "/a"})))

	require.Equal(t, http.StatusInternalServerError, w.Code, "body=%s", w.Body.String())
	assert.Empty(t, w.Header().Get("Retry-After"))
	var body struct {
		Error     string `json:"error"`
		Retryable *bool  `json:"retryable"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "this role's insert permissions cannot be enforced on this table", body.Error)
	require.NotNil(t, body.Retryable)
	assert.False(t, *body.Retryable)
	assert.Empty(t, pub.Messages)
}

// TestIngest_UninjectableCheckValueFailsTheCheck: a check value the column
// cannot hold (`count UInt64` = "abc") does not compile as the column's
// DEFAULT. The role is served without the default, so a record omitting the
// column fails the check (403) rather than every insert being refused, and
// the log says what was done — naming the defaults, not blaming the role.
func TestIngest_UninjectableCheckValueFailsTheCheck(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelWarn)
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	required := "abc"
	h.PolicySource = viewerInsertPolicy(&policy.InsertPermissions{
		Check: map[string]policy.Filter{"count": {Eq: &required}},
	})

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{"page": "/a"})))

	require.Equal(t, http.StatusForbidden, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, `check failed for column "count"`, jsonErrorMessage(t, w))
	assert.Empty(t, pub.Messages)
	assert.Contains(t, buf.String(), "does not compile with its insert check values as column defaults")
}

// TestIngest_CheckOnASuppliableEphemeralColumn_StillRefused: a record may now
// supply an EPHEMERAL column a DEFAULT reads, but a check on one still cannot
// be enforced — the value is never stored or published — so it is refused
// with 403 whether the record supplies the column or not, and nothing is
// published.
func TestIngest_CheckOnASuppliableEphemeralColumn_StillRefused(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, ephemeralRegistry(t), pub)
	required := "1.2.3.4"
	h.PolicySource = viewerInsertPolicy(&policy.InsertPermissions{
		Check: map[string]policy.Filter{"ip": {Eq: &required}},
	})

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerRaw(t, "clicks", "application/x-ndjson",
		`{"page":"/a"}`+"\n"+`{"page":"/b","ip":"1.2.3.4"}`+"\n")))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	resp := decodeBatchResult(t, w)
	for i := 1; i <= 2; i++ {
		r := resultAt(t, resp, i)
		assert.Contains(t, r.Error, "is ephemeral and is never stored", "record %d", i)
	}
	assert.Empty(t, pub.Messages)

	w = httptest.NewRecorder()
	h.Handle(w, withTenant(viewerIngestRequest(t, "clicks", map[string]any{"page": "/c", "ip": "1.2.3.4"})))
	assert.Equal(t, http.StatusForbidden, w.Code, "body=%s", w.Body.String())
	assert.Empty(t, pub.Messages)
}

// ephemeralRegistry is a clicks table with an EPHEMERAL column a DEFAULT
// reads — the shape EPHEMERAL exists for.
func ephemeralRegistry(t testing.TB) *discovery.SchemaRegistry {
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{{
		Name: "clicks",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "ip", Type: "String", HasDefault: true, DefaultKind: "EPHEMERAL", DefaultExpression: "''", Position: 2},
			{Name: "ip_len", Type: "UInt64", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "length(ip)", Position: 3},
		},
	}})
}

// TestIngest_EphemeralColumnFeedsItsDefault: a record may name an EPHEMERAL
// column in a format that names its columns; the value feeds the DEFAULT over
// it and is never published. A positional CSV body has no slot for it.
func TestIngest_EphemeralColumnFeedsItsDefault(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, ephemeralRegistry(t), pub)

	for _, tc := range []struct{ contentType, body string }{
		{"application/json", `{"page":"/a","ip":"1.2.3.4"}`},
		{"application/json", `[{"page":"/a","ip":"1.2.3.4"}]`},
		{"application/x-ndjson", `{"page":"/a","ip":"1.2.3.4"}` + "\n"},
		{"text/csv; header=present", "page,ip\n/a,1.2.3.4\n"},
		{"text/tab-separated-values; header=present", "ip\tpage\n1.2.3.4\t/a\n"},
		{"text/csv; header=absent", "/a,7\n"},
	} {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(rawIngestRequest(t, "clicks", tc.contentType, tc.body)))
		require.Equal(t, http.StatusOK, w.Code, "%s: body=%s", tc.contentType, w.Body.String())

		var evt ingest.EventMessage
		require.NoError(t, json.Unmarshal(pub.LastMessage().Data, &evt))
		assert.Equal(t, []string{"page", "ip_len"}, evt.Columns, tc.contentType)
		assert.JSONEq(t, `["/a", 7]`, string(evt.Row), tc.contentType)
	}
}
