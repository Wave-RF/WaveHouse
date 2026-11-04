package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
)

// idDefaultTables are tables whose dedupe id column stores a value when a
// record omits it: a generated UUID, an integer's 0, a constant DEFAULT.
func idDefaultTables(t *testing.T) *discovery.SchemaRegistry {
	page := discovery.Column{Name: "page", Type: "String"}
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "ids_uuid", Columns: []discovery.Column{
			{Name: "event_id", Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()"}, page,
		}},
		{Name: "ids_int", Columns: []discovery.Column{{Name: "event_id", Type: "UInt64"}, page}},
		{Name: "ids_const", Columns: []discovery.Column{
			{Name: "event_id", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'none'"}, page,
		}},
	})
}

// TestIngest_DedupeIDOmittedIsMissing: a record that omits its dedupe id is
// missing it, whatever its column stores in its place, so it is neither keyed
// on that value (which would make every such record after the first a
// duplicate) nor let past require_id. A supplied id equal to the column's
// default is supplied, and dedupes. A role that may not write the id column
// never supplies one: every record's id is missing, and naming it is code 117.
func TestIngest_DedupeIDOmittedIsMissing(t *testing.T) {
	t.Parallel()
	tables := []struct {
		table, sameAsDefault, stored string
	}{
		{"ids_uuid", `"00000000-0000-0000-0000-000000000000"`, "00000000-0000-0000-0000-000000000000"},
		{"ids_int", `0`, "0"},
		{"ids_const", `"none"`, "none"},
	}
	for _, tc := range tables {
		for _, requireID := range []bool{false, true} {
			for _, ndjson := range []bool{false, true} {
				for _, restricted := range []bool{false, true} {
					name := fmt.Sprintf("%s/require_id=%t/ndjson=%t/restricted=%t", tc.table, requireID, ndjson, restricted)
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						pub := &testutil.MockPublisher{}
						dd := testutil.NewMockDeduplicator()
						h := newTestIngestHandler(t, idDefaultTables(t), pub)
						h.Dedup = staticDedup(dd)
						h.DedupeSettings = func(*settings.Store, string) settings.Dedupe {
							return settings.Dedupe{Enabled: true, IDField: "event_id", RequireID: requireID}
						}
						if restricted {
							h.PolicySource = staticPolicy(&policy.Policy{Tables: map[string]policy.TablePolicy{
								tc.table: {"writer": {Insert: &policy.InsertPermissions{AllowColumns: []string{"page"}}}},
							}})
						}
						send := func(lines ...string) *httptest.ResponseRecorder {
							w := httptest.NewRecorder()
							var r *http.Request
							if ndjson {
								r = ndjsonRequest(t, tc.table, lines...)
							} else {
								require.Len(t, lines, 1)
								r = rawIngestRequest(t, tc.table, "application/json", lines[0])
							}
							if restricted {
								r = r.WithContext(auth.WithRole(r.Context(), "writer"))
							}
							h.Handle(w, withTenant(r))
							return w
						}

						omitted := []string{`{"page":"/a"}`, `{"page":"/b"}`}
						if !ndjson {
							omitted = omitted[:1]
						}
						w := send(omitted...)
						switch {
						case requireID && ndjson:
							require.Equal(t, http.StatusOK, w.Code, w.Body.String())
							resp := decodeBatchResult(t, w)
							assert.Equal(t, 2, resp.Failed)
							assert.Contains(t, resultAt(t, resp, 1).Error, "missing dedupe id field")
							assert.Empty(t, pub.Messages)
						case requireID:
							require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
							assert.Contains(t, w.Body.String(), "missing dedupe id field")
							assert.Empty(t, pub.Messages)
						case ndjson:
							require.Equal(t, http.StatusOK, w.Code, w.Body.String())
							resp := decodeBatchResult(t, w)
							assert.Equal(t, 2, resp.Succeeded, "neither is a duplicate of the other")
							assert.Len(t, pub.Messages, 2)
						default:
							require.Equal(t, http.StatusOK, w.Code, w.Body.String())
							assert.JSONEq(t, `{"ok":true}`, w.Body.String())
							assert.Len(t, pub.Messages, 1)
						}
						assert.False(t, dd.Committed(dedupe.Key{Table: tc.table, ID: tc.stored}), "an omitted id is never keyed on its default")

						supplied := `{"page":"/c","event_id":` + tc.sameAsDefault + `}`
						if restricted {
							before := len(pub.Messages)
							w = send(supplied)
							require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
							assert.Contains(t, w.Body.String(), `"exception_code":117`)
							assert.Len(t, pub.Messages, before)
							return
						}

						// Supplied, the default's own value is an id like any other.
						before := len(pub.Messages)
						w = send(supplied)
						require.Equal(t, http.StatusOK, w.Code, w.Body.String())
						assert.True(t, dd.Committed(dedupe.Key{Table: tc.table, ID: tc.stored}), "a supplied id equal to the default is supplied")
						w = send(supplied)
						require.Equal(t, http.StatusOK, w.Code, w.Body.String())
						if ndjson {
							assert.Equal(t, 1, decodeBatchResult(t, w).Duplicates)
						} else {
							assert.JSONEq(t, `{"duplicate":true}`, w.Body.String())
						}
						assert.Len(t, pub.Messages, before+1)
					})
				}
			}
		}
	}
}

// TestIngest_DedupeIDExplicitNullIsMissing: a null id the column reads as its
// default (input_format_null_as_default) is missing, as a null id always has
// been, not keyed on the value stored in its place.
func TestIngest_DedupeIDExplicitNullIsMissing(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dd := testutil.NewMockDeduplicator()
	h := newTestIngestHandler(t, idDefaultTables(t), pub)
	h.Dedup = staticDedup(dd)
	h.DedupeSettings = func(*settings.Store, string) settings.Dedupe {
		return settings.Dedupe{Enabled: true, IDField: "event_id", RequireID: true}
	}
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "ids_int", "application/json", `{"page":"/a","event_id":null}`)))
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "missing dedupe id field")
	assert.Empty(t, pub.Messages)
}
