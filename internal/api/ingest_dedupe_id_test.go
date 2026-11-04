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

// dedupeIDTables holds one table per id column shape, each with event_id and a
// page to tell its records apart. The ids_* tables follow the documented
// recommendation (Nullable(T) or String, no DEFAULT); ids_uint and ids_default
// ignore it.
func dedupeIDTables(t testing.TB) *discovery.SchemaRegistry {
	page := discovery.Column{Name: "page", Type: "String"}
	table := func(name string, id discovery.Column) *discovery.TableSchema {
		id.Name = "event_id"
		return &discovery.TableSchema{Name: name, Columns: []discovery.Column{id, page}}
	}
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		table("ids_str", discovery.Column{Type: "String"}),
		table("ids_nstr", discovery.Column{Type: "Nullable(String)"}),
		table("ids_nuint", discovery.Column{Type: "Nullable(UInt64)"}),
		table("ids_uint", discovery.Column{Type: "UInt64"}),
		table("ids_default", discovery.Column{Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'unset'"}),
	})
}

// dedupeIDHandler is a handler over dedupeIDTables that dedupes every table
// on event_id.
func dedupeIDHandler(t *testing.T, requireID bool) (*IngestHandler, *testutil.MockPublisher, *testutil.MockDeduplicator) {
	t.Helper()
	pub := &testutil.MockPublisher{}
	dd := testutil.NewMockDeduplicator()
	h := newTestIngestHandler(t, dedupeIDTables(t), pub)
	h.Dedup = staticDedup(dd)
	h.DedupeSettings = func(*settings.Store, string) settings.Dedupe {
		return settings.Dedupe{Enabled: true, IDField: "event_id", RequireID: requireID}
	}
	return h, pub, dd
}

func handle(h *IngestHandler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(r))
	return w
}

// dedupeIDRecommended are the id columns the docs recommend: an id the record
// does not send is stored as NULL (or "" for a String) and so is missing. id is
// a real id as the JSON the producer sends and key the stored value it is
// keyed on; emptyString says whether an empty string also reads as missing (a
// Nullable number stores it as 0).
var dedupeIDRecommended = []struct {
	table, id, key string
	emptyString    bool
}{
	{"ids_str", `"evt-1"`, "evt-1", true},
	{"ids_nstr", `"evt-1"`, "evt-1", true},
	{"ids_nuint", `7`, "7", false},
}

// TestIngest_DedupeIDNotSentIsMissing: on the recommended id columns, an
// omitted or null id is missing, so it is published un-deduped (never keyed on
// a stored value, which would make every later one a duplicate) or refused
// under require_id, while an id the record does send dedupes across requests,
// a numeric one included.
func TestIngest_DedupeIDNotSentIsMissing(t *testing.T) {
	t.Parallel()
	for _, c := range dedupeIDRecommended {
		for _, requireID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/require_id=%t", c.table, requireID), func(t *testing.T) {
				t.Parallel()
				h, pub, dd := dedupeIDHandler(t, requireID)

				lines := []string{`{"page":"/a"}`, `{"page":"/b","event_id":null}`}
				if c.emptyString {
					lines = append(lines, `{"page":"/c","event_id":""}`, `{"page":"/c2","event_id":""}`)
				}
				w := handle(h, ndjsonRequest(t, c.table, lines...))
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				resp := decodeBatchResult(t, w)
				if requireID {
					assert.Equal(t, len(lines), resp.Failed)
					for i := range lines {
						assert.Contains(t, resultAt(t, resp, i+1).Error, "missing dedupe id field", "record %d", i+1)
					}
					assert.Empty(t, pub.Messages)
				} else {
					assert.Equal(t, len(lines), resp.Succeeded, "none is a duplicate of another")
					assert.Zero(t, resp.Duplicates)
					assert.Len(t, pub.Messages, len(lines))
				}
				assert.False(t, dd.Committed(dedupe.Key{Table: c.table, ID: ""}), "a missing id is never keyed")
				assert.False(t, dd.Committed(dedupe.Key{Table: c.table, ID: "0"}), "a missing id is never keyed")

				before := len(pub.Messages)
				for i, wantDuplicates := range []int{0, 1} {
					w = handle(h, ndjsonRequest(t, c.table, fmt.Sprintf(`{"page":"/d","event_id":%s}`, c.id)))
					require.Equal(t, http.StatusOK, w.Code, "send %d: %s", i+1, w.Body.String())
					assert.Equal(t, wantDuplicates, decodeBatchResult(t, w).Duplicates, "send %d", i+1)
				}
				assert.True(t, dd.Committed(dedupe.Key{Table: c.table, ID: c.key}))
				assert.Len(t, pub.Messages, before+1, "the repeated id was published once")
			})
		}
	}
}

// TestIngest_DedupeIDNotSentIsMissingSingleObject is the same rule on the
// single-object response shape.
func TestIngest_DedupeIDNotSentIsMissingSingleObject(t *testing.T) {
	t.Parallel()
	for _, c := range dedupeIDRecommended {
		for _, requireID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/require_id=%t", c.table, requireID), func(t *testing.T) {
				t.Parallel()
				h, pub, _ := dedupeIDHandler(t, requireID)
				bodies := []string{`{"page":"/a"}`, `{"page":"/a","event_id":null}`}
				if c.emptyString {
					bodies = append(bodies, `{"page":"/a","event_id":""}`)
				}
				for _, body := range bodies {
					before := len(pub.Messages)
					w := handle(h, rawIngestRequest(t, c.table, "application/json", body))
					if requireID {
						require.Equal(t, http.StatusBadRequest, w.Code, "%s: %s", body, w.Body.String())
						assert.Contains(t, w.Body.String(), "missing dedupe id field", body)
						assert.Len(t, pub.Messages, before, body)
						continue
					}
					require.Equal(t, http.StatusOK, w.Code, "%s: %s", body, w.Body.String())
					assert.JSONEq(t, `{"ok":true}`, w.Body.String(), body)
					assert.Len(t, pub.Messages, before+1, body)
				}
			})
		}
	}
}

// TestIngest_DedupeIDTheRoleMayNotWriteIsMissing: a column the role may not
// write stores NULL for every record, so the id is missing for each of them
// and naming the column is code 117.
func TestIngest_DedupeIDTheRoleMayNotWriteIsMissing(t *testing.T) {
	t.Parallel()
	for _, requireID := range []bool{false, true} {
		t.Run(fmt.Sprintf("require_id=%t", requireID), func(t *testing.T) {
			t.Parallel()
			h, pub, dd := dedupeIDHandler(t, requireID)
			h.PolicySource = staticPolicy(&policy.Policy{Tables: map[string]policy.TablePolicy{
				"ids_nstr": {"writer": {Insert: &policy.InsertPermissions{AllowColumns: []string{"page"}}}},
			}})
			send := func(body string) *httptest.ResponseRecorder {
				r := rawIngestRequest(t, "ids_nstr", "application/json", body)
				return handle(h, r.WithContext(auth.WithRole(r.Context(), "writer")))
			}

			w := send(`{"page":"/a"}`)
			if requireID {
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				assert.Contains(t, w.Body.String(), "missing dedupe id field")
				assert.Empty(t, pub.Messages)
			} else {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				assert.Len(t, pub.Messages, 1)
			}

			w = send(`{"page":"/b","event_id":"evt-1"}`)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), `"exception_code":117`)
			assert.False(t, dd.Committed(dedupe.Key{Table: "ids_nstr", ID: "evt-1"}))
		})
	}
}

// TestIngest_DedupeIDColumnThatStoresAValueIsKeyedOnIt pins the documented
// caveat, not a behavior to rely on: WaveHouse does not check the id column, so
// on one that ignores the recommendation (Nullable, or String, with no
// DEFAULT) an id the record did not send is stored as a value, and that value
// is the key. The first such record is published and the rest are duplicates
// of it, require_id included, since the id is not missing.
func TestIngest_DedupeIDColumnThatStoresAValueIsKeyedOnIt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, table, first, second, key string
	}{
		{"omitted id in a non-Nullable UInt64 stores 0", "ids_uint", `{"page":"/a"}`, `{"page":"/b"}`, "0"},
		{"null id in a non-Nullable UInt64 stores 0", "ids_uint", `{"page":"/a","event_id":null}`, `{"page":"/b"}`, "0"},
		{"empty string into a Nullable number stores 0", "ids_nuint", `{"page":"/a","event_id":""}`, `{"page":"/b","event_id":""}`, "0"},
		{"omitted id in a column with a DEFAULT stores it", "ids_default", `{"page":"/a"}`, `{"page":"/b"}`, "unset"},
	}
	for _, tt := range tests {
		for _, requireID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/require_id=%t", tt.name, requireID), func(t *testing.T) {
				t.Parallel()
				h, pub, dd := dedupeIDHandler(t, requireID)

				w := handle(h, rawIngestRequest(t, tt.table, "application/json", tt.first))
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				assert.Len(t, pub.Messages, 1)
				assert.True(t, dd.Committed(dedupe.Key{Table: tt.table, ID: tt.key}), "the stored value is the key")

				w = handle(h, ndjsonRequest(t, tt.table, tt.second))
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				resp := decodeBatchResult(t, w)
				assert.Equal(t, 1, resp.Duplicates, "the second id-less record is answered as a duplicate")
				assert.Zero(t, resp.Failed)
				assert.Len(t, pub.Messages, 1, "and not published")
			})
		}
	}
}
