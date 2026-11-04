package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
)

func TestDedupeIDStoresNothing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		col  discovery.Column
		want bool
	}{
		{"String", discovery.Column{Type: "String"}, true},
		{"LowCardinality(String)", discovery.Column{Type: "LowCardinality(String)"}, true},
		{"Nullable(String)", discovery.Column{Type: "Nullable(String)"}, true},
		{"LowCardinality(Nullable(String))", discovery.Column{Type: "LowCardinality(Nullable(String))"}, true},
		{"Nullable(UUID)", discovery.Column{Type: "Nullable(UUID)"}, true},
		{"Nullable(UInt64)", discovery.Column{Type: "Nullable(UInt64)"}, true},
		{"Nullable(FixedString(16))", discovery.Column{Type: "Nullable(FixedString(16))"}, true},
		{"Nullable(IPv4)", discovery.Column{Type: "Nullable(IPv4)"}, true},
		{"Nullable(Enum8)", discovery.Column{Type: "Nullable(Enum8('a' = 0, 'b' = 1))"}, true},
		{"Nullable(Date)", discovery.Column{Type: "Nullable(Date)"}, true},
		{"LowCardinality(Nullable(UInt64))", discovery.Column{Type: "LowCardinality(Nullable(UInt64))"}, true},

		{"UInt64", discovery.Column{Type: "UInt64"}, false},
		{"Int32", discovery.Column{Type: "Int32"}, false},
		{"UUID", discovery.Column{Type: "UUID"}, false},
		{"FixedString", discovery.Column{Type: "FixedString(16)"}, false},
		{"IPv4", discovery.Column{Type: "IPv4"}, false},
		{"Enum8", discovery.Column{Type: "Enum8('a' = 0, 'b' = 1)"}, false},
		{"Date", discovery.Column{Type: "Date"}, false},
		{"DateTime", discovery.Column{Type: "DateTime"}, false},
		{"LowCardinality(UInt64)", discovery.Column{Type: "LowCardinality(UInt64)"}, false},
		{"LowCardinality(FixedString)", discovery.Column{Type: "LowCardinality(FixedString(4))"}, false},
		{"Array(Nullable(String))", discovery.Column{Type: "Array(Nullable(String))"}, false},
		{"unbalanced LowCardinality", discovery.Column{Type: "LowCardinality(String"}, false},
		{"empty type", discovery.Column{}, false},

		{"String DEFAULT", discovery.Column{Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'x'"}, false},
		{"String DEFAULT '' (flag only)", discovery.Column{Type: "String", HasDefault: true, DefaultExpression: "''"}, false},
		{"UUID DEFAULT generator", discovery.Column{Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()"}, false},
		{"Nullable(String) DEFAULT", discovery.Column{Type: "Nullable(String)", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "NULL"}, false},
		{"Nullable(String) MATERIALIZED", discovery.Column{Type: "Nullable(String)", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "toString(page)"}, false},
		{"String ALIAS", discovery.Column{Type: "String", HasDefault: true, DefaultKind: "ALIAS", DefaultExpression: "page"}, false},
		{"String EPHEMERAL", discovery.Column{Type: "String", HasDefault: true, DefaultKind: "EPHEMERAL", DefaultExpression: "''"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dedupeIDStoresNothing(tt.col))
		})
	}
}

// dedupeIDTables holds one table per id column shape, each with event_id and a
// page to tell its records apart: the ids_* ones conform to the rule, the
// refused_* ones do not.
func dedupeIDTables(t testing.TB) *discovery.SchemaRegistry {
	page := discovery.Column{Name: "page", Type: "String"}
	table := func(name string, id discovery.Column) *discovery.TableSchema {
		id.Name = "event_id"
		return &discovery.TableSchema{Name: name, Columns: []discovery.Column{id, page}}
	}
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		table("ids_nuuid", discovery.Column{Type: "Nullable(UUID)"}),
		table("ids_nuint", discovery.Column{Type: "Nullable(UInt64)"}),
		table("ids_str", discovery.Column{Type: "String"}),
		table("ids_lcnstr", discovery.Column{Type: "LowCardinality(Nullable(String))"}),
		table("refused_uint", discovery.Column{Type: "UInt64"}),
		table("refused_default", discovery.Column{Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'x'"}),
		table("refused_generated", discovery.Column{Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()"}),
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

// TestIngest_DedupeIDNotSentIsMissing: on a column that stores nothing the
// record did not send, an omitted, null or empty id is missing, so it is
// published un-deduped (never keyed on a stored value, which would make every
// later one a duplicate) or refused under require_id, while an id the record
// does send dedupes across requests.
func TestIngest_DedupeIDNotSentIsMissing(t *testing.T) {
	t.Parallel()
	tables := []struct {
		table string
		sent  string // an id as the record writes it
		key   string // as the row stores it
		empty bool   // whether "" is a value of the column
	}{
		{"ids_nuuid", `"61f0c404-5cb3-11e7-907b-a6006ad3dba0"`, "61f0c404-5cb3-11e7-907b-a6006ad3dba0", false},
		{"ids_nuint", `7`, "7", false},
		{"ids_str", `"evt-1"`, "evt-1", true},
		{"ids_lcnstr", `"evt-1"`, "evt-1", true},
	}
	for _, tc := range tables {
		for _, requireID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/require_id=%t", tc.table, requireID), func(t *testing.T) {
				t.Parallel()
				h, pub, dd := dedupeIDHandler(t, requireID)

				lines := []string{`{"page":"/a"}`, `{"page":"/b","event_id":null}`}
				if tc.empty {
					lines = append(lines, `{"page":"/c","event_id":""}`)
				}
				w := handle(h, ndjsonRequest(t, tc.table, lines...))
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

				before := len(pub.Messages)
				for i, wantDuplicates := range []int{0, 1} {
					w = handle(h, ndjsonRequest(t, tc.table, `{"page":"/d","event_id":`+tc.sent+`}`))
					require.Equal(t, http.StatusOK, w.Code, "send %d: %s", i+1, w.Body.String())
					assert.Equal(t, wantDuplicates, decodeBatchResult(t, w).Duplicates, "send %d", i+1)
				}
				assert.True(t, dd.Committed(dedupe.Key{Table: tc.table, ID: tc.key}))
				assert.Len(t, pub.Messages, before+1, "the repeated id was published once")
			})
		}
	}
}

// TestIngest_DedupeIDNotSentIsMissingSingleObject is the same rule on the
// single-object response shape.
func TestIngest_DedupeIDNotSentIsMissingSingleObject(t *testing.T) {
	t.Parallel()
	for _, requireID := range []bool{false, true} {
		for _, body := range []string{`{"page":"/a"}`, `{"page":"/a","event_id":null}`} {
			t.Run(fmt.Sprintf("require_id=%t/%s", requireID, body), func(t *testing.T) {
				t.Parallel()
				h, pub, _ := dedupeIDHandler(t, requireID)
				w := handle(h, rawIngestRequest(t, "ids_nuuid", "application/json", body))
				if requireID {
					require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
					assert.Contains(t, w.Body.String(), "missing dedupe id field")
					assert.Empty(t, pub.Messages)
					return
				}
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				assert.JSONEq(t, `{"ok":true}`, w.Body.String())
				assert.Len(t, pub.Messages, 1)
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
				"ids_nuuid": {"writer": {Insert: &policy.InsertPermissions{AllowColumns: []string{"page"}}}},
			}})
			send := func(body string) *httptest.ResponseRecorder {
				r := rawIngestRequest(t, "ids_nuuid", "application/json", body)
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

			w = send(`{"page":"/b","event_id":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}`)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), `"exception_code":117`)
			assert.False(t, dd.Committed(dedupe.Key{Table: "ids_nuuid", ID: "61f0c404-5cb3-11e7-907b-a6006ad3dba0"}))
		})
	}
}

// TestIngest_DedupeIDColumnThatStoresAValueIsRefused: a table whose dedupe id
// column stores a value the record never sent (a non-Nullable number, a
// DEFAULT, a generated UUID) answers every ingest with a 500 that says why and
// is not retryable, publishes nothing and never consults the dedupe store.
func TestIngest_DedupeIDColumnThatStoresAValueIsRefused(t *testing.T) {
	t.Parallel()
	const want = `dedupe id column "event_id" must be Nullable or String with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression`
	bodies := []struct{ name, contentType, body string }{
		{"id sent", "application/json", `{"page":"/a","event_id":%s}`},
		{"id omitted", "application/json", `{"page":"/a"}`},
		{"ndjson", "application/x-ndjson", `{"page":"/a"}` + "\n" + `{"page":"/b"}` + "\n"},
	}
	sent := map[string]string{"refused_uint": "7", "refused_default": `"e1"`, "refused_generated": `"61f0c404-5cb3-11e7-907b-a6006ad3dba0"`}
	for _, table := range []string{"refused_uint", "refused_default", "refused_generated"} {
		for _, requireID := range []bool{false, true} {
			for _, b := range bodies {
				t.Run(fmt.Sprintf("%s/require_id=%t/%s", table, requireID, b.name), func(t *testing.T) {
					t.Parallel()
					h, pub, dd := dedupeIDHandler(t, requireID)
					dd.Err = errors.New("the dedupe store must not be consulted")
					body := b.body
					if b.name == "id sent" {
						body = fmt.Sprintf(body, sent[table])
					}

					w := handle(h, rawIngestRequest(t, table, b.contentType, body))

					require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
					assert.Empty(t, w.Header().Get("Retry-After"))
					var resp struct {
						Error     string `json:"error"`
						Retryable *bool  `json:"retryable"`
					}
					require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
					assert.Equal(t, want, resp.Error)
					require.NotNil(t, resp.Retryable)
					assert.False(t, *resp.Retryable)
					assert.Empty(t, pub.Messages)
				})
			}
		}
	}
}

// TestIngest_DedupeIDRefusalIsLoggedOnce: the refusal stands until an operator
// changes the settings or the table, so one line a minute says so, not one per
// request.
func TestIngest_DedupeIDRefusalIsLoggedOnce(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelError)
	h, _, _ := dedupeIDHandler(t, false)
	for range 3 {
		w := handle(h, rawIngestRequest(t, "refused_uint", "application/json", `{"page":"/a"}`))
		require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	}
	assert.Equal(t, 1, strings.Count(buf.String(), "the dedupe id column stores a value the record did not send"), buf.String())
	assert.Contains(t, buf.String(), `"id_field":"event_id"`)
	assert.Contains(t, buf.String(), `"type":"UInt64"`)
}

// TestIngest_DedupeIDRuleAppliesOnlyToTheIDColumnOfADedupingTable: dedupe off,
// or on with another column as the id, leaves a UInt64 event_id an ordinary
// column, and an id_field naming no column is the per-record missing-id path,
// not a refusal.
func TestIngest_DedupeIDRuleAppliesOnlyToTheIDColumnOfADedupingTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dd   settings.Dedupe
	}{
		{"dedupe off", settings.Dedupe{IDField: "event_id", RequireID: true}},
		{"another column is the id", settings.Dedupe{Enabled: true, IDField: "page"}},
		{"id_field names no column", settings.Dedupe{Enabled: true, IDField: "nope"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, pub, _ := dedupeIDHandler(t, false)
			h.DedupeSettings = func(*settings.Store, string) settings.Dedupe { return tt.dd }

			w := handle(h, rawIngestRequest(t, "refused_uint", "application/json", `{"page":"/a","event_id":5}`))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Len(t, pub.Messages, 1)
		})
	}
}
