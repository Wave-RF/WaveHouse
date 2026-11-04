package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
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
		{"Nullable(String)", discovery.Column{Type: "Nullable(String)"}, true},
		{"LowCardinality(String)", discovery.Column{Type: "LowCardinality(String)"}, true},
		{"LowCardinality(Nullable(String))", discovery.Column{Type: "LowCardinality(Nullable(String))"}, true},

		// An empty string sent for these is stored as the type's zero value, not
		// NULL, so the first one would key every later one as a duplicate.
		{"Nullable(UInt64)", discovery.Column{Type: "Nullable(UInt64)"}, false},
		{"Nullable(Int32)", discovery.Column{Type: "Nullable(Int32)"}, false},
		{"Nullable(Int8)", discovery.Column{Type: "Nullable(Int8)"}, false},
		{"Nullable(Decimal)", discovery.Column{Type: "Nullable(Decimal(10, 2))"}, false},
		{"Nullable(FixedString(4))", discovery.Column{Type: "Nullable(FixedString(4))"}, false},
		{"Nullable(UUID)", discovery.Column{Type: "Nullable(UUID)"}, false},
		{"Nullable(Date)", discovery.Column{Type: "Nullable(Date)"}, false},
		{"Nullable(Float64)", discovery.Column{Type: "Nullable(Float64)"}, false},
		{"Nullable(IPv4)", discovery.Column{Type: "Nullable(IPv4)"}, false},
		{"Nullable(Enum8)", discovery.Column{Type: "Nullable(Enum8('a' = 0, 'b' = 1))"}, false},
		{"Nullable(Bool)", discovery.Column{Type: "Nullable(Bool)"}, false},
		{"LowCardinality(Nullable(UInt64))", discovery.Column{Type: "LowCardinality(Nullable(UInt64))"}, false},

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
		{"Array(String)", discovery.Column{Type: "Array(String)"}, false},
		{"Array(Nullable(String))", discovery.Column{Type: "Array(Nullable(String))"}, false},
		{"Nullable(LowCardinality(String))", discovery.Column{Type: "Nullable(LowCardinality(String))"}, false},
		{"unbalanced LowCardinality", discovery.Column{Type: "LowCardinality(String"}, false},
		{"unbalanced Nullable", discovery.Column{Type: "Nullable(String"}, false},
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
// page to tell its records apart: the ids_* ones are the four forms the rule
// admits, the refused_* ones do not conform.
func dedupeIDTables(t testing.TB) *discovery.SchemaRegistry {
	page := discovery.Column{Name: "page", Type: "String"}
	table := func(name string, id discovery.Column) *discovery.TableSchema {
		id.Name = "event_id"
		return &discovery.TableSchema{Name: name, Columns: []discovery.Column{id, page}}
	}
	return testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		table("ids_str", discovery.Column{Type: "String"}),
		table("ids_nstr", discovery.Column{Type: "Nullable(String)"}),
		table("ids_lcstr", discovery.Column{Type: "LowCardinality(String)"}),
		table("ids_lcnstr", discovery.Column{Type: "LowCardinality(Nullable(String))"}),
		table("refused_uint", discovery.Column{Type: "UInt64"}),
		table("refused_nuint", discovery.Column{Type: "Nullable(UInt64)"}),
		table("refused_nuuid", discovery.Column{Type: "Nullable(UUID)"}),
		table("refused_nfixed", discovery.Column{Type: "Nullable(FixedString(4))"}),
		table("refused_default", discovery.Column{Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'x'"}),
		table("refused_generated", discovery.Column{Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()"}),
	})
}

// dedupeIDStringTables are the tables of dedupeIDTables a dedupe id may sit in.
var dedupeIDStringTables = []string{"ids_str", "ids_nstr", "ids_lcstr", "ids_lcnstr"}

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

// TestIngest_DedupeIDNotSentIsMissing: on a string id column, an omitted, null
// or empty id is missing, so it is published un-deduped (never keyed on a
// stored value, which would make every later one a duplicate) or refused under
// require_id, while an id the record does send dedupes across requests.
func TestIngest_DedupeIDNotSentIsMissing(t *testing.T) {
	t.Parallel()
	for _, table := range dedupeIDStringTables {
		for _, requireID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/require_id=%t", table, requireID), func(t *testing.T) {
				t.Parallel()
				h, pub, dd := dedupeIDHandler(t, requireID)

				lines := []string{`{"page":"/a"}`, `{"page":"/b","event_id":null}`, `{"page":"/c","event_id":""}`, `{"page":"/c2","event_id":""}`}
				w := handle(h, ndjsonRequest(t, table, lines...))
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
				assert.False(t, dd.Committed(dedupe.Key{Table: table, ID: ""}), "the empty id is never keyed")

				before := len(pub.Messages)
				for i, wantDuplicates := range []int{0, 1} {
					w = handle(h, ndjsonRequest(t, table, `{"page":"/d","event_id":"evt-1"}`))
					require.Equal(t, http.StatusOK, w.Code, "send %d: %s", i+1, w.Body.String())
					assert.Equal(t, wantDuplicates, decodeBatchResult(t, w).Duplicates, "send %d", i+1)
				}
				assert.True(t, dd.Committed(dedupe.Key{Table: table, ID: "evt-1"}))
				assert.Len(t, pub.Messages, before+1, "the repeated id was published once")
			})
		}
	}
}

// TestIngest_DedupeIDNotSentIsMissingSingleObject is the same rule on the
// single-object response shape.
func TestIngest_DedupeIDNotSentIsMissingSingleObject(t *testing.T) {
	t.Parallel()
	for _, table := range dedupeIDStringTables {
		for _, requireID := range []bool{false, true} {
			for _, body := range []string{`{"page":"/a"}`, `{"page":"/a","event_id":null}`, `{"page":"/a","event_id":""}`} {
				t.Run(fmt.Sprintf("%s/require_id=%t/%s", table, requireID, body), func(t *testing.T) {
					t.Parallel()
					h, pub, _ := dedupeIDHandler(t, requireID)
					w := handle(h, rawIngestRequest(t, table, "application/json", body))
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

// TestIngest_DedupeIDColumnThatStoresAValueIsRefused: a table whose dedupe id
// column is not a string, or has a DEFAULT, answers every ingest with a 500
// that says why and is not retryable, publishes nothing and never consults the
// dedupe store. That includes a Nullable number, UUID or FixedString, which
// store a zero value for an empty id rather than NULL.
func TestIngest_DedupeIDColumnThatStoresAValueIsRefused(t *testing.T) {
	t.Parallel()
	const want = `dedupe id column "event_id" must be String or Nullable(String) with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression`
	bodies := []struct{ name, contentType, body string }{
		{"id sent", "application/json", `{"page":"/a","event_id":%s}`},
		{"id omitted", "application/json", `{"page":"/a"}`},
		{"ndjson", "application/x-ndjson", `{"page":"/a"}` + "\n" + `{"page":"/b"}` + "\n"},
	}
	sent := map[string]string{
		"refused_uint": "7", "refused_nuint": "7", "refused_nfixed": `"abcd"`, "refused_default": `"e1"`,
		"refused_nuuid":     `"61f0c404-5cb3-11e7-907b-a6006ad3dba0"`,
		"refused_generated": `"61f0c404-5cb3-11e7-907b-a6006ad3dba0"`,
	}
	for _, table := range []string{"refused_uint", "refused_nuint", "refused_nuuid", "refused_nfixed", "refused_default", "refused_generated"} {
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

// TestIngest_DedupeIDReloadMidRequest: dedupeIDReady judges the settings read
// before the body, but a reload can land while the body is processed. The
// settings read for each record are checked against the same rule, so a switch
// to a column that could store a value the record never sent, or a switch of
// dedupe on with one, refuses the request instead of keying on it. Nothing in
// the open window is published and nothing is keyed.
func TestIngest_DedupeIDReloadMidRequest(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{{Name: "reloads", Columns: []discovery.Column{
		{Name: "event_id", Type: "String"},
		{Name: "alt_id", Type: "Nullable(String)"},
		{Name: "seq", Type: "UInt64"},
		{Name: "page", Type: "String"},
	}}})
	const refused = `dedupe id column "seq" must be String or Nullable(String) with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression`
	off := settings.Dedupe{}
	on := func(field string) settings.Dedupe { return settings.Dedupe{Enabled: true, IDField: field} }
	lines := []string{
		`{"page":"/a","event_id":"a","alt_id":"x","seq":7}`,
		`{"page":"/b","event_id":"b","alt_id":"y","seq":8}`,
		`{"page":"/c","event_id":"c","alt_id":"z","seq":9}`,
	}
	tests := []struct {
		name string
		// reads is what each read of the settings returns, in order: the
		// request's check first, then one per record; the last repeats.
		reads         []settings.Dedupe
		lines         []string
		window        int
		wantRefused   bool
		wantPublished int
		wantKeyed     []string
	}{
		{
			name:  "id_field moves to a column the rule refuses",
			reads: []settings.Dedupe{on("event_id"), on("event_id"), on("seq")}, lines: lines,
			wantRefused: true,
		},
		{
			name:  "dedupe switched on with a column the rule refuses",
			reads: []settings.Dedupe{off, off, on("seq")}, lines: lines,
			wantRefused: true,
		},
		{
			name:  "a single object, dedupe switched on with a column the rule refuses",
			reads: []settings.Dedupe{off, on("seq")}, lines: lines[:1],
			wantRefused: true,
		},
		{
			name:  "windows already published stand",
			reads: []settings.Dedupe{on("event_id"), on("event_id"), on("seq")}, lines: lines, window: 1,
			wantRefused: true, wantPublished: 1, wantKeyed: []string{"a"},
		},
		{
			name:  "dedupe switched on with a string column",
			reads: []settings.Dedupe{off, off, on("event_id")}, lines: lines,
			wantPublished: 3, wantKeyed: []string{"b", "c"},
		},
		{
			name:  "id_field moves to another string column",
			reads: []settings.Dedupe{on("event_id"), on("event_id"), on("alt_id")}, lines: lines,
			wantPublished: 3, wantKeyed: []string{"a", "y", "z"},
		},
		{
			name:  "id_field moves to a column the table does not have",
			reads: []settings.Dedupe{on("event_id"), on("event_id"), on("nope")}, lines: lines,
			wantPublished: 3, wantKeyed: []string{"a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			dd := testutil.NewMockDeduplicator()
			h := newTestIngestHandler(t, reg, pub)
			h.Dedup = staticDedup(dd)
			h.window = tt.window
			var reads atomic.Int32
			h.DedupeSettings = func(*settings.Store, string) settings.Dedupe {
				return tt.reads[min(int(reads.Add(1))-1, len(tt.reads)-1)]
			}

			req := ndjsonRequest(t, "reloads", tt.lines...)
			if len(tt.lines) == 1 {
				req = rawIngestRequest(t, "reloads", "application/json", tt.lines[0])
			}
			w := handle(h, req)

			if tt.wantRefused {
				require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
				var resp struct {
					Error     string `json:"error"`
					Retryable *bool  `json:"retryable"`
				}
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
				assert.Equal(t, refused, resp.Error)
				require.NotNil(t, resp.Retryable)
				assert.False(t, *resp.Retryable)
				assert.Empty(t, w.Header().Get("Retry-After"))
			} else {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			}
			assert.Len(t, pub.Messages, tt.wantPublished)
			for _, id := range []string{"a", "b", "c", "x", "y", "z", "7", "8", "9"} {
				assert.Equal(t, slices.Contains(tt.wantKeyed, id), dd.Committed(dedupe.Key{Table: "reloads", ID: id}), "id %q", id)
			}
		})
	}
}
