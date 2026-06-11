package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestParseQueryTreeTables(t *testing.T) {
	t.Parallel()
	// Representative EXPLAIN QUERY TREE dump: TABLE nodes carry the resolved
	// table; non-TABLE lines have no table_name; the qualified value may be the
	// last field on its line or be followed by more fields; quoted identifiers can
	// contain commas.
	lines := []string{
		"QUERY id: 0",
		"  PROJECTION COLUMNS",
		"    page String",
		"  JOIN TREE",
		"    TABLE id: 3, alias: __table1, table_name: default.events",
		"    TABLE id: 5, table_name: default.users, alias: __table2",
		"    TABLE id: 7, table_name: default.`weird,name`",
	}
	got := parseQueryTreeTables(lines)
	assert.Equal(t, []string{
		"default.events",
		"default.users",
		"default.`weird,name`",
	}, got)
}

func TestIdentifierField(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		" default.events":              "default.events",
		" default.users, alias: x":     "default.users",
		" default.`weird,name`":        "default.`weird,name`",
		" default.`weird,name`, id: 9": "default.`weird,name`",
		"":                             "",
	}
	for in, want := range tests {
		assert.Equal(t, want, identifierField(in), "input %q", in)
	}
}

func TestSplitQualified(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, db, table string }{
		{"default.events", "default", "events"},
		{"events", "", "events"},
		{"`db`.`weird.name`", "db", "weird.name"}, // dot inside backticks stays in the table
		{"`a.b`", "", "a.b"},                      // quoted, dot inside ticks, no qualifier
	}
	for _, c := range cases {
		db, table := splitQualified(c.in)
		assert.Equal(t, c.db, db, "db of %q", c.in)
		assert.Equal(t, c.table, table, "table of %q", c.in)
	}
}

func TestUnquoteIdent(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "events", unquoteIdent("events"))
	assert.Equal(t, "weird name", unquoteIdent("`weird name`"))
	assert.Equal(t, "a`b", unquoteIdent("`a\\`b`"))   // escaped backtick
	assert.Equal(t, "a\\b", unquoteIdent("`a\\\\b`")) // escaped backslash
}

func TestFilterKnownTables(t *testing.T) {
	t.Parallel()
	registry := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{Name: "events"},
		{Name: "users"},
	})
	raw := []string{
		"default.events",       // kept
		"default.users",        // kept
		"default.events",       // duplicate → collapsed
		"other.events",         // different database → dropped
		"default.unknown_view", // not in registry → dropped
		"system.one",           // system table, different database → dropped
		"events",               // bare, default database → kept (collapses with default.events)
	}
	got := filterKnownTables(raw, "default", registry)
	assert.Equal(t, []string{"events", "users"}, got) // deduped + sorted
}

func TestPipeDeps(t *testing.T) {
	t.Parallel()
	assert.Nil(t, pipeDeps(nil))
	assert.Nil(t, pipeDeps([]string{}))

	got := pipeDeps([]string{"events", "users"})
	assert.Equal(t, []cache.Namespace{
		{Table: query.SafeEncodeNATS("events")},
		{Table: query.SafeEncodeNATS("users")},
	}, got)
}

// fakeCache records the dependency namespaces handed to Get/Set and can serve a
// canned hit, so a handler test can assert what deps it built without a
// ClickHouse connection.
type fakeCache struct {
	getDeps []cache.Namespace
	setDeps []cache.Namespace
	getData []byte
}

func (f *fakeCache) Get(_ context.Context, _ string, deps []cache.Namespace) ([]byte, time.Duration, error) {
	f.getDeps = deps
	return f.getData, 0, nil
}

func (f *fakeCache) Set(_ context.Context, _ string, deps []cache.Namespace, _ []byte, _ time.Duration) error {
	f.setDeps = deps
	return nil
}

func (f *fakeCache) Invalidate(_ context.Context, _ []cache.Namespace) (uint64, error) {
	return 0, nil
}

func (f *fakeCache) Close() error { return nil }

func TestPipesHandler_Execute_PassesResolvedDepsToCache(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(&pipes.NamedQuery{
		Name:           "by_page",
		SQL:            "SELECT * FROM events",
		ResolvedTables: []string{"events"},
	})
	fc := &fakeCache{getData: []byte(`[{"n":1}]`)} // serve a hit so Execute returns before any query
	h := NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), nil, fc, 0, testutil.NopLogger())

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/by_page/execute", "by_page", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))
	h.Execute(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "HIT", w.Header().Get("X-Cache"))
	assert.Equal(t, []cache.Namespace{{Table: query.SafeEncodeNATS("events")}}, fc.getDeps)
}

func TestPipesHandler_Execute_NoResolvedDeps_NilToCache(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore(&pipes.NamedQuery{
		Name: "any",
		SQL:  "SELECT * FROM events", // no ResolvedTables → TTL-only (nil deps)
	})
	fc := &fakeCache{getData: []byte(`[{"n":1}]`)}
	h := NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), nil, fc, 0, testutil.NopLogger())

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/any/execute", "any", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))
	h.Execute(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Nil(t, fc.getDeps)
}

func TestPipesHandler_Put_IgnoresClientResolvedTables(t *testing.T) {
	t.Parallel()
	store := pipes.NewMemoryStore()
	// No Registry/CHConn wired → resolution is skipped, so ResolvedTables must come
	// out nil regardless of what the client sent (deps are server-owned).
	h := NewPipesHandler(store, nil, nil, nil, 0, testutil.NopLogger())

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodPut, "/v1/pipes/p", "p", map[string]any{
		"sql":             "SELECT 1",
		"resolved_tables": []string{"injected"},
	})
	h.Put(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	saved := store.Get("p")
	assert.NotNil(t, saved)
	assert.Nil(t, saved.ResolvedTables)
}
