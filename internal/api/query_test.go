package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// safeHandle calls a handler and recovers from panics.
// Used when tests verify validation logic but pass nil for driver.Conn,
// which would panic once the handler reaches executeQuery. Shared with
// structured_query_test.go and pipes_test.go.
func safeHandle(handler http.HandlerFunc, w *httptest.ResponseRecorder, r *http.Request) {
	defer func() { _ = recover() }()
	handler(w, r)
}

func TestQueryHandler_MissingSQL(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler(nil, nil, 0)
	body, _ := json.Marshal(queryRequest{SQL: ""})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/query", bytes.NewReader(body))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing sql")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler(nil, nil, 0)
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/query", bytes.NewReader([]byte(`{bad}`)))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
}

func TestQueryCacheKey_Deterministic(t *testing.T) {
	t.Parallel()
	k1 := queryCacheKey("SELECT 1", nil)
	k2 := queryCacheKey("SELECT 1", nil)
	assert.Equal(t, k1, k2)

	k3 := queryCacheKey("SELECT 1", []any{"a"})
	assert.NotEqual(t, k1, k3)

	k4 := queryCacheKey("SELECT 2", nil)
	assert.NotEqual(t, k1, k4)
}

func TestIsMutation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{"select", "SELECT 1", false},
		{"select lower", "select 1", false},
		{"with cte", "WITH x AS (SELECT 1) SELECT * FROM x", false},
		{"show", "SHOW TABLES", false},
		{"describe", "DESCRIBE clicks", false},
		{"explain", "EXPLAIN SELECT 1", false},
		{"exists", "EXISTS TABLE clicks", false},

		{"insert", "INSERT INTO t VALUES (1)", true},
		{"update", "UPDATE t SET a=1 WHERE b=2", true},
		{"delete", "DELETE FROM t WHERE id=1", true},
		{"truncate", "TRUNCATE TABLE t", true},
		{"truncate lower", "truncate table t", true},
		{"drop", "DROP TABLE t", true},
		{"alter", "ALTER TABLE t ADD COLUMN c String", true},
		{"create", "CREATE TABLE t (a Int)", true},
		{"rename", "RENAME TABLE a TO b", true},
		{"exchange", "EXCHANGE TABLES t1 AND t2", true},
		{"optimize", "OPTIMIZE TABLE t", true},
		{"replace", "REPLACE INTO t VALUES (1)", true},
		{"grant", "GRANT SELECT ON t TO u", true},
		{"revoke", "REVOKE SELECT ON t FROM u", true},
		{"system", "SYSTEM RELOAD CONFIG", true},
		{"attach", "ATTACH TABLE t FROM '/path'", true},
		{"detach", "DETACH TABLE t", true},
		{"kill", "KILL QUERY WHERE query_id = 'abc'", true},
		{"set", "SET max_threads = 4", true},
		{"use", "USE mydb", true},

		{"leading whitespace", "   \n\tTRUNCATE TABLE t", true},
		{"line comment then mutation", "-- drop guard\nDROP TABLE t", true},
		{"hash line comment then mutation", "# audit\nDROP TABLE t", true},
		{"block comment then mutation", "/* admin */ ALTER TABLE t ADD COLUMN c Int", true},
		{"mixed comments then select", "-- foo\n# bar\n/* baz */ SELECT 1", false},
		{"with insert", "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte", true},
		{"with insert lower", "with cte as (select 1) insert into t select * from cte", true},
		{"with delete", "WITH cte AS (SELECT id FROM x) DELETE FROM t WHERE id IN (SELECT id FROM cte)", true},
		{"with update", "WITH cte AS (SELECT 1) ALTER TABLE t UPDATE a=1 WHERE id IN (SELECT id FROM cte)", true},
		{"with truncate", "WITH cte AS (SELECT 1) TRUNCATE TABLE t", true},
		{"with multi-cte insert", "WITH a AS (SELECT 1), b AS (SELECT 2) INSERT INTO t SELECT * FROM a JOIN b", true},
		{"with nested parens insert", "WITH cte AS (SELECT id FROM t WHERE id IN (1,2,3)) INSERT INTO t2 SELECT * FROM cte", true},
		{"with paren-in-string insert", "WITH cte AS (SELECT ')' AS x) INSERT INTO t2 SELECT * FROM cte", true},
		{"with materialized insert", "WITH cte AS MATERIALIZED (SELECT 1) INSERT INTO t SELECT * FROM cte", true},
		{"with recursive select", "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT * FROM x) SELECT * FROM x", false},
		{"with nested select", "WITH x AS (SELECT 1) SELECT * FROM (SELECT * FROM x)", false},
		{"with scalar insert", "WITH '/path' AS p INSERT INTO files VALUES (p)", true},
		{"with line comment containing DELETE then select", "WITH cte AS (SELECT 1) -- old DELETE approach\nSELECT * FROM cte", false},
		{"with hash comment containing TRUNCATE then select", "WITH cte AS (SELECT 1) # was TRUNCATE\nSELECT * FROM cte", false},
		{"with block comment containing INSERT then select", "WITH cte AS (SELECT 1) /* INSERT reminder */ SELECT * FROM cte", false},
		{"with comment then real mutation", "WITH cte AS (SELECT 1) -- explanatory\nINSERT INTO t SELECT * FROM cte", true},
		{"with unclosed block comment", "WITH cte AS (SELECT 1) /* unterminated comment DELETE", false},

		{"empty", "", false},
		{"comment only", "-- just a comment", false},
		{"unclosed block comment", "/* never closed", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isMutation(tt.sql))
		})
	}
}

// stubConn records how many times Exec or Query was called and returns
// canned results. The embedded nil driver.Conn keeps every method we don't
// override undefined-method-call-panic'd, which is what we want — the test
// fails loudly if executeQuery starts touching new surface area.
type stubConn struct {
	driver.Conn
	execCount  int
	queryCount int
	execErr    error
}

func (c *stubConn) Exec(_ context.Context, _ string, _ ...any) error {
	c.execCount++
	return c.execErr
}

func (c *stubConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.queryCount++
	return &chainEmptyRows{}, nil
}

func TestExecuteQuery_MutationRoutesToExec(t *testing.T) {
	t.Parallel()
	// TRUNCATE through /v1/admin/query previously errored out of rows.ColumnTypes()
	// and surfaced as HTTP 500. Mutations now go through Exec and marshal to
	// an empty `[]` on success.
	for _, sql := range []string{
		"TRUNCATE TABLE clicks",
		"DROP TABLE clicks",
		"DELETE FROM clicks WHERE id = 1",
		"ALTER TABLE clicks ADD COLUMN c String",
		"INSERT INTO clicks VALUES (1)",
		"  -- audit log\n  UPDATE clicks SET v = 1 WHERE id = 2",
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			conn := &stubConn{}
			h := NewQueryHandler(conn, nil, 0)
			rows, err := h.executeQuery(context.Background(), sql, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, conn.execCount, "Exec must be used for mutations")
			assert.Zero(t, conn.queryCount, "Query must not be used for mutations")
			assert.Equal(t, []map[string]any{}, rows, "mutation result must marshal to [] not null")
		})
	}
}

func TestExecuteQuery_SelectRoutesToQuery(t *testing.T) {
	t.Parallel()
	conn := &stubConn{}
	h := NewQueryHandler(conn, nil, 0)
	rows, err := h.executeQuery(context.Background(), "SELECT 1", nil)
	require.NoError(t, err)
	assert.Zero(t, conn.execCount, "Exec must not be used for SELECT")
	assert.Equal(t, 1, conn.queryCount, "Query must be used for SELECT")
	assert.Equal(t, []map[string]any{}, rows, "zero-row SELECT must marshal to [] not null")
}

// TestQueryHandler_MutationBypassesCache pins the cache-bypass contract for
// mutations. Without the bypass, the first TRUNCATE would land its `[]`
// response in the cache under the SQL key, and the second identical
// TRUNCATE would hit the cache and return `[]` without re-executing — the
// silent-data-loss scenario claude-review flagged on the #118 PR. With the
// bypass, both calls reach Exec.
//
// The test also asserts the cache is *empty* after the mutation completes,
// so a regression that re-introduces `Cache.Set` post-execution (instead
// of fully early-returning) would also fail loudly.
func TestQueryHandler_MutationBypassesCache(t *testing.T) {
	t.Parallel()

	local, err := cache.NewLocal(1 << 20) // 1MiB is plenty for one entry
	require.NoError(t, err)
	defer func() { _ = local.Close() }()
	tiered := cache.NewTiered(local, nil)

	conn := &stubConn{}
	h := NewQueryHandler(conn, tiered, 5*time.Minute)

	const sql = "TRUNCATE TABLE clicks"
	body, _ := json.Marshal(queryRequest{SQL: sql})

	doRequest := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/query", bytes.NewReader(body))
		h.Handle(w, r)
		return w
	}

	w1 := doRequest()
	require.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "[]", w1.Body.String(), "first mutation response must be []")
	assert.Equal(t, "MISS", w1.Header().Get("X-Cache"), "mutations never serve from cache")

	// After the first call, the cache must NOT carry the mutation result.
	// Ristretto buffers writes asynchronously, so flush before reading.
	local.Wait()
	cached, _, err := tiered.Get(context.Background(), queryCacheKey(sql, nil))
	require.NoError(t, err)
	assert.Nil(t, cached, "mutation result must not be written to cache (silent-data-loss guard)")

	w2 := doRequest()
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "[]", w2.Body.String())
	assert.Equal(t, "MISS", w2.Header().Get("X-Cache"))

	assert.Equal(t, 2, conn.execCount,
		"second identical mutation must re-execute against ClickHouse, not be served from cache")
	assert.Zero(t, conn.queryCount, "mutations must never route through Query")
}
