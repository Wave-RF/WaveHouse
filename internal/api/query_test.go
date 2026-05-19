package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// safeHandle calls a handler and recovers from panics.
// Used when tests verify validation logic but pass nil for driver.Conn,
// which would panic once the handler reaches executeQuery.
func safeHandle(handler http.HandlerFunc, w *httptest.ResponseRecorder, r *http.Request) {
	defer func() { _ = recover() }()
	handler(w, r)
}

func TestQueryHandler_MissingSQL(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler(nil, nil, 0)
	body, _ := json.Marshal(queryRequest{SQL: ""})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing sql")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler(nil, nil, 0)
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader([]byte(`{bad}`)))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_PolicyForbidsRawSQL(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	store := policy.NewMemoryStore(p)
	h := NewQueryHandler(nil, nil, 0)
	h.PolicyStore = store

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM clicks"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	// Set role to "viewer" (no RawSQL permission).
	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	h.Handle(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "raw SQL queries require admin role")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_PolicyAllowsAdmin(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	store := policy.NewMemoryStore(p)
	h := NewQueryHandler(nil, nil, 0)
	h.PolicyStore = store

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM clicks"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	// Admin bypasses the raw SQL check.
	ctx := context.WithValue(r.Context(), ContextKeyRole, "admin")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	// Will fail at executeQuery (nil conn) but should get past the policy check.
	safeHandle(h.Handle, w, r)
	// Should NOT be 403.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
}

func TestQueryHandler_PolicyAllowsRawSQLRole(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"analyst": {AllowColumns: []string{"page"}, RawSQL: true},
				},
			},
		},
	}
	store := policy.NewMemoryStore(p)
	h := NewQueryHandler(nil, nil, 0)
	h.PolicyStore = store

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM clicks"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	ctx := context.WithValue(r.Context(), ContextKeyRole, "analyst")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	safeHandle(h.Handle, w, r)
	// Should NOT be 403 — analyst has RawSQL: true.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
}

func TestQueryHandler_NoPolicyAllowsAll(t *testing.T) {
	t.Parallel()
	// No PolicyStore — raw SQL should be allowed for any role.
	h := NewQueryHandler(nil, nil, 0)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query", bytes.NewReader(body))

	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	r = r.WithContext(ctx)

	safeHandle(h.Handle, w, r)
	// Should NOT be 403.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
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
		{"optimize", "OPTIMIZE TABLE t", true},
		{"replace", "REPLACE INTO t VALUES (1)", true},
		{"grant", "GRANT SELECT ON t TO u", true},
		{"revoke", "REVOKE SELECT ON t FROM u", true},
		{"system", "SYSTEM RELOAD CONFIG", true},

		{"leading whitespace", "   \n\tTRUNCATE TABLE t", true},
		{"line comment then mutation", "-- drop guard\nDROP TABLE t", true},
		{"block comment then mutation", "/* admin */ ALTER TABLE t ADD COLUMN c Int", true},
		{"mixed comments then select", "-- foo\n/* bar */ SELECT 1", false},

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

// stubConn records whether Exec or Query was called and returns canned
// results. The embedded nil driver.Conn keeps every method we don't override
// undefined-method-call-panic'd, which is what we want — the test fails
// loudly if executeQuery starts touching new surface area.
type stubConn struct {
	driver.Conn
	execCalled  bool
	queryCalled bool
	execErr     error
}

func (c *stubConn) Exec(_ context.Context, _ string, _ ...any) error {
	c.execCalled = true
	return c.execErr
}

func (c *stubConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.queryCalled = true
	return &chainEmptyRows{}, nil
}

func TestExecuteQuery_MutationRoutesToExec(t *testing.T) {
	t.Parallel()
	// TRUNCATE through /v1/query previously errored out of rows.ColumnTypes()
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
			assert.True(t, conn.execCalled, "Exec must be used for mutations")
			assert.False(t, conn.queryCalled, "Query must not be used for mutations")
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
	assert.False(t, conn.execCalled, "Exec must not be used for SELECT")
	assert.True(t, conn.queryCalled, "Query must be used for SELECT")
	assert.Equal(t, []map[string]any{}, rows, "zero-row SELECT must marshal to [] not null")
}
