package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/singleflight"
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

func TestExtractCacheTags(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "happy path standard select",
			sql:      "SELECT * FROM users",
			expected: []string{"users"},
		},
		{
			name:     "happy path multiple clauses",
			sql:      "SELECT * FROM users JOIN clicks ON users.id = clicks.user_id",
			expected: []string{"users", "clicks"},
		},
		{
			name:     "comma separated tables (known limitation)",
			sql:      "SELECT * FROM t1, t2",
			expected: []string{"t1"}, // t2 is missed by current simple regex, documented stop-gap
		},
		{
			name:     "cte alias captured",
			sql:      "WITH cte AS (SELECT id FROM users) SELECT * FROM cte",
			expected: []string{"users", "cte"},
		},
		{
			name:     "subquery outer paren rejected, inner captured",
			sql:      "SELECT * FROM (SELECT * FROM inner_tbl)",
			expected: []string{"inner_tbl"},
		},
		{
			name:     "mutations",
			sql:      "UPDATE state SET x = 1; INSERT INTO events (id) VALUES (1)",
			expected: []string{"state", "events"},
		},
		{
			name:     "mutation with block comment stripped",
			sql:      "INSERT INTO /*x*/ events (id) VALUES (1)",
			expected: []string{"events"},
		},
		{
			name:     "select with trailing line comment stripped",
			sql:      "SELECT * FROM users -- tail",
			expected: []string{"users"},
		},
		{
			name:     "quoted identifiers supported",
			sql:      "SELECT * FROM `my_db`.`my_table` JOIN \"other_table\"",
			expected: []string{"my_table", "other_table"},
		},
		{
			name:     "column named drop does not trigger mutation logic",
			sql:      "SELECT id, drop_rate FROM metrics",
			expected: []string{"metrics"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleaned := cleanSQLForTags(tc.sql)
			actual := extractCacheTagsFromCleaned(cleaned)

			if len(tc.expected) == 0 {
				assert.Empty(t, actual)
			} else {
				assert.ElementsMatch(t, tc.expected, actual)
			}
		})
	}
}

func TestQueryHandler_MutationInvalidation(t *testing.T) {
	mockCache := testutil.NewMockCache()

	// Stub connection
	mockConn := &testutil.MockConn{
		QueryFn: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return &testutil.MockRows{}, nil
		},
	}

	handler := NewQueryHandler(mockConn, mockCache, time.Minute)
	handler.sf = singleflight.Group{}

	t.Run("successful mutation triggers invalidation", func(t *testing.T) {
		mockCache.Reset() // Clear previous state

		sql := "INSERT INTO orders (id) VALUES (1)"
		reqBody, _ := json.Marshal(queryRequest{SQL: sql})

		req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/query", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()

		handler.Handle(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		// Verify the tag was recorded in our new InvalidatedTags slice
		assert.Contains(t, mockCache.InvalidatedTags, "orders")
	})

	t.Run("read query does not invalidate cache", func(t *testing.T) {
		mockCache.Reset()
		sql := "SELECT * FROM orders"
		reqBody, _ := json.Marshal(queryRequest{SQL: sql})
		req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/query", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()

		handler.Handle(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, mockCache.InvalidatedTags, "reads must not trigger InvalidateByTags")
	})
}

func TestQueryHandler_StringLiteralMutationBypass(t *testing.T) {
	t.Parallel()

	sql := `SELECT * FROM events WHERE message = 'INSERT INTO secret_table VALUES (1)'`
	cleaned := cleanSQLForTags(sql)

	// The word "INSERT" should be gone from the cleaned SQL
	assert.NotContains(t, cleaned, "INSERT")

	// Logic Check: Simulate the new first-word logic
	fields := strings.Fields(cleaned)
	isMutation := false
	if len(fields) > 0 {
		switch strings.ToUpper(fields[0]) {
		case "INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "REPLACE":
			isMutation = true
		}
	}

	assert.False(t, isMutation, "keyword inside a string literal at index > 0 should not trigger isMutation")
	assert.Equal(t, "SELECT", strings.ToUpper(fields[0]))
}
