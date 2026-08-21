package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func structuredQueryRequest(t *testing.T, table string, sq query.StructuredQuery) *http.Request {
	t.Helper()
	body, err := json.Marshal(sq)
	require.NoError(t, err)

	return httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query?table="+url.QueryEscape(table), bytes.NewReader(body))
}

func newStructuredQueryHandler(t testing.TB) *StructuredQueryHandler {
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{
			Name: "clicks",
			Columns: []discovery.Column{
				{Name: "page", Type: "String"},
				{Name: "count", Type: "UInt64"},
				{Name: "ts", Type: "DateTime"},
			},
		},
	})
	return NewStructuredQueryHandler(nil, nil, reg, nil, 60, 5*time.Second, nil, testutil.NopLogger())
}

func TestStructuredQuery_MissingTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{
			name: "no query string at all",
			url:  "/v1/query",
		},
		{
			name: "trailing slash without query",
			url:  "/v1/query/",
		},
		{
			name: "empty query symbol",
			url:  "/v1/query?",
		},
		{
			name: "table parameter provided but empty",
			url:  "/v1/query?table=",
		},
		{
			name: "completely wrong query parameter",
			url:  "/v1/query?invalid_param=clicks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newStructuredQueryHandler(t)

			body, err := json.Marshal(query.StructuredQuery{Columns: []string{"page"}})
			require.NoError(t, err)

			req := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				tt.url,
				bytes.NewReader(body),
			)

			w := httptest.NewRecorder()
			h.Handle(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "missing table")
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestStructuredQuery_UnknownTable(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler(t)
	r := structuredQueryRequest(t, "nope", query.StructuredQuery{Columns: []string{"x"}})
	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "unknown table")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestStructuredQuery_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler(t)
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query?table=clicks", bytes.NewReader([]byte(`{bad}`)))
	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	testutil.AssertJSONErrorResponse(t, w)
}

// TestStructuredQuery_RequestBodyCap pins the control-plane body cap on the
// public read path. The handler wraps r.Body in http.MaxBytesReader before the
// decode (#315), so a well-formed-but-oversized query returns 413 — distinct
// from "invalid json", and well before the JSON array amplifies in the decoder.
// maxRequestBytes is set to a tiny value so we don't allocate 1 MiB per run.
func TestStructuredQuery_RequestBodyCap(t *testing.T) {
	t.Parallel()

	const testCap = 64
	h := newStructuredQueryHandler(t)
	h.maxRequestBytes = testCap

	// A valid query whose JSON exceeds the cap — a big `in`-list, the exact
	// array-amplification vector from #315. 413 (not 400) proves the cap fired.
	sq := query.StructuredQuery{
		Filters: []query.Filter{{Column: "page", Op: "in", Value: make([]int, 100)}},
	}
	body, err := json.Marshal(sq)
	require.NoError(t, err)
	require.Greater(t, len(body), testCap, "test body must exceed the cap")

	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/query?table=clicks", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Handle(w, r)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, "oversized request must 413, not 400")
	assert.Contains(t, w.Body.String(), "request body exceeded")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestStructuredQuery_PolicyForbidden(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"admin": {AllowColumns: []string{"page"}},
				},
			},
		},
	}
	h := newStructuredQueryHandler(t)
	h.PolicyStore = policy.NewMemoryStore(p)

	sq := query.StructuredQuery{Columns: []string{"page"}}
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := auth.WithRole(r.Context(), "viewer")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "forbidden")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestStructuredQuery_ColumnNotAllowed(t *testing.T) {
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
	h := newStructuredQueryHandler(t)
	h.PolicyStore = policy.NewMemoryStore(p)

	// Request "count" column which is not in AllowColumns.
	sq := query.StructuredQuery{Columns: []string{"count"}}
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := auth.WithRole(r.Context(), "viewer")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "column")
	assert.Contains(t, w.Body.String(), "not allowed")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestStructuredQuery_AggregationNotAllowed(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {
						AllowColumns:       []string{"page", "count"},
						DeniedAggregations: []string{"avg"},
					},
				},
			},
		},
	}
	h := newStructuredQueryHandler(t)
	h.PolicyStore = policy.NewMemoryStore(p)

	sq := query.StructuredQuery{
		Aggregations: []query.Aggregation{
			{Fn: "avg", Column: "count", Alias: "avg_count"},
		},
	}
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := auth.WithRole(r.Context(), "viewer")
	ctx = auth.WithClaims(ctx, jwt.MapClaims{})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "aggregation")
	assert.Contains(t, w.Body.String(), "not allowed")
	testutil.AssertJSONErrorResponse(t, w)
}

func TestStructuredQuery_NoPolicyAllowsAll(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler(t)
	// No PolicyStore — all queries should be allowed (past policy).
	sq := query.StructuredQuery{Columns: []string{"page"}}
	r := structuredQueryRequest(t, "clicks", sq)
	w := httptest.NewRecorder()
	safeHandle(h.Handle, w, r)

	// Will fail at executeQuery (nil CH conn) but should get past the policy checks.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
	assert.NotEqual(t, http.StatusNotFound, w.Code)
}

// ─── #223: column allowlist is a hard cap on every read, end-to-end ──────────

// sqlCapturingConn records the SQL (and bound args) the handler hands to
// ClickHouse so tests can assert the generated query without a live database.
// Query returns an empty result set (the handler marshals it to []); these
// tests assert on the SQL string, args, and HTTP status, not on rows. lastSQL
// stays empty when the request is rejected before execution — which is itself
// the assertion for denied paths.
type sqlCapturingConn struct {
	driver.Conn
	lastSQL  string
	lastArgs []any
}

func (c *sqlCapturingConn) Query(_ context.Context, sql string, args ...any) (driver.Rows, error) {
	c.lastSQL = sql
	c.lastArgs = args
	return &chainEmptyRows{}, nil
}

// sensitiveSchema has a column (payload, user_id) that restrictive policies hide.
func newCapturingHandler(t *testing.T, conn driver.Conn, p *policy.Policy) *StructuredQueryHandler {
	t.Helper()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{
			Name: "clicks",
			Columns: []discovery.Column{
				{Name: "page", Type: "String"},
				{Name: "user_id", Type: "String"},
				{Name: "payload", Type: "String"},
				{Name: "ts", Type: "DateTime"},
			},
		},
	})
	return NewStructuredQueryHandler(conn, nil, reg, policy.NewMemoryStore(p), 60, 5*time.Second, nil, testutil.NopLogger())
}

func viewerRequest(t *testing.T, sq query.StructuredQuery) *http.Request {
	t.Helper()
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := auth.WithClaims(auth.WithRole(r.Context(), "viewer"), jwt.MapClaims{})
	return r.WithContext(ctx)
}

func policyWithViewer(perms policy.RolePermissions) *policy.Policy {
	return &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {Select: map[string]policy.RolePermissions{"viewer": perms}},
		},
	}
}

// TestStructuredQuery_SelectAll_RestrictedRoleGetsAllowedProjection is the #223
// regression: select_all for a column-restricted role must NOT become a raw
// SELECT *. It expands to exactly the role's allowed columns, so the denied
// payload/user_id never reach ClickHouse — let alone the client.
func TestStructuredQuery_SelectAll_RestrictedRoleGetsAllowedProjection(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{AllowColumns: []string{"page", "ts"}}))

	w := httptest.NewRecorder()
	h.Handle(w, viewerRequest(t, query.StructuredQuery{SelectAll: true}))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, "SELECT `page`, `ts` FROM `clicks` LIMIT 10000", conn.lastSQL)
	assert.NotContains(t, conn.lastSQL, "*")
	assert.NotContains(t, conn.lastSQL, "payload")
	assert.NotContains(t, conn.lastSQL, "user_id")
}

// TestStructuredQuery_RowFilterAndMaxRows_ReachClickHouse pins the handler seam
// #322 rewired: the role's row-filter predicate and max_rows cap now reach
// ClickHouse only through Build itself — the handler no longer post-edits the
// built SQL — so if perms ever stopped flowing into Build, nothing downstream
// would re-add them. Asserts the predicate leads both the WHERE clause and the
// bound args (policy value before the caller's filter value) and that the
// role's cap replaces the default LIMIT.
func TestStructuredQuery_RowFilterAndMaxRows_ReachClickHouse(t *testing.T) {
	t.Parallel()
	eq := "{{ jwt.org_id }}"
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{
		Filter:  map[string]policy.Filter{"user_id": {Eq: &eq}},
		MaxRows: 100,
	}))

	r := structuredQueryRequest(t, "clicks", query.StructuredQuery{
		Columns: []string{"page"},
		Filters: []query.Filter{{Column: "page", Op: "eq", Value: "/home"}},
	})
	ctx := auth.WithClaims(auth.WithRole(r.Context(), "viewer"), jwt.MapClaims{"org_id": "org-1"})

	w := httptest.NewRecorder()
	h.Handle(w, r.WithContext(ctx))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, "SELECT `page` FROM `clicks` WHERE (`user_id` = ?) AND `page` = ? LIMIT 100", conn.lastSQL)
	assert.Equal(t, []any{"org-1", "/home"}, conn.lastArgs)
}

// TestStructuredQuery_OmittedColumns_ReturnsNothing pins safe-by-default: a request
// with no columns, no aggregations, and no select_all asks for no data, so it
// returns 200 [] and never reaches ClickHouse. A hidden column can't leak by
// simply leaving columns out.
func TestStructuredQuery_OmittedColumns_ReturnsNothing(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{AllowColumns: []string{"page", "ts"}}))

	w := httptest.NewRecorder()
	h.Handle(w, viewerRequest(t, query.StructuredQuery{}))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.JSONEq(t, "[]", w.Body.String())
	assert.Empty(t, conn.lastSQL, "an empty projection must not reach ClickHouse")
}

// TestStructuredQuery_SelectAll_DenyListExpands: select_all under a deny-list
// (empty allow) expands to the non-denied columns, never a raw SELECT *.
func TestStructuredQuery_SelectAll_DenyListExpands(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{DenyColumns: []string{"payload"}}))

	w := httptest.NewRecorder()
	h.Handle(w, viewerRequest(t, query.StructuredQuery{SelectAll: true}))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, "SELECT `page`, `user_id`, `ts` FROM `clicks` LIMIT 10000", conn.lastSQL)
	assert.NotContains(t, conn.lastSQL, "payload")
}

// TestStructuredQuery_LiteralStarColumn_Unknown: columns:["*"] is a literal column
// name now, not a wildcard. clicks has no column named "*", so it is a 400 unknown
// column — the all-columns wildcard is select_all.
func TestStructuredQuery_LiteralStarColumn_Unknown(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{AllowColumns: []string{"*"}}))

	w := httptest.NewRecorder()
	h.Handle(w, viewerRequest(t, query.StructuredQuery{Columns: []string{"*"}}))

	require.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
	assert.Empty(t, conn.lastSQL)
}

// TestStructuredQuery_UnrestrictedRoleKeepsSelectStar proves the common case is
// untouched: a role allowed all columns still gets SELECT * (special-character
// columns and admin convenience preserved; no behavior change off the hot path).
func TestStructuredQuery_UnrestrictedRoleKeepsSelectStar(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{AllowColumns: []string{"*"}}))

	w := httptest.NewRecorder()
	h.Handle(w, viewerRequest(t, query.StructuredQuery{SelectAll: true}))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, "SELECT * FROM `clicks` LIMIT 10000", conn.lastSQL)
}

// TestStructuredQuery_DeniedColumnInAnyClause_Returns403 is the regression for
// the recon-found siblings of #223: a denied column referenced via group_by,
// filter, or order_by leaked data even though it was never in the SELECT list.
// Every clause must now reject it with 403 before any SQL reaches ClickHouse.
func TestStructuredQuery_DeniedColumnInAnyClause_Returns403(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sq   query.StructuredQuery
	}{
		{"projection", query.StructuredQuery{Columns: []string{"payload"}}},
		{
			"group_by enumerates distinct denied values",
			query.StructuredQuery{
				Aggregations: []query.Aggregation{{Fn: "count", Column: "*", Alias: "n"}},
				GroupBy:      []string{"payload"},
			},
		},
		{
			"filter as a value-inference oracle",
			query.StructuredQuery{
				Columns: []string{"page"},
				Filters: []query.Filter{{Column: "payload", Op: "eq", Value: "secret"}},
			},
		},
		{
			"order_by leaks ordering",
			query.StructuredQuery{
				Columns: []string{"page"},
				OrderBy: []query.OrderClause{{Column: "payload", Dir: "desc"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn := &sqlCapturingConn{}
			h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{AllowColumns: []string{"page", "ts"}}))

			w := httptest.NewRecorder()
			h.Handle(w, viewerRequest(t, tt.sq))

			assert.Equal(t, http.StatusForbidden, w.Code, "body=%s", w.Body.String())
			assert.Contains(t, w.Body.String(), "not allowed")
			assert.Empty(t, conn.lastSQL, "a denied query must never reach ClickHouse")
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

// TestStructuredQuery_NoReadableColumns_Returns403 covers the degenerate policy
// where a role may select the table but no columns: fail closed with 403, never
// a fail-open SELECT *.
func TestStructuredQuery_NoReadableColumns_Returns403(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.RolePermissions{AllowColumns: []string{"nonexistent"}}))

	w := httptest.NewRecorder()
	h.Handle(w, viewerRequest(t, query.StructuredQuery{SelectAll: true}))

	assert.Equal(t, http.StatusForbidden, w.Code, "body=%s", w.Body.String())
	assert.Empty(t, conn.lastSQL)
	testutil.AssertJSONErrorResponse(t, w)
}

// TestStructuredQuery_UnauthenticatedUsesDefaultRoleProjection mirrors the
// reported exploit: an unauthenticated `{"limit":2}` resolves to default_role
// and must get only that role's columns, not every column.
func TestStructuredQuery_UnauthenticatedUsesDefaultRoleProjection(t *testing.T) {
	t.Parallel()
	conn := &sqlCapturingConn{}
	p := policyWithViewer(policy.RolePermissions{AllowColumns: []string{"page"}})
	p.DefaultRole = "viewer" // public access resolves to the restricted viewer role
	h := newCapturingHandler(t, conn, p)

	// No role on the context — a tokenless request.
	r := structuredQueryRequest(t, "clicks", query.StructuredQuery{SelectAll: true, Limit: 2})
	w := httptest.NewRecorder()
	h.Handle(w, r)

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	assert.Equal(t, "SELECT `page` FROM `clicks` LIMIT 2", conn.lastSQL)
	assert.NotContains(t, conn.lastSQL, "payload")
}

// TestStructuredQuery_CacheKeyIsolatesColumnVisibility pins the cache-isolation
// property that the projection fix gives us for free: two roles that may see
// different columns now emit different SQL for the same omitted-columns request,
// so the cache key (derived from that SQL) differs and the narrower role can
// never be served the wider role's cached row. Before the fix both emitted the
// same SELECT * under one key.
func TestStructuredQuery_CacheKeyIsolatesColumnVisibility(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {Select: map[string]policy.RolePermissions{
			"viewer":  {AllowColumns: []string{"page"}},
			"auditor": {AllowColumns: []string{"page", "user_id"}},
		}},
	}}
	sqlFor := func(role string) string {
		conn := &sqlCapturingConn{}
		h := newCapturingHandler(t, conn, p)
		r := structuredQueryRequest(t, "clicks", query.StructuredQuery{SelectAll: true})
		r = r.WithContext(auth.WithClaims(auth.WithRole(r.Context(), role), jwt.MapClaims{}))
		w := httptest.NewRecorder()
		h.Handle(w, r)
		require.Equal(t, http.StatusOK, w.Code, "role=%s body=%s", role, w.Body.String())
		return conn.lastSQL
	}
	viewerSQL, auditorSQL := sqlFor("viewer"), sqlFor("auditor")
	assert.NotEqual(t, viewerSQL, auditorSQL)
	assert.NotEqual(t, queryCacheKey(viewerSQL, nil), queryCacheKey(auditorSQL, nil),
		"roles with different column visibility must not share a cache key")
}
