package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

func structuredQueryRequest(t *testing.T, table string, sq query.StructuredQuery) *http.Request {
	t.Helper()
	body, _ := json.Marshal(sq)
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/tables/"+table+"/query", bytes.NewReader(body))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("table", table)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func newStructuredQueryHandler() *StructuredQueryHandler {
	reg := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{
			Name: "clicks",
			Columns: []discovery.Column{
				{Name: "page", Type: "String"},
				{Name: "count", Type: "UInt64"},
				{Name: "ts", Type: "DateTime"},
			},
		},
	})
	return NewStructuredQueryHandler(nil, nil, 5*time.Second, reg, nil, 60)
}

func TestStructuredQuery_MissingTable(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler()
	body, _ := json.Marshal(query.StructuredQuery{Columns: []string{"page"}})
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/tables//query", bytes.NewReader(body))
	rctx := chi.NewRouteContext()
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing table")
	assertJSONErrorResponse(t, w)
}

func TestStructuredQuery_UnknownTable(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler()
	r := structuredQueryRequest(t, "nope", query.StructuredQuery{Columns: []string{"x"}})
	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "unknown table")
	assertJSONErrorResponse(t, w)
}

func TestStructuredQuery_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/tables/clicks/query", bytes.NewReader([]byte(`{bad}`)))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("table", "clicks")
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
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
	h := newStructuredQueryHandler()
	h.PolicyStore = policy.NewMemoryStore(p)

	sq := query.StructuredQuery{Columns: []string{"page"}}
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "forbidden")
	assertJSONErrorResponse(t, w)
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
	h := newStructuredQueryHandler()
	h.PolicyStore = policy.NewMemoryStore(p)

	// Request "count" column which is not in AllowColumns.
	sq := query.StructuredQuery{Columns: []string{"count"}}
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "column")
	assert.Contains(t, w.Body.String(), "not allowed")
	assertJSONErrorResponse(t, w)
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
	h := newStructuredQueryHandler()
	h.PolicyStore = policy.NewMemoryStore(p)

	sq := query.StructuredQuery{
		Aggregations: []query.Aggregation{
			{Fn: "avg", Column: "count", Alias: "avg_count"},
		},
	}
	r := structuredQueryRequest(t, "clicks", sq)
	ctx := context.WithValue(r.Context(), ContextKeyRole, "viewer")
	ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()
	h.Handle(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "aggregation")
	assert.Contains(t, w.Body.String(), "not allowed")
	assertJSONErrorResponse(t, w)
}

func TestStructuredQuery_NoPolicyAllowsAll(t *testing.T) {
	t.Parallel()
	h := newStructuredQueryHandler()
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
