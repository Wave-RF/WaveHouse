package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/singleflight"
)

// StructuredQueryHandler handles POST /v1/tables/{table}/query.
type StructuredQueryHandler struct {
	CHConn      driver.Conn
	Cache       *cache.TieredCache
	DefaultTTL  time.Duration
	Registry    *discovery.SchemaRegistry
	PolicyStore *policy.Store
	BucketSecs  int
	sf          singleflight.Group
}

func NewStructuredQueryHandler(
	conn driver.Conn,
	c *cache.TieredCache,
	defaultTTL time.Duration,
	registry *discovery.SchemaRegistry,
	policyStore *policy.Store,
	bucketSecs int,
) *StructuredQueryHandler {
	return &StructuredQueryHandler{
		CHConn:      conn,
		Cache:       c,
		DefaultTTL:  defaultTTL,
		Registry:    registry,
		PolicyStore: policyStore,
		BucketSecs:  bucketSecs,
	}
}

func (h *StructuredQueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	if table == "" {
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	schema := h.Registry.Get(table)
	if schema == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("unknown table: %s", table))
		return
	}

	var sq query.StructuredQuery
	if err := json.NewDecoder(r.Body).Decode(&sq); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	// Resolve permissions.
	role := RoleFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())
	p := h.PolicyStore.Get()
	perms := policy.Evaluate(p, role, table, "select", claims)
	if !perms.Allowed {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}

	// Check column permissions.
	for _, col := range sq.Columns {
		if !perms.IsColumnAllowed(col) {
			writeJSONError(w, http.StatusForbidden, fmt.Sprintf("column %q not allowed", col))
			return
		}
	}
	for _, agg := range sq.Aggregations {
		if agg.Column != "*" && !perms.IsColumnAllowed(agg.Column) {
			writeJSONError(w, http.StatusForbidden, fmt.Sprintf("column %q not allowed", agg.Column))
			return
		}
		if !perms.IsAggregationAllowed(agg.Fn) {
			writeJSONError(w, http.StatusForbidden, fmt.Sprintf("aggregation %q not allowed", agg.Fn))
			return
		}
	}

	// Build SQL.
	result, err := query.Build(table, &sq, schema, h.BucketSecs)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Inject row-level security filters from policy.
	query.InjectPermissionFilters(result, perms.WhereClause, perms.WhereParams)

	// Apply resource limits.
	if perms.MaxRows > 0 {
		query.ApplyMaxRows(result, perms.MaxRows)
	}

	// Cache key.
	cacheKey := queryCacheKey(result.SQL, result.Params)
	ttl := h.DefaultTTL
	if sq.CacheTTL != nil && *sq.CacheTTL > 0 {
		ttl = time.Duration(*sq.CacheTTL) * time.Second
	}

	// Try cache.
	if h.Cache != nil {
		if data, _, err := h.Cache.Get(r.Context(), cacheKey); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	// Execute with singleflight.
	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
		timeout := 30 * time.Second
		if perms.MaxExecutionTimeMs > 0 {
			timeout = time.Duration(perms.MaxExecutionTimeMs) * time.Millisecond
		}

		queryCtx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		rows, err := executeCHQuery(queryCtx, h.CHConn, result.SQL, result.Params)
		if err != nil {
			return nil, err
		}

		data, err := json.Marshal(rows)
		if err != nil {
			return nil, err
		}

		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), cacheKey, data, ttl)
		}
		return data, nil
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(v.([]byte))
}
