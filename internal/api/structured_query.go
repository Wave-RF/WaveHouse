package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"golang.org/x/sync/singleflight"
)

// StructuredQueryHandler handles POST /v1/query?table={table}
type StructuredQueryHandler struct {
	CHConn          driver.Conn
	Cache           cache.Cache
	Registry        *discovery.SchemaRegistry
	PolicyStore     *policy.Store
	BucketSecs      int
	sf              singleflight.Group
	maxQueryTimeout time.Duration
	logger          *slog.Logger
}

func NewStructuredQueryHandler(
	conn driver.Conn,
	c cache.Cache,
	registry *discovery.SchemaRegistry,
	policyStore *policy.Store,
	bucketSecs int,
	queryTimeout time.Duration,
	logger *slog.Logger,
) *StructuredQueryHandler {
	return &StructuredQueryHandler{
		CHConn:          conn,
		Cache:           c,
		Registry:        registry,
		PolicyStore:     policyStore,
		BucketSecs:      bucketSecs,
		maxQueryTimeout: queryTimeout,
		logger:          logger,
	}
}

func (h *StructuredQueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	if table == "" {
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	schema := h.Registry.Get(table)
	if schema == nil {
		writeJSONError(w, http.StatusNotFound, "unknown table: "+table)
		return
	}

	var sq query.StructuredQuery
	if err := json.NewDecoder(r.Body).Decode(&sq); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	// Resolve permissions.
	p := h.PolicyStore.Get()
	role := policy.ResolveRole(p, auth.RoleFromContext(r.Context()))
	claims, _ := auth.ClaimsFromContext(r.Context())
	perms := policy.Evaluate(p, role, table, "select", claims)
	if !perms.Allowed {
		writeAuthzDenied(w, r, h.logger, role, nil,
			slog.String("gate", "policy"),
			slog.String("table", table),
			slog.String("action", "select"),
		)
		return
	}

	// Build SQL. The builder is the single chokepoint that validates every column
	// reference against the schema AND authorizes it against perms — per-column
	// allow/deny, aggregation policy, and the SELECT * → allowed-columns expansion
	// that closes the omitted/"*" column bypass (#223) all live there, so no
	// clause (columns, aggregations, filters, group_by, order_by, time_range) can
	// skip the check. A policy denial returns a typed error we map to 403; a
	// malformed query maps to 400.
	result, err := query.Build(table, &sq, schema, perms, h.BucketSecs)
	if err != nil {
		// A query that selects nothing — no columns, no aggregations, no
		// select_all — is a request for no data, not an error: return an empty
		// result. Authorization already passed above, so this leaks nothing.
		if errors.Is(err, query.ErrEmptyProjection) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		var forbiddenCol *query.ForbiddenColumnError
		var forbiddenAgg *query.ForbiddenAggregationError
		switch {
		case errors.As(err, &forbiddenCol), errors.As(err, &forbiddenAgg), errors.Is(err, query.ErrNoReadableColumns):
			writeJSONError(w, http.StatusForbidden, err.Error())
		default:
			// Malformed query — unknown column, bad operator, columns+select_all,
			// '?' in an identifier, etc.
			writeJSONError(w, http.StatusBadRequest, err.Error())
		}
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

	// TODO: impl scope
	scope := ""
	safeTableName := query.SafeEncodeNATS(table)
	// A structured query reads one table, so it depends on a single namespace.
	deps := []cache.Namespace{{Table: safeTableName, Scope: scope}}

	// Try cache.
	if h.Cache != nil {
		if data, _, err := h.Cache.Get(r.Context(), cacheKey, deps); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data) //nolint:gosec // G705 XSS only JSON
			return
		}
	}

	// Execute with singleflight.
	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
		timeout := h.maxQueryTimeout
		if perms.MaxExecutionTimeMs > 0 {
			timeout = min(time.Duration(perms.MaxExecutionTimeMs)*time.Millisecond, timeout)
		}

		queryCtx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		start := time.Now()

		rows, err := executeCHQuery(queryCtx, h.CHConn, result.SQL, result.Params)
		queryDuration := time.Since(start)
		if err != nil {
			// TODO: depending on the error, we may actually want to cache it
			return nil, err
		}

		data, err := json.Marshal(rows)
		if err != nil {
			// TODO: eventually we want CSV support etc
			return nil, err
		}

		ttl := cache.QueryTimeToTTL(queryDuration)

		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), cacheKey, deps, data, ttl)
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
