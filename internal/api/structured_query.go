package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
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
	defaultMaxRows  int
	logger          *slog.Logger

	// maxRequestBytes optionally overrides the default inbound request body
	// cap (maxControlBodyBytes). When 0, the default applies. Exists so
	// same-package tests can pin the cap-overflow path without allocating
	// 1 MiB per run; not a production tuning knob, hence unexported. Mirrors
	// QueryHandler.
	maxRequestBytes int64
}

func NewStructuredQueryHandler(
	conn driver.Conn,
	c cache.Cache,
	registry *discovery.SchemaRegistry,
	policyStore *policy.Store,
	bucketSecs int,
	queryTimeout time.Duration,
	defaultMaxRows int,
	logger *slog.Logger,
) *StructuredQueryHandler {
	return &StructuredQueryHandler{
		CHConn:          conn,
		Cache:           c,
		Registry:        registry,
		PolicyStore:     policyStore,
		BucketSecs:      bucketSecs,
		maxQueryTimeout: queryTimeout,
		defaultMaxRows:  defaultMaxRows,
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

	// Bound the inbound body before decoding it. The query AST is a small,
	// bounded description, but a JSON array of many tiny elements (e.g. a huge
	// `in`-list value) amplifies ~13× bytes→live-heap when decoded, so an
	// uncapped decoder on this public endpoint is a single-request OOM vector
	// (#315). 413 (not 400) tells a caller "too much", distinct from "garbage".
	reqCap := int64(maxControlBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)

	var sq query.StructuredQuery
	if err := json.NewDecoder(r.Body).Decode(&sq); err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
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
	result, err := query.Build(table, &sq, schema, perms, h.BucketSecs, h.defaultMaxRows)
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
	safeTableName := chsql.SafeEncodeNATS(table)
	// A structured query reads one table, so it depends on a single namespace.
	// Encode the scope the way the ingest worker does (worker.go handleSuccess) so
	// the read and invalidation sides build identical namespace keys once scope is
	// implemented; SafeEncodeNATS("") is "", so this is a no-op while scope is empty.
	deps := []cache.Namespace{{Table: safeTableName, Scope: chsql.SafeEncodeNATS(scope)}}

	// Snapshot the version-folded key once, before the query runs, and reuse it
	// for the Get, the singleflight, and the Set (the Cache.Key contract). Keying
	// the singleflight on the folded key (not the bare sha) also keeps a post-bump
	// request from coalescing onto a pre-bump flight.
	entryKey := cacheKey
	if h.Cache != nil {
		entryKey = h.Cache.Key(cacheKey, deps)
		if data, _, err := h.Cache.Get(r.Context(), entryKey); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	// Execute with singleflight.
	v, err, _ := h.sf.Do(entryKey, func() (interface{}, error) {
		timeout := h.maxQueryTimeout
		if perms.MaxExecutionTime > 0 {
			timeout = min(perms.MaxExecutionTime.Duration(), timeout)
		}

		queryCtx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		// Enforce the role's resource caps server-side, not just via the client
		// context deadline (#316). The settings ride on the query context, so they
		// reach ClickHouse for this query only. Server-wide backstops are
		// ClickHouse's job (settings profiles / quotas); a role with no caps (e.g.
		// admin) sends nothing here. An explicit max_execution_time is sent only
		// when the role set a time cap; otherwise the context deadline (=
		// query_timeout) is the time bound the driver derives.
		limits := chQueryLimits{
			MaxResultRows:  perms.MaxRows,
			MaxRowsToRead:  perms.MaxRowsToRead,
			MaxMemoryBytes: perms.MaxMemoryUsage.Bytes(),
		}
		if perms.MaxExecutionTime > 0 {
			limits.ExecutionTime = timeout
		}
		if settings := chReadSettings(limits); settings != nil {
			queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(settings))
		}

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
		if !h.Registry.IsKnown(table) {
			// The name resolved a schema at the top of Handle, but it's an UNFOLDABLE
			// view the refresh-time cascade never bumps. Same rule as pipes: cap the
			// TTL (cache.UnresolvedDepsTTLCap) so the result self-expires. A base
			// table or foldable view is always known, so the happy path is untouched.
			ttl = min(ttl, cache.UnresolvedDepsTTLCap)
		}

		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), entryKey, data, ttl)
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
