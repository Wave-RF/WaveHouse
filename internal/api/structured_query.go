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
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"golang.org/x/sync/singleflight"
)

// StructuredQueryHandler handles POST /v1/query?table={table}
type StructuredQueryHandler struct {
	// CHConn yields the request tenant's connection (chconn.Pools.For in
	// production); nil is a tenant on no pool, a 503.
	CHConn       func(*settings.Store) driver.Conn
	Cache        cache.Cache
	Registry     RegistrySource
	PolicySource PolicySource
	sf           singleflight.Group
	// queryTimeout bounds each query, read per request off the tenant's
	// settings ((*settings.Store).ClickHouse().QueryTimeout in production)
	// so a settings reload applies without a restart.
	queryTimeout func(*settings.Store) time.Duration

	// bucketSecs returns the request tenant's current time-range bucket
	// ((*settings.Store).TimestampBucketSeconds in production) and
	// defaultMaxRows its current fallback result LIMIT
	// ((*settings.Store).DefaultMaxRows) — funcs, not ints, so a settings
	// reload takes effect on the next query without a restart. A nil
	// bucketSecs means no bucketing; a nil defaultMaxRows or a non-positive
	// return means the builder's compiled constant.
	bucketSecs     func(*settings.Store) int
	defaultMaxRows func(*settings.Store) int

	// maxRequestBytes optionally overrides the default inbound request body
	// cap (maxControlBodyBytes). When 0, the default applies. Exists so
	// same-package tests can pin the cap-overflow path without allocating
	// 1 MiB per run; not a production tuning knob, hence unexported. Mirrors
	// QueryHandler.
	maxRequestBytes int64
}

func NewStructuredQueryHandler(
	conn func(*settings.Store) driver.Conn,
	c cache.Cache,
	registry RegistrySource,
	policyStore PolicySource,
	bucketSecs func(*settings.Store) int,
	queryTimeout func(*settings.Store) time.Duration,
	defaultMaxRows func(*settings.Store) int,
) *StructuredQueryHandler {
	return &StructuredQueryHandler{
		CHConn:         conn,
		Cache:          c,
		Registry:       registry,
		PolicySource:   policyStore,
		bucketSecs:     bucketSecs,
		queryTimeout:   queryTimeout,
		defaultMaxRows: defaultMaxRows,
	}
}

func (h *StructuredQueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	store, ok := requestStore(w, r)
	if !ok {
		return
	}
	table := r.URL.Query().Get("table")
	if table == "" {
		writeJSONError(w, http.StatusBadRequest, "missing table")
		return
	}

	schema, err := lookupSchema(w, h.Registry, store, table, "unknown table: "+table)
	if err != nil {
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
	p := h.PolicySource(store)
	role := policy.ResolveRole(p, auth.RoleFromContext(r.Context()))
	claims, _ := auth.ClaimsFromContext(r.Context())
	perms := policy.Evaluate(p, role, table, "select", claims)
	if !perms.Allowed {
		writeAuthzDenied(w, r, role, nil,
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
	// skip the check. The role's row-filter predicate and max_rows cap are emitted
	// by Build too, structurally (#322). A policy denial returns a typed error we
	// map to 403; a malformed query maps to 400.
	maxRows := 0 // non-positive → the builder's compiled constant
	if h.defaultMaxRows != nil {
		maxRows = h.defaultMaxRows(store)
	}
	bucketSecs := 0
	if h.bucketSecs != nil {
		bucketSecs = h.bucketSecs(store)
	}
	result, err := query.Build(table, &sq, schema, perms, bucketSecs, maxRows)
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

	// The tenant's pool, ahead of the cache: a tenant on none — its tuple
	// could not be opened, such as by the connection ceiling — fails
	// closed rather than serve what it cached before (#583 story 6).
	conn := connOf(h.CHConn, store)
	if conn == nil {
		writeUnavailable(w, noConnectionMessage, retryAfterPool)
		return
	}

	// Cache key, led by the tenant the store was resolved for (#583 story 8);
	// the singleflight key too.
	cacheKey := queryCacheKey(store.Tenant(), result.SQL, result.Params)

	// TODO: impl scope
	scope := ""
	safeTableName := query.SafeEncodeToken(table)
	// A structured query reads one table, so it depends on a single namespace:
	// the request's tenant, the table, the scope. Encode the scope the way the
	// ingest worker does (worker.go invalidate) so the read and invalidation
	// sides build identical namespace keys once scope is implemented;
	// SafeEncodeToken("") is "", so this is a no-op while scope is empty.
	deps := []cache.Namespace{{Tenant: store.Tenant(), Table: safeTableName, Scope: query.SafeEncodeToken(scope)}}

	// Try cache. The snapshot is of the versions before the query runs, so a
	// write landing mid-query orphans the fill (#382).
	var snap cache.Snapshot
	if h.Cache != nil {
		var entry cache.Entry
		if entry, snap, _ = h.Cache.Lookup(r.Context(), store.Tenant(), cacheKey, deps); entry.Value != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(entry.Value)
			return
		}
	}

	// Execute with singleflight.
	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
		timeout := timeoutOf(h.queryTimeout, store)
		// Bare Select reads: this handler resolved the grant for "select" (above),
		// so Select is non-nil, and query.Build has already rejected a mis-resolved
		// grant before this closure runs. If that changed, these would panic rather
		// than silently apply no caps — do not add a nil guard, which would drop the
		// limits instead.
		if perms.Select.MaxExecutionTime > 0 {
			timeout = min(perms.Select.MaxExecutionTime.Duration(), timeout)
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
			MaxResultRows:  perms.Select.MaxRows,
			MaxRowsToRead:  perms.Select.MaxRowsToRead,
			MaxMemoryBytes: perms.Select.MaxMemoryUsage.Bytes(),
		}
		if perms.Select.MaxExecutionTime > 0 {
			limits.ExecutionTime = timeout
		}
		if settings := chReadSettings(limits); settings != nil {
			queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(settings))
		}

		start := time.Now()

		rows, err := executeCHQuery(queryCtx, conn, result.SQL, result.Params)
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
			_ = h.Cache.Set(r.Context(), snap, data, ttl)
		}
		return data, nil
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(v.([]byte)) //nolint:gosec // G705: the tenant id on the key only selects the entry; the bytes are JSON the handler marshalled from ClickHouse rows
}
