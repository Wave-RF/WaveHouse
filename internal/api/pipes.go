package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"
)

const maxPipeDeps = 50000

var (
	pipeFallbackCounter, _ = otel.Meter("wavehouse-pipes").Int64Counter(
		"wavehouse_pipe_dep_resolution_fallback_total",
		metric.WithDescription("Pipe executions served on the database-version fallback (any write evicts) because ClickHouse couldn't analyze the query (a write/DDL pipe, a missing table, an unreachable server)"),
	)
	pipeUnresolvedCounter, _ = otel.Meter("wavehouse-pipes").Int64Counter(
		"wavehouse_pipe_dep_unresolved_total",
		metric.WithDescription("Pipe executions TTL-capped because the result can't be reliably version-invalidated (an unfoldable view, a cross-database/unknown name, an external read like a table function, or a dead-branch-pruned dependency set)"),
	)
	pipeDepsEvictionCounter, _ = otel.Meter("wavehouse-pipes").Int64Counter(
		"wavehouse_pipe_dep_memo_evictions_total",
		metric.WithDescription("Resolved-dependency memo entries evicted because the memo hit its size cap (maxPipeDeps); sustained growth means a high-cardinality pipe parameter"),
	)
)

type resolvedDeps struct {
	tables []string
	// fallback is set when EXPLAIN couldn't be used (a write/DDL pipe, a missing
	// table, an unreachable ClickHouse), in which case the pipe folds the single
	// DATABASE-version namespace (cache.DatabaseNamespace): any write bumps it, so
	// any write evicts the result — never an under-resolution, and O(1) per request
	// instead of folding every base table.
	fallback bool
	// unresolved is set when EXPLAIN succeeded but the result can't be reliably
	// version-invalidated, so the Execute path TTL-caps it
	// (cache.UnresolvedDepsTTLCap) instead of trusting eviction. Three triggers,
	// logged individually at resolve time: a resolved dependency whose version the
	// cascade never bumps (an unfoldable view or unknown name, per
	// SchemaRegistry.IsKnown), an external read no table version can watch (a
	// table function, a cross-database table, a non-local dictionary —
	// discovery.Resolution.External), and a dead-branch-pruned dependency set
	// (discovery.Resolution.Pruned, a belt-and-suspenders bound on a hypothetical
	// wrong prune). Distinct from fallback, whose database-version namespace is
	// always maintained.
	unresolved bool
}

// PipesHandler handles named query pipe endpoints.
type PipesHandler struct {
	Store           *pipes.Store
	PolicyStore     *policy.Store // resolves empty role to default_role; may be nil
	CHConn          driver.Conn
	Cache           cache.Cache
	sf              singleflight.Group
	maxQueryTimeout time.Duration
	logger          *slog.Logger
	registry        *discovery.SchemaRegistry
	resolveTablesFn func(ctx context.Context, sql string) (discovery.Resolution, error)
	// resolveSF coalesces concurrent cold-miss resolutions per bound SQL — one
	// EXPLAIN, every waiter shares the outcome. Distinct from sf, which keys on
	// the folded cache key and coalesces query EXECUTION.
	resolveSF singleflight.Group
	// pipeDeps memoizes, per bound query, which cache namespaces gate the pipe —
	// the version-key inputs Cache.Key folds. Guarded by pipeDepsMu (RWMutex: the
	// steady-state read on every Execute takes only the read lock). pipeDepsGen
	// increments on every ClearResolvedDeps so a resolution that RAN against a
	// pre-refresh schema is never inserted after the refresh wiped the memo (the
	// insert re-checks the generation it started under).
	pipeDeps    map[string]*resolvedDeps
	pipeDepsGen uint64
	pipeDepsMu  sync.RWMutex

	// maxRequestBytes optionally overrides the default inbound request body
	// cap (maxControlBodyBytes) for the body-decoding paths (Put, Execute).
	// When 0, the default applies. Test-only seam (pin the cap-overflow path
	// without allocating 1 MiB per run); not a production knob. Mirrors
	// StructuredQueryHandler / QueryHandler.
	maxRequestBytes int64
}

func NewPipesHandler(store *pipes.Store, policyStore *policy.Store, conn driver.Conn, c cache.Cache, registry *discovery.SchemaRegistry, queryTimeout time.Duration, logger *slog.Logger) *PipesHandler {
	h := &PipesHandler{
		Store:           store,
		PolicyStore:     policyStore,
		CHConn:          conn,
		Cache:           c,
		registry:        registry,
		maxQueryTimeout: queryTimeout,
		logger:          logger,
		pipeDeps:        make(map[string]*resolvedDeps),
	}
	// Resolve a pipe's tables by asking ClickHouse (EXPLAIN QUERY TREE). Overridable
	// in tests; the database comes from the Registry.
	h.resolveTablesFn = func(ctx context.Context, sql string) (discovery.Resolution, error) {
		return discovery.ResolveTables(ctx, h.CHConn, h.registry.Database(), sql)
	}
	return h
}

// List returns all named queries (admin endpoint).
func (h *PipesHandler) List(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := h.Store.List()
	if q == nil {
		q = []*pipes.NamedQuery{}
	}
	_ = json.NewEncoder(w).Encode(q)
}

// Get returns a specific named query (admin endpoint).
func (h *PipesHandler) Get(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	q := h.Store.Get(name)
	if q == nil {
		writeJSONError(w, http.StatusNotFound, "pipe not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(q)
}

// Put creates or updates a named query (admin endpoint).
func (h *PipesHandler) Put(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	reqCap := int64(maxControlBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)

	var q pipes.NamedQuery
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	q.Name = name

	if err := h.Store.Put(r.Context(), &q); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// Delete removes a named query (admin endpoint).
func (h *PipesHandler) Delete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.Store.Delete(r.Context(), name); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// Execute runs a named query with the provided parameters.
func (h *PipesHandler) Execute(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	q := h.Store.Get(name)
	if q == nil {
		writeJSONError(w, http.StatusNotFound, "pipe not found")
		return
	}

	// Authorization is allowlist membership (policy.RoleAllowed): the caller's
	// role — a tokenless or roleless request first mapped to the configured
	// default_role — must appear in allowed_roles by exact match. There is no
	// "*" any-role wildcard and empty entries are ignored, so a stray "" can't
	// authorize an empty role. The admin role bypasses every pipe's allowlist by
	// design, not oversight: admins author pipes and can run arbitrary SQL via
	// /v1/admin/query, so allowed_roles is never a confidentiality boundary
	// against them (mirrors Evaluate's admin bypass). A pipe with no
	// allowed_roles therefore authorizes nobody but admin (fails closed).
	var p *policy.Policy
	if h.PolicyStore != nil {
		p = h.PolicyStore.Get()
	}
	role := policy.ResolveRole(p, auth.RoleFromContext(r.Context()))
	if !policy.RoleAllowed(p, role, q.AllowedRoles) {
		writeAuthzDenied(w, r, h.logger, role, q.AllowedRoles,
			slog.String("gate", "pipe"),
			slog.String("pipe", q.Name),
		)
		return
	}

	// Gather parameters from query string and/or JSON body.
	supplied := make(map[string]any)
	for key, vals := range r.URL.Query() {
		if len(vals) > 0 {
			supplied[key] = vals[0]
		}
	}
	if r.Method == http.MethodPost {
		reqCap := int64(maxControlBodyBytes)
		if h.maxRequestBytes > 0 {
			reqCap = h.maxRequestBytes
		}
		r.Body = http.MaxBytesReader(w, r.Body, reqCap)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			// An oversized body is a hard stop: MaxBytesReader truncated it
			// mid-decode, so the parameters can't be trusted — surface 413
			// (parity with /v1/query and the ingest/admin handlers). Any other
			// decode error keeps the historical lenient behavior: parameters may
			// legitimately come from the query string alone, so a malformed or
			// empty body falls through to those rather than failing the request.
			if writeMaxBytesError(w, err, reqCap) {
				return
			}
		} else {
			for k, v := range body {
				supplied[k] = v
			}
		}
	}

	sql, params, err := pipes.BindParams(q, supplied)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	cacheKey := queryCacheKey(sql, params)

	deps, depsUnresolved := h.resolveDeps(r.Context(), sql)

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

	v, err, _ := h.sf.Do(entryKey, func() (interface{}, error) {
		queryCtx, cancel := context.WithTimeout(r.Context(), h.maxQueryTimeout)
		defer cancel()

		start := time.Now()
		rows, err := executeCHQuery(queryCtx, h.CHConn, sql, params)
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

		if h.Cache != nil {
			ttl := cache.QueryTimeToTTL(queryDuration)
			if depsUnresolved {
				// The result can't be reliably version-invalidated — an unfoldable
				// view, an external read (table function/cross-database), or a
				// dead-branch-pruned set (belt-and-suspenders): cap the TTL so it
				// self-expires rather than serving stale on a write the folded
				// versions never see.
				ttl = min(ttl, cache.UnresolvedDepsTTLCap)
			}
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

// resolveOutcome carries a coalesced resolution out of resolveSF: the resolved
// deps, whether they may be memoized (see resolvePipe), and the memo generation
// the resolution STARTED under (see the insert gate in resolveDeps).
type resolveOutcome struct {
	rd      *resolvedDeps
	memoize bool
	gen     uint64
}

// resolveDeps returns the cache namespaces a pipe's result depends on, by asking
// ClickHouse which tables the bound query reads (EXPLAIN QUERY TREE, via
// resolveTablesFn), memoized per bound query. A table EXPLAIN reports is folded
// as its own versioned namespace; a write to it — or one that reaches it via the
// cascade: a write to a view's base, or to the source of an MV populating it —
// evicts the result. When ClickHouse can't analyze the query (a write/DDL pipe, a
// missing table, an unreachable server) the pipe folds the single database-version
// namespace, which any write bumps: coarse, but never a stale read and O(1) per
// request.
//
// The second return reports whether the result must be TTL-capped because it
// can't be reliably version-invalidated — see resolvedDeps.unresolved for the
// triggers.
//
// TODO: impl scope (per-tenant cache invalidation) — each dep is whole-table for now.
func (h *PipesHandler) resolveDeps(ctx context.Context, boundSQL string) ([]cache.Namespace, bool) {
	if h.registry == nil {
		return nil, false // dependency tracking disabled (no schema registry): TTL-only
	}

	// Steady state is a read — every Execute passes through here, including cache
	// hits — so only the read lock is taken; the write lock is for the cold-miss
	// insert and ClearResolvedDeps.
	h.pipeDepsMu.RLock()
	rd, ok := h.pipeDeps[boundSQL]
	h.pipeDepsMu.RUnlock()
	if !ok {
		// Coalesce concurrent cold misses for the same bound query: one EXPLAIN,
		// every waiter shares the outcome. The generation is captured INSIDE the
		// flight, before the EXPLAIN runs, so it dates the schema state the
		// resolution was computed against for every waiter alike.
		v, _, _ := h.resolveSF.Do(boundSQL, func() (any, error) {
			h.pipeDepsMu.RLock()
			gen := h.pipeDepsGen
			h.pipeDepsMu.RUnlock()
			rd, memoize := h.resolvePipe(ctx, boundSQL)
			return resolveOutcome{rd: rd, memoize: memoize, gen: gen}, nil
		})
		out := v.(resolveOutcome)
		rd = out.rd
		if out.memoize {
			h.pipeDepsMu.Lock()
			// Insert only if no ClearResolvedDeps ran since the resolution started:
			// a refresh mid-EXPLAIN means this rd was computed against the
			// pre-refresh schema, and inserting it would silently undo the clear —
			// leaving a stale dependency set (the version-key inputs) memoized until
			// the NEXT refresh. Dropped instead; the next execution re-resolves.
			if h.pipeDepsGen == out.gen {
				// Bound the memo: a high-cardinality parameter could otherwise grow
				// it without limit between schema refreshes. Over the cap, evict ONE
				// arbitrary entry (Go map iteration order) — random replacement —
				// rather than dropping the whole map and forcing every working pipe
				// to re-EXPLAIN. Metered so an operator can spot the cardinality
				// problem; the cap bounds the memo's memory, not the per-request
				// work (the incoming query's EXPLAIN had to run regardless).
				if len(h.pipeDeps) >= maxPipeDeps {
					for k := range h.pipeDeps {
						delete(h.pipeDeps, k)
						break
					}
					pipeDepsEvictionCounter.Add(ctx, 1)
				}
				h.pipeDeps[boundSQL] = rd
			}
			h.pipeDepsMu.Unlock()
		}
	}

	// fallback and unresolved are mutually exclusive: fallback is set when EXPLAIN
	// can't run (resolvePipe returns early), unresolved only when it succeeds. Both
	// counters meter EXECUTIONS (including cache hits), not resolution events: the
	// operator signal is "what share of pipe traffic is running degraded", which a
	// resolve-time-only count would under-report (a memoized fallback resolves once
	// but can serve degraded for hours).
	if rd.fallback {
		pipeFallbackCounter.Add(ctx, 1)
		return []cache.Namespace{cache.DatabaseNamespace()}, false
	}
	if rd.unresolved {
		pipeUnresolvedCounter.Add(ctx, 1)
	}
	deps := make([]cache.Namespace, 0, len(rd.tables))
	for _, name := range rd.tables {
		deps = append(deps, cache.Namespace{Table: chsql.SafeEncodeNATS(name)})
	}
	return deps, rd.unresolved
}

// resolvePipe asks ClickHouse for the bound query's tables. An error yields a
// fallback marker so the caller over-resolves rather than risk an under-resolution.
//
// memoize reports whether the result may be kept in the per-bound-query cache. A
// ClickHouse-answered failure (*clickhouse.Exception: a write/DDL pipe, a missing
// table) is a property of the query against the current schema, so it is memoized
// like a success — ClearResolvedDeps drops it when the schema changes. A failure to
// ASK (an unreachable server, the resolve timeout, the caller's request being
// canceled mid-EXPLAIN) is transient, so it is NOT memoized: memoizing it would pin
// the pipe to the database-version fallback — evicted by every write, effectively
// uncacheable — until some future schema CHANGE happens to clear it, which on a
// stable schema is never. The execution still falls back this once (never stale),
// and the next execution simply retries the EXPLAIN.
func (h *PipesHandler) resolvePipe(ctx context.Context, boundSQL string) (rd *resolvedDeps, memoize bool) {
	if h.resolveTablesFn == nil {
		return &resolvedDeps{fallback: true}, true
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := h.resolveTablesFn(rctx, boundSQL)
	if err != nil {
		h.logger.DebugContext(ctx, "pipe dependency resolution failed; falling back to database-version invalidation", "error", err)
		_, answered := errors.AsType[*clickhouse.Exception](err)
		return &resolvedDeps{fallback: true}, answered
	}
	rd = &resolvedDeps{tables: res.Tables, unresolved: res.Pruned || res.External}
	if res.Pruned {
		h.logger.DebugContext(ctx, "pipe dependency set was dead-branch pruned; TTL-capping result as a safety bound")
	}
	if res.External {
		h.logger.DebugContext(ctx, "pipe reads an external source (table function, cross-database, or non-local dictionary); TTL-capping result")
	}
	// A resolved dep whose version isn't reliably maintained (an unfoldable view, an
	// unknown name) makes the whole result untrustworthy to version invalidation, so
	// mark it for a TTL cap. registry is non-nil here (resolveDeps guards it).
	for _, t := range res.Tables {
		if !h.registry.IsKnown(t) {
			rd.unresolved = true
			h.logger.DebugContext(ctx, "pipe dependency can't be reliably version-invalidated; TTL-capping result", "table", t)
			break
		}
	}
	return rd, true
}

// ClearResolvedDeps drops every pipe's cached table resolution so each re-resolves
// against the current schema on its next execution. main wires this to a schema
// refresh, so a developer's "edit pipe/schema -> refresh" picks up the change. The
// generation bump makes the clear stick: a resolution already in flight (computed
// against the pre-refresh schema) sees a stale generation at insert time and is
// dropped instead of silently re-populating the memo.
func (h *PipesHandler) ClearResolvedDeps() {
	h.pipeDepsMu.Lock()
	h.pipeDeps = make(map[string]*resolvedDeps)
	h.pipeDepsGen++
	h.pipeDepsMu.Unlock()
}
