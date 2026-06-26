package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/singleflight"
)

const maxPipeDeps = 50000

type resolvedDeps struct {
	tables []string
	// fallback is set when EXPLAIN couldn't be used (a write/DDL pipe, a
	// missing table, an unreachable ClickHouse), in which case the pipe OVER-resolves
	// to every base table so any write evicts it — never an under-resolution. Cached
	// per pipe SQL template and cleared on schema refresh.
	fallback bool
	// unresolved is set when EXPLAIN succeeded but at least one resolved dependency
	// can't be reliably version-invalidated — an unfoldable view (unparsed
	// definition, or a cross-database/unknown source) or an unknown name, per
	// SchemaRegistry.IsKnown. The refresh-time cascade never bumps such a name's
	// version, so trusting it would serve stale; the Execute path TTL-floors the
	// result instead (cache.UnresolvedDepsTTLCap). A table function or cross-database
	// table referenced directly is absent from tables entirely (not flagged here) and
	// stays documented-stale. Distinct from fallback, whose all-base-tables set is
	// fully known and version-maintained.
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
	Registry        *discovery.SchemaRegistry
	resolveTablesFn func(ctx context.Context, sql string) ([]string, error)
	pipeDeps        map[string]*resolvedDeps
	pipeDepsMu      sync.Mutex

	// maxRequestBytes optionally overrides the default inbound request body
	// cap (maxControlBodyBytes) for the body-decoding paths (Put, Execute).
	// When 0, the default applies. Test-only seam (pin the cap-overflow path
	// without allocating 1 MiB per run); not a production knob. Mirrors
	// StructuredQueryHandler / QueryHandler.
	maxRequestBytes int64
}

func NewPipesHandler(store *pipes.Store, policyStore *policy.Store, conn driver.Conn, c cache.Cache, queryTimeout time.Duration, logger *slog.Logger) *PipesHandler {
	h := &PipesHandler{
		Store:           store,
		PolicyStore:     policyStore,
		CHConn:          conn,
		Cache:           c,
		maxQueryTimeout: queryTimeout,
		logger:          logger,
		pipeDeps:        make(map[string]*resolvedDeps),
	}
	// Resolve a pipe's tables by asking ClickHouse (EXPLAIN QUERY TREE). Overridable
	// in tests; the database comes from the Registry (set by main post-construction).
	h.resolveTablesFn = func(ctx context.Context, sql string) ([]string, error) {
		return discovery.ResolveTables(ctx, h.CHConn, h.Registry.Database(), sql)
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

	if h.Cache != nil {
		if data, _, err := h.Cache.Get(r.Context(), cacheKey, deps); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
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
				// A resolved dependency can't be reliably version-invalidated (an
				// unfoldable view): floor the TTL so the result self-expires rather
				// than serving stale on a write the cascade never bumps.
				ttl = min(ttl, cache.UnresolvedDepsTTLCap)
			}
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

// resolveDeps returns the cache namespaces a pipe's result depends on, by asking
// ClickHouse which tables the bound query reads (EXPLAIN QUERY TREE, via
// resolveTablesFn), cached per pipe SQL template. A table EXPLAIN reports is folded
// as its own versioned namespace; a write to it — or, for a view, a write to the
// view's base via the cascade — evicts the result. When ClickHouse can't analyze the
// query (a write/DDL pipe, a missing table, an unreachable server) the pipe
// OVER-resolves to every base table, so any write evicts it: coarse, but never a
// stale read. A table function (s3()/numbers()) or a cross-database table is simply
// absent from the list — those pipes are unsupported and may go stale (documented).
//
// The second return reports whether the result must be TTL-floored: true when a
// resolved dependency can't be reliably version-invalidated (an unfoldable view or
// unknown name — see resolvedDeps.unresolved), so the caller caps the cache TTL
// rather than trust an unmaintained version.
//
// TODO: impl scope (per-tenant cache invalidation) — each dep is whole-table for now.
func (h *PipesHandler) resolveDeps(ctx context.Context, boundSQL string) ([]cache.Namespace, bool) {
	if h.Registry == nil {
		return nil, false // dependency tracking disabled (no schema registry): TTL-only
	}

	h.pipeDepsMu.Lock()
	rd, ok := h.pipeDeps[boundSQL]
	h.pipeDepsMu.Unlock()
	if !ok {
		rd = h.resolvePipe(ctx, boundSQL)
		h.pipeDepsMu.Lock()
		// Bound the per-bound-query cache: a high-cardinality parameter could
		// otherwise grow it without limit between schema refreshes. On overflow drop
		// the whole map; entries simply re-resolve (one EXPLAIN) on next execution.
		if len(h.pipeDeps) >= maxPipeDeps {
			h.pipeDeps = make(map[string]*resolvedDeps)
		}
		h.pipeDeps[boundSQL] = rd
		h.pipeDepsMu.Unlock()
	}

	names := rd.tables
	if rd.fallback {
		names = h.Registry.AllBaseTables() // over-resolve: any write evicts
	}
	deps := make([]cache.Namespace, 0, len(names))
	for _, name := range names {
		deps = append(deps, cache.Namespace{Table: chsql.SafeEncodeNATS(name)})
	}
	return deps, rd.unresolved
}

// resolvePipe asks ClickHouse for the bound query's tables. An error (a write/DDL
// pipe, a missing table, an unreachable ClickHouse) yields a fallback marker so the
// caller over-resolves rather than risk an under-resolution.
func (h *PipesHandler) resolvePipe(ctx context.Context, boundSQL string) *resolvedDeps {
	if h.resolveTablesFn == nil {
		return &resolvedDeps{fallback: true}
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tables, err := h.resolveTablesFn(rctx, boundSQL)
	if err != nil {
		h.logger.DebugContext(ctx, "pipe dependency resolution failed; over-resolving to all tables", "error", err)
		return &resolvedDeps{fallback: true}
	}
	rd := &resolvedDeps{tables: tables}
	// A resolved dep whose version isn't reliably maintained (an unfoldable view, an
	// unknown name) makes the whole result untrustworthy to version invalidation, so
	// mark it for a TTL floor. Registry is non-nil here (resolveDeps guards it).
	if h.Registry != nil {
		for _, t := range tables {
			if !h.Registry.IsKnown(t) {
				rd.unresolved = true
				break
			}
		}
	}
	return rd
}

// ClearResolvedDeps drops every pipe's cached table resolution so each re-resolves
// against the current schema on its next execution. main wires this to a schema
// refresh, so a developer's "edit pipe/schema -> refresh" picks up the change.
func (h *PipesHandler) ClearResolvedDeps() {
	h.pipeDepsMu.Lock()
	h.pipeDeps = make(map[string]*resolvedDeps)
	h.pipeDepsMu.Unlock()
}
