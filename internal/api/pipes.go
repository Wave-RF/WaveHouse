package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/singleflight"
)

// PipesHandler serves named query pipes: execution for callers, read-only
// listing for admins. Pipes are defined in the settings directory's
// pipes.json and read per request, so a reload applies immediately.
type PipesHandler struct {
	// Source yields a tenant's pipes (the store itself in production).
	Source       func(*settings.Store) pipes.Source
	PolicySource PolicySource // resolves empty role to default_role; may be nil
	// Tenants resolves the tenant the admin reads (List, Get) serve: /v1/ops
	// is tenant-exempt, so they carry no request tenant and read the one
	// ?tenant= names, the default one without it (opsStore).
	Tenants *settings.Registry
	// Target yields the request tenant's ClickHouse HTTP wiring
	// (chconn.Pools.Target in production); the zero Target is a tenant on no
	// pool, a 503.
	Target func(*settings.Store) chconn.Target
	// MaxConns caps the concurrent reads on the tenant's pool (its native
	// pool's size in production); nil or non-positive is defaultReadConns.
	MaxConns func(*settings.Store) int
	Cache    cache.Cache
	sf       singleflight.Group
	ch       *chReader
	// queryTimeout bounds each pipe execution, read per request off the
	// tenant's settings ((*settings.Store).ClickHouse().QueryTimeout in
	// production) so a settings reload applies without a restart.
	queryTimeout func(*settings.Store) time.Duration

	// maxRequestBytes optionally overrides the default inbound request body
	// cap (maxControlBodyBytes) for the body-decoding path (Execute).
	// When 0, the default applies. Test-only seam (pin the cap-overflow path
	// without allocating 1 MiB per run); not a production knob. Mirrors
	// StructuredQueryHandler / QueryHandler.
	maxRequestBytes int64
}

func NewPipesHandler(source func(*settings.Store) pipes.Source, policySource PolicySource, target func(*settings.Store) chconn.Target, c cache.Cache, queryTimeout func(*settings.Store) time.Duration) *PipesHandler {
	return &PipesHandler{Source: source, PolicySource: policySource, Target: target, Cache: c, ch: NewCHReader(), queryTimeout: queryTimeout}
}

// SetReader replaces the handler's private reader with a shared one. Call it
// before serving.
func (h *PipesHandler) SetReader(r *CHReader) { h.ch = r }

// List returns all named queries of the ?tenant= (admin endpoint).
func (h *PipesHandler) List(w http.ResponseWriter, r *http.Request) {
	store, ok := opsStore(w, r, h.Tenants)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	q := h.Source(store).Pipes()
	if q == nil {
		q = []*pipes.NamedQuery{}
	}
	_ = json.NewEncoder(w).Encode(q)
}

// Get returns a specific named query of the ?tenant= (admin endpoint).
func (h *PipesHandler) Get(w http.ResponseWriter, r *http.Request) {
	store, ok := opsStore(w, r, h.Tenants)
	if !ok {
		return
	}
	name := chi.URLParam(r, "name")
	q := h.Source(store).Pipe(name)
	if q == nil {
		writeJSONError(w, http.StatusNotFound, "pipe not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(q)
}

// Execute runs a named query with the provided parameters.
func (h *PipesHandler) Execute(w http.ResponseWriter, r *http.Request) {
	store, ok := requestStore(w, r)
	if !ok {
		return
	}
	name := chi.URLParam(r, "name")
	q := h.Source(store).Pipe(name)
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
	// /v1/ops/query, so allowed_roles is never a confidentiality boundary
	// against them (mirrors Evaluate's admin bypass). A pipe with no
	// allowed_roles therefore authorizes nobody but admin (fails closed).
	var p *policy.Policy
	if h.PolicySource != nil {
		p = h.PolicySource(store)
	}
	role := policy.ResolveRole(p, auth.RoleFromContext(r.Context()))
	if !policy.RoleAllowed(p, role, q.AllowedRoles) {
		writeAuthzDenied(w, r, role, q.AllowedRoles,
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

	// BindParams inlines every value as an escaped SQL literal — a pipe's
	// placeholders can sit anywhere in the statement, including positions
	// (LIMIT, an identifier) where a bound parameter is not legal — so the
	// rendered SQL carries no placeholders and nothing is bound here.
	sql, err := pipes.BindParams(q, supplied)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if IsMutation(sql) {
		h.executeWrite(w, r, store, sql)
		return
	}

	// Cache. A pipe can read several tables, but the current pipe impl doesn't
	// expose its table/scope dependencies, so we pass no deps: the result folds
	// the tenant's version alone, so InvalidateTenant orphans it but no insert
	// does (TTL-bound until #343). The snapshot is of the versions before
	// anything the query reads is chosen, so a bump landing after — mid-query
	// (#382), or a reload moving the tenant to another address or database
	// once its pool below is taken — orphans the fill.
	// TODO: once pipes expose their tables/scopes, pass them as deps here so writes
	// invalidate cached pipe results.
	cacheKey := queryCacheKey(store.Tenant(), sql, nil)
	var entry cache.Entry
	var snap cache.Snapshot
	if h.Cache != nil {
		entry, snap, _ = h.Cache.Lookup(r.Context(), store.Tenant(), cacheKey, nil)
	}

	// The tenant's pool, ahead of serving a hit: a tenant on none — its
	// tuple could not be opened, such as by the connection ceiling — fails
	// closed rather than serve what it cached before (#583 story 6).
	target := targetOf(h.Target, store)
	if target.URL == "" {
		writeUnavailable(w, noConnectionMessage, retryAfterPool)
		return
	}
	if entry.Value != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		_, _ = w.Write(entry.Value)
		return
	}

	// Execute with singleflight.
	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
		start := time.Now()
		data, err := h.run(r.Context(), store, target, chRequest{sql: sql})
		if err != nil {
			return nil, err
		}
		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), snap, data, cache.QueryTimeToTTL(time.Since(start)))
		}
		return data, nil
	})
	if err != nil {
		writeCHError(w, r, err, chErrorMessage(err), http.StatusInternalServerError, queryCaps{readonly: true})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(v.([]byte)) //nolint:gosec // G705: the tenant id on the key only selects the entry; the bytes are ClickHouse's JSONEachRow rows framed as an array
}

// executeWrite runs a pipe that writes, on every call: a cached or coalesced
// response would answer a repeat without executing it, silently dropping the
// write (#386) — on every instance once the cache is shared. It is the one
// statement sent without readonly=2, so what bypasses here is exactly what
// may write. no-store keeps an HTTP cache in front of a GET from answering a
// repeat the same way.
func (h *PipesHandler) executeWrite(w http.ResponseWriter, r *http.Request, store *settings.Store, sql string) {
	target := targetOf(h.Target, store)
	if target.URL == "" {
		writeUnavailable(w, noConnectionMessage, retryAfterPool)
		return
	}
	data, err := h.run(r.Context(), store, target, chRequest{sql: sql, write: true})
	if err != nil {
		writeCHWriteError(w, r, err, chErrorMessage(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "BYPASS")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data) //nolint:gosec // G705: ClickHouse's JSONEachRow rows framed as an array, [] for a write
}

// run executes a pipe's bound SQL under the tenant's query timeout and
// returns ClickHouse's rows as a JSON array. A pipe carries no per-role
// resource caps (allowed_roles is its whole policy), so the query timeout is
// the only limit it sends; the deadline outlasts it by capBackstop, as on
// the structured query.
func (h *PipesHandler) run(ctx context.Context, store *settings.Store, target chconn.Target, req chRequest) ([]byte, error) {
	timeout := timeoutOf(h.queryTimeout, store)
	queryCtx, cancel := context.WithTimeout(ctx, timeout+capBackstop)
	defer cancel()
	req.settings = chReadSettings(chQueryLimits{ExecutionTime: timeout})
	return h.ch.do(queryCtx, target, connsOf(h.MaxConns, store), req)
}
