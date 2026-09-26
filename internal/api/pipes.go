package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
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
	// CHConn yields the request tenant's connection (chconn.Pools.For in
	// production); nil is a tenant on no pool, a 503.
	CHConn func(*settings.Store) driver.Conn
	Cache  cache.Cache
	sf     singleflight.Group
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

func NewPipesHandler(source func(*settings.Store) pipes.Source, policySource PolicySource, conn func(*settings.Store) driver.Conn, c cache.Cache, queryTimeout func(*settings.Store) time.Duration) *PipesHandler {
	return &PipesHandler{Source: source, PolicySource: policySource, CHConn: conn, Cache: c, queryTimeout: queryTimeout}
}

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

	sql, params, err := pipes.BindParams(q, supplied)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
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
	cacheKey := queryCacheKey(store.Tenant(), sql, params)
	var entry cache.Entry
	var snap cache.Snapshot
	if h.Cache != nil {
		entry, snap, _ = h.Cache.Lookup(r.Context(), store.Tenant(), cacheKey, nil)
	}

	// The tenant's pool, ahead of serving a hit: a tenant on none — its
	// tuple could not be opened, such as by the connection ceiling — fails
	// closed rather than serve what it cached before (#583 story 6).
	conn := connOf(h.CHConn, store)
	if conn == nil {
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
		queryCtx, cancel := context.WithTimeout(r.Context(), timeoutOf(h.queryTimeout, store))
		defer cancel()

		start := time.Now()

		rows, err := executeCHQuery(queryCtx, conn, sql, params)
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
		writeCHError(w, r, err, err.Error(), http.StatusInternalServerError, queryCaps{})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(v.([]byte)) //nolint:gosec // G705: the tenant id on the key only selects the entry; the bytes are JSON the handler marshalled from ClickHouse rows
}
