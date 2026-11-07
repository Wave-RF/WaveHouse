package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/chsql"
	"github.com/Wave-RF/WaveHouse/pkg/pipes"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

	// metrics records each ClickHouse call; nil records nothing.
	metrics *queryMetrics
}

func NewPipesHandler(source func(*settings.Store) pipes.Source, policySource PolicySource, target func(*settings.Store) chconn.Target, c cache.Cache, queryTimeout func(*settings.Store) time.Duration) *PipesHandler {
	return &PipesHandler{Source: source, PolicySource: policySource, Target: target, Cache: c, ch: NewCHReader(), queryTimeout: queryTimeout, metrics: newQueryMetrics()}
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

	// The verified token's claims bind the SQL's {{ jwt.<path> }} references;
	// a request with no valid token has none, so every such reference fails
	// closed below. The request's parameters never reach a claim reference.
	// BindParams inlines every value, claims included, as an escaped SQL
	// literal — a pipe's placeholders can sit anywhere in the statement,
	// including positions (LIMIT, an identifier) where a bound parameter is
	// not legal — so the rendered SQL carries no placeholders and nothing is
	// bound here.
	claims, _ := auth.ClaimsFromContext(r.Context())
	sql, err := pipes.BindParams(q, supplied, claims)
	if err != nil {
		var unresolved *pipes.UnresolvedClaimError
		if errors.As(err, &unresolved) {
			writeClaimUnresolved(w, r, q, role, unresolved)
			return
		}
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if chsql.IsMutation(sql) {
		h.executeWrite(w, r, store, q.Name, sql)
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
		data, err := h.run(r.Context(), store, target, opPipe, q.Name, chRequest{sql: sql})
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

// writeClaimUnresolved answers a pipe whose SQL reads a claim the caller's
// token cannot bind (pipes.UnresolvedClaimError). The pipe fails closed the
// way a policy filter on the same claim does (#385): nothing runs, and no
// stand-in is bound. A read answers 200 with no rows and no X-Cache header,
// as a structured query's row read does under an unresolvable filter (a
// global aggregate gets no row here where a structured query gets one row of
// zeros, since no stand-in value is safe to bind); a write is refused with
// 403, as an insert under an unresolvable _in check is, since "no rows" means
// nothing for a write. A present-but-invalid token fails loud instead, a 401
// naming the token problem, so a caller whose expired token fell back to the
// default role learns why its rows are gone (mirrors writeAuthzDenied). Admin
// is no exception: the reference is the pipe author's SQL, not policy, so an
// admin token binds what it carries, and one without the claim — the operator
// key carries none — gets the same answer; a see-everything pipe is a
// separate pipe with no allowed_roles. The write classification reads the
// template's Skeleton, every placeholder bound to an empty string literal,
// since the bound SQL does not exist: on the raw template a placeholder in
// the `EXECUTE AS <user>` slot, or a quoted inline default ahead of an
// INSERT, would hide the verb from chsql.IsMutation and answer a write's unresolved
// claim with a success-looking `[]`.
func writeClaimUnresolved(w http.ResponseWriter, r *http.Request, q *pipes.NamedQuery, role string, err *pipes.UnresolvedClaimError) {
	attrs := []slog.Attr{
		slog.String("gate", "pipe"),
		slog.String("pipe", q.Name),
		slog.String("claim", err.Path),
	}
	switch authErr := auth.AuthErrorFromContext(r.Context()); {
	case authErr != nil:
		logAuthzDenied(r, authErr.Error(), role, q.AllowedRoles, http.StatusUnauthorized, attrs...)
		writeJSONError(w, http.StatusUnauthorized, authErr.Error())
	case chsql.IsMutation(q.Skeleton()):
		logAuthzDenied(r, err.Error(), role, q.AllowedRoles, http.StatusForbidden, attrs...)
		writeJSONError(w, http.StatusForbidden, "forbidden: "+err.Error())
	default:
		// Not a denial: a public pipe reading a claim answers every anonymous
		// caller this way by design, so the line is DEBUG, there for an
		// operator asking why a pipe returns nothing.
		slog.LogAttrs(r.Context(), slog.LevelDebug, "pipe claim unresolved; answering no rows",
			append([]slog.Attr{slog.String("role_resolved", role)}, attrs...)...)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}
}

// executeWrite runs a pipe that writes, on every call: a cached or coalesced
// response would answer a repeat without executing it, silently dropping the
// write (#386) — on every instance once the cache is shared. It is the one
// statement sent without readonly=2, so what bypasses here is exactly what
// may write. no-store keeps an HTTP cache in front of a GET from answering a
// repeat the same way.
func (h *PipesHandler) executeWrite(w http.ResponseWriter, r *http.Request, store *settings.Store, name, sql string) {
	target := targetOf(h.Target, store)
	if target.URL == "" {
		writeUnavailable(w, noConnectionMessage, retryAfterPool)
		return
	}
	data, err := h.run(r.Context(), store, target, opPipeWrite, name, chRequest{sql: sql, write: true})
	if err != nil {
		writeCHWriteError(w, r, err, chErrorMessage(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "BYPASS")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data) //nolint:gosec // G705: ClickHouse's JSONEachRow rows framed as an array, [] for a write
}

// run executes pipe name's bound SQL under the tenant's query timeout and
// returns ClickHouse's rows as a JSON array, recording the call as operation
// (opPipe or opPipeWrite). A pipe carries no per-role
// resource caps (allowed_roles is its whole policy), so the query timeout is
// the only limit it sends; the deadline outlasts it by capBackstop, as on
// the structured query.
func (h *PipesHandler) run(ctx context.Context, store *settings.Store, target chconn.Target, operation, name string, req chRequest) ([]byte, error) {
	timeout := timeoutOf(h.queryTimeout, store)
	queryCtx, cancel := context.WithTimeout(ctx, timeout+capBackstop)
	defer cancel()
	req.settings = chReadSettings(chQueryLimits{ExecutionTime: timeout})
	queryCtx, span := otel.Tracer(chTracerName).Start(queryCtx, chQuerySpanName,
		chQuerySpanOptions(target, operation, req.sql, attribute.String("pipe", name))...)
	start := time.Now()
	data, err := h.ch.do(queryCtx, target, connsOf(h.MaxConns, store), req)
	// A read is sent under readonly=2, so its refusal classifies as the
	// response's does; a write is not.
	setCHQueryResult(span, h.metrics.observe(ctx, operation, time.Since(start), err, queryCaps{readonly: !req.write}))
	span.End()
	return data, err
}
