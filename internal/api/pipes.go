package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/singleflight"
)

// PipesHandler handles named query pipe endpoints.
type PipesHandler struct {
	Store       *pipes.Store
	PolicyStore *policy.Store // resolves empty role to default_role; may be nil
	CHConn      driver.Conn
	Cache       *cache.TieredCache
	DefaultTTL  time.Duration
	sf          singleflight.Group
}

func NewPipesHandler(store *pipes.Store, conn driver.Conn, c *cache.TieredCache, defaultTTL time.Duration) *PipesHandler {
	return &PipesHandler{Store: store, CHConn: conn, Cache: c, DefaultTTL: defaultTTL}
}

// List returns all named queries (admin endpoint).
func (h *PipesHandler) List(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.Store.List())
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
	var q pipes.NamedQuery
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
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

	// Authorization is allowlist membership: the caller's role — a tokenless or
	// roleless request first mapped to the configured default_role — must appear
	// in allowed_roles. Matching is exact (there is no "*" any-role wildcard) and
	// empty allowlist entries are skipped, so a stray "" can't authorize an empty
	// role. A pipe with no allowed_roles therefore authorizes nobody but the
	// admin/service bypass below — an absent allowlist grants no one (fails closed).
	role := RoleFromContext(r.Context())
	if role == "" && h.PolicyStore != nil {
		if p := h.PolicyStore.Get(); p != nil {
			role = policy.ResolveRole(p, role)
		}
	}
	// admin/service bypass every pipe's allowlist, listed or not — by design, not
	// an oversight: they author pipes and can run arbitrary SQL via /v1/admin/query,
	// so allowed_roles is never a confidentiality boundary against them. Mirrors
	// policy.Evaluate's built-in admin/service bypass.
	if role != "admin" && role != "service" {
		allowed := false
		for _, ar := range q.AllowedRoles {
			if ar != "" && ar == role {
				allowed = true
				break
			}
		}
		if !allowed {
			writeJSONError(w, http.StatusForbidden, forbiddenForRole(role))
			return
		}
	}

	// Gather parameters from query string and/or JSON body.
	supplied := make(map[string]any)
	for key, vals := range r.URL.Query() {
		if len(vals) > 0 {
			supplied[key] = vals[0]
		}
	}
	if r.Method == http.MethodPost {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
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

	// Cache.
	cacheKey := queryCacheKey(sql, params)
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
		queryCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		rows, err := executeCHQuery(queryCtx, h.CHConn, sql, params)
		if err != nil {
			return nil, err
		}

		data, err := json.Marshal(rows)
		if err != nil {
			return nil, err
		}

		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), cacheKey, data, h.DefaultTTL)
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
