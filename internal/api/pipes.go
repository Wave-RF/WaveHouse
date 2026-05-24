package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"
)

// sfSharedPipesAttrs is the static label set for the singleflight-shared
// counter from this handler. See sfSharedStructuredAttrs in structured_query.go
// for the rationale.
var sfSharedPipesAttrs = metric.WithAttributes(attribute.String("surface", "pipes"))

// PipesHandler handles named query pipe endpoints.
type PipesHandler struct {
	Store           *pipes.Store
	CHConn          driver.Conn
	Cache           cache.Cache
	sf              singleflight.Group
	maxQueryTimeout time.Duration
}

func NewPipesHandler(store *pipes.Store, conn driver.Conn, c cache.Cache, queryTimeout time.Duration) *PipesHandler {
	return &PipesHandler{Store: store, CHConn: conn, Cache: c, maxQueryTimeout: queryTimeout}
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
	r = r.WithContext(observability.WithComponent(r.Context(), "api/pipes"))
	name := chi.URLParam(r, "name")
	q := h.Store.Get(name)
	if q == nil {
		writeJSONError(w, http.StatusNotFound, "pipe not found")
		return
	}

	// Check role permissions.
	if len(q.AllowedRoles) > 0 {
		role := RoleFromContext(r.Context())
		if role != "" {
			allowed := false
			for _, ar := range q.AllowedRoles {
				if ar == role || ar == "*" {
					allowed = true
					break
				}
			}
			if !allowed {
				writeJSONError(w, http.StatusForbidden, "forbidden")
				return
			}
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

	// TODO: scope impl
	scope := ""
	safePipeName := query.SafeEncodeNATS(name)

	// TODO: current pipe impl doesn't have a list of tables/scopes, so bento ingest cannot invalidate it

	// Cache.
	cacheKey := queryCacheKey(sql, params)
	if h.Cache != nil {
		if data, _, err := h.Cache.Get(r.Context(), cacheKey, safePipeName, scope); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	// Execute with singleflight.
	v, err, shared := h.sf.Do(cacheKey, func() (interface{}, error) {
		queryCtx, cancel := context.WithTimeout(r.Context(), h.maxQueryTimeout)
		defer cancel()

		start := time.Now()

		rows, err := executeCHQuery(queryCtx, h.CHConn, sql, params, "pipes")
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
			_ = h.Cache.Set(r.Context(), cacheKey, safePipeName, scope, data, ttl)
		}
		return data, nil
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if shared {
		observability.QuerySingleflightShared.Add(r.Context(), 1, sfSharedPipesAttrs)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(v.([]byte))
}
