package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// PolicySource yields the access-control policy of the request's tenant,
// read per request so a settings reload applies to the next one
// ((*settings.Store).Policy in production). A nil policy from a wired source
// is a deliberate lockout; a nil source means policy filtering is not wired
// at all, which only tests do.
type PolicySource func(*settings.Store) *policy.Policy

type tenantStoreKey struct{}

// WithStore returns ctx carrying the request's resolved tenant store. These
// helpers live here rather than in internal/tenant because settings names
// tenant.ID, so tenant cannot name settings.Store back.
func WithStore(ctx context.Context, store *settings.Store) context.Context {
	return context.WithValue(ctx, tenantStoreKey{}, store)
}

// StoreFromContext returns the store TenantMW resolved for this request.
// Handlers call it once and pass the store down as an argument; nothing
// below a handler reads it from the context.
func StoreFromContext(ctx context.Context) (*settings.Store, bool) {
	store, _ := ctx.Value(tenantStoreKey{}).(*settings.Store)
	return store, store != nil
}

// requestStore is a handler's single read of the tenant store. A request
// that reaches a tenant route without one skipped TenantMW — a routing bug,
// answered with a 500 rather than by serving some other tenant's settings.
func requestStore(w http.ResponseWriter, r *http.Request) (*settings.Store, bool) {
	store, ok := StoreFromContext(r.Context())
	if !ok {
		slog.ErrorContext(r.Context(), "tenant route reached without a resolved tenant", "path", r.URL.Path)
		writeJSONError(w, http.StatusInternalServerError, "internal server error")
	}
	return store, ok
}

// resolveStore is the one answer for a tenant that cannot be served: a 404
// for an id the registry does not hold, and a 503 for a tenant it holds whose
// settings folder was rejected (nested directories only). The 503 says
// nothing more: tenant routes resolve before authentication, and the findings
// quote the tenant's settings — they go to the log and to the reload
// response, which is behind the ops gate.
func resolveStore(w http.ResponseWriter, tenants *settings.Registry, id tenant.ID) (*settings.Store, bool) {
	store, known := tenants.Resolve(id)
	switch {
	case store != nil:
		return store, true
	case known:
		writeJSONError(w, http.StatusServiceUnavailable, "tenant settings are invalid")
	default:
		writeJSONError(w, http.StatusNotFound, "unknown tenant: "+id.String())
	}
	return nil, false
}

// TenantMW resolves the request's tenant before authentication runs and
// stores it in the request context: the tenant.Header value, tenant.Default
// when absent. A malformed id is a 400; an id that cannot be served is
// resolveStore's 404 or 503. A repeated header is refused rather than picked
// from, so a value a proxy sets can never be shadowed by one the client sent.
// Every answer, the refusals included, carries Vary: X-Tenant-ID so a shared
// cache cannot replay one tenant's response to another — added, not set, so
// the CORS Vary: Origin survives.
func TenantMW(tenants *settings.Registry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Vary", tenant.Header)
			id := tenant.Default
			values := r.Header.Values(tenant.Header)
			if len(values) > 1 {
				writeJSONError(w, http.StatusBadRequest, "invalid "+tenant.Header+": sent more than once")
				return
			}
			if len(values) == 1 && values[0] != "" {
				parsed, err := tenant.Parse(values[0])
				if err != nil {
					writeJSONError(w, http.StatusBadRequest, "invalid "+tenant.Header+": "+err.Error())
					return
				}
				id = parsed
			}
			store, ok := resolveStore(w, tenants, id)
			if !ok {
				return
			}
			next.ServeHTTP(w, r.WithContext(WithStore(r.Context(), store)))
		})
	}
}
