package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

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

// resolveTenant turns the tenant values a request carries — one header line
// or one query value, none meaning tenant.Default — into that tenant's store.
// A malformed id is a 400 and a well-formed id the registry does not hold is
// a 404. A repeated value is refused rather than picked from, so a value a
// proxy sets can never be shadowed by one the client sent. where names the
// source in the error body.
func resolveTenant(w http.ResponseWriter, tenants *settings.Registry, where string, values []string) (*settings.Store, bool) {
	id := tenant.Default
	if len(values) > 1 {
		writeJSONError(w, http.StatusBadRequest, "invalid "+where+": sent more than once")
		return nil, false
	}
	if len(values) == 1 && values[0] != "" {
		parsed, err := tenant.Parse(values[0])
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid "+where+": "+err.Error())
			return nil, false
		}
		id = parsed
	}
	store, ok := tenants.For(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "unknown tenant: "+id.String())
		return nil, false
	}
	return store, true
}

// TenantMW resolves the request's tenant from the tenant.Header before
// authentication runs and stores it in the request context.
func TenantMW(tenants *settings.Registry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			store, ok := resolveTenant(w, tenants, tenant.Header, r.Header.Values(tenant.Header))
			if !ok {
				return
			}
			next.ServeHTTP(w, r.WithContext(WithStore(r.Context(), store)))
		})
	}
}

// opsTenantParam is the query parameter an ops route takes its tenant from.
// The ops tree is tenant-exempt — it runs no TenantMW and ignores the header
// — so an admin names the tenant explicitly, and none means tenant.Default.
const opsTenantParam = "tenant"

// opsStore resolves the tenant an ops route addresses from its
// opsTenantParam, with the same answers as TenantMW. The query string is
// parsed strictly: url.Values silently drops a malformed pair, which would
// turn "?tenant=acme;x=1" into the default tenant rather than a 400.
func opsStore(w http.ResponseWriter, r *http.Request, tenants *settings.Registry) (*settings.Store, bool) {
	where := "?" + opsTenantParam
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid "+where+": malformed query string")
		return nil, false
	}
	return resolveTenant(w, tenants, where, values[opsTenantParam])
}
