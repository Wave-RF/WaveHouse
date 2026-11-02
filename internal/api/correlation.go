package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// requestIDHeader carries the request id both ways: an upstream proxy's id
// is kept, and the response names the id the request was logged under.
const requestIDHeader = "X-Request-Id"

// maxRequestIDLen bounds a caller's id, which is copied onto every log
// record and span of its request.
const maxRequestIDLen = 128

// requestID gives every request an id and puts it in the request scope the
// log handler and AnnotateSpan read (observability.WithRequest). An incoming
// X-Request-Id is kept when it is sent once and is 1–128 printable ASCII
// characters without spaces; otherwise the request gets a fresh random id,
// so a repeated header cannot pick which value the logs carry.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id string
		if values := r.Header.Values(requestIDHeader); len(values) == 1 && validRequestID(values[0]) {
			id = values[0]
		} else {
			id = newRequestID()
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(observability.WithRequest(r.Context(), id)))
	})
}

func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for i := range len(id) {
		if id[i] < '!' || id[i] > '~' {
			return false
		}
	}
	return true
}

// newRequestID is 128 random bits in hex, the shape of a trace id. Unlike
// chi's middleware.RequestID it names no host, since it goes back to the
// caller.
func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // never fails (crypto/rand.Read)
	return hex.EncodeToString(b[:])
}

// opsReloadPath is the one ops route that, naming no tenant, acts on every
// tenant rather than on tenant.Default.
const opsReloadPath = "/v1/ops/settings/reload"

// opsRequestTenant records the tenant an ops request addresses before
// authentication, so the operator-key audit line and an admin-gate denial
// carry it: the one ?tenant= names, or tenant.Default when it names none,
// except on the reload, which then acts on every tenant. It writes nothing:
// a query opsTenant refuses is left without a tenant, and its handler
// answers the 400.
func opsRequestTenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if params, err := url.ParseQuery(r.URL.RawQuery); err == nil {
			values, named := params[opsTenantParam]
			switch {
			case !named && r.URL.Path != opsReloadPath:
				observability.SetRequestTenant(r.Context(), tenant.Default.String())
			case len(values) == 1:
				if id, err := tenant.Parse(values[0]); err == nil {
					observability.SetRequestTenant(r.Context(), id.String())
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// annotateSpan copies the request scope onto the server span, which the
// otelhttp middleware starts after requestID has run, and names the span by
// the route the request matched.
//
// otelhttp takes the span name and the http.route of its metrics from
// r.Pattern once the handler returns. chi sets r.Pattern on the request it
// routes, but a sub-router whose middleware derives a request (r.WithContext,
// as the /v1/ops gates do) sets it on that copy, so otelhttp would keep the
// mount's pattern, /v1/ops/*, for every ops route (#892). The route context
// is shared by every copy, so its pattern is complete after the handler, and
// annotateSpan writes it back to r, the request otelhttp handed it: it must
// run directly inside the otelhttp middleware.
func annotateSpan(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observability.AnnotateSpan(r.Context())
		next.ServeHTTP(w, r)
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			if p := rctx.RoutePattern(); p != "" {
				r.Pattern = p
				trace.SpanFromContext(r.Context()).SetAttributes(semconv.HTTPRoute(p))
			}
		}
	})
}
