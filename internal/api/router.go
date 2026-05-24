package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Dependencies holds all handler dependencies.
type Dependencies struct {
	Ingest          *IngestHandler
	Query           *QueryHandler
	SSE             *SSEHandler
	WS              *WSHandler
	Health          *HealthHandler
	Schema          *SchemaHandler
	DLQ             *DLQHandler
	Policy          *PolicyHandler
	Pipes           *PipesHandler
	StructuredQuery *StructuredQueryHandler
	AuthMW          func(http.Handler) http.Handler
	// PolicyStore backs the RequireAdmin gate: the admin role (policy.AdminRole)
	// is read live from the policy, so admin_role changes apply without a restart.
	PolicyStore *policy.Store
	JS          jetstream.JetStream // for SSE/WS gap-fill
	CORSOrigins []string            // allowed CORS origins; ["*"] = allow all
	LogLevel    *slog.LevelVar
	// MetricsHandler, if non-nil, is mounted at MetricsPath as an unauthenticated
	// endpoint (Prometheus convention). Wired by main.go from the OTel Prometheus
	// exporter when observability.metrics.prometheus.enabled is true AND port is 0.
	MetricsHandler http.Handler
	MetricsPath    string
}

// NewRouter creates the chi router with all routes.
func NewRouter(deps Dependencies) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(jsonRecoverer)
	r.Use(corsMiddleware(deps.CORSOrigins))

	// Route the chi router's own 404/405 paths through writeJSONError so
	// hits to unknown URLs and unsupported methods carry the same JSON
	// error contract as handler-emitted errors. Without this chi falls
	// back to http.Error / empty bodies and the response is text/plain.
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONError(w, http.StatusNotFound, "not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	})

	metricsPath := deps.MetricsPath
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip span creation on infra/probe paths:
			//   /v1/stream/*  — long-lived streams (SSE/WS); the standard
			//                   HTTP tracer would emit one span per stream
			//                   that lives until the client disconnects.
			//   prometheus    — scrape every ~15s would produce ~4 spans/min
			//                   of pure infra cardinality, and creates a
			//                   self-loop when the same backend stores both
			//                   traces and scraped metrics.
			//   /health, /ready — liveness/readiness probes inflate span
			//                   counts and skew latency percentiles.
			p := r.URL.Path
			if strings.HasPrefix(p, "/v1/stream/") || p == "/health" || p == "/ready" ||
				(metricsPath != "" && p == metricsPath) {
				next.ServeHTTP(w, r)
				return
			}
			// Normal REST tracing for everything else
			otelhttp.NewMiddleware("wavehouse-api")(next).ServeHTTP(w, r)
		})
	})

	// Public endpoints.
	r.Get("/health", deps.Health.Liveness)
	r.Get("/ready", deps.Health.Readiness)

	// Prometheus scrape endpoint — wired only when prometheus.enabled is true
	// AND prometheus.port is 0 (mount on this router). When prometheus.port
	// is non-zero, main.go runs a dedicated listener instead and this is nil.
	if deps.MetricsHandler != nil && deps.MetricsPath != "" {
		r.Method(http.MethodGet, deps.MetricsPath, deps.MetricsHandler)
	}

	// API v1 endpoints (auth middleware may be no-op if disabled).
	r.Route("/v1", func(r chi.Router) {
		r.Use(deps.AuthMW)

		// Single admin gate for every admin-equivalent surface. The admin role
		// is policy.AdminRole (configurable via admin_role, "admin" by default),
		// read live from the policy store so changes apply without a restart.
		// Declaring it once keeps the gate consistent across the tree.
		requireAdmin := RequireAdmin(deps.PolicyStore)

		r.Post("/ingest", deps.Ingest.Handle)
		r.Get("/stream/sse", deps.SSE.Handle)
		r.Get("/stream/ws", deps.WS.Handle)

		// Schema discovery — admin-only (no policy gate of its own).
		r.With(requireAdmin).Get("/schema", deps.Schema.Get)
		r.With(requireAdmin).Post("/schema/refresh", deps.Schema.Refresh)

		// Structured query endpoint.
		if deps.StructuredQuery != nil {
			r.Post("/query", deps.StructuredQuery.Handle)
		}

		// Named query pipes.
		if deps.Pipes != nil {
			r.Get("/pipes/{name}", deps.Pipes.Execute)
			r.Post("/pipes/{name}", deps.Pipes.Execute)
		}

		// DLQ stats — admin-only (no policy gate of its own).
		if deps.DLQ != nil {
			r.With(requireAdmin).Get("/dlq/stats", deps.DLQ.Stats)
		}

		// Admin routes. The requireAdmin gate covers the whole tree; every
		// surface below — including raw-SQL passthrough — shares the same admin
		// principal set (policy.AdminRole).
		r.Route("/admin", func(r chi.Router) {
			r.Use(requireAdmin)

			// Raw-SQL passthrough. The only sanctioned surface for
			// non-insert mutations (DELETE/UPDATE/TRUNCATE/DROP/ALTER/…)
			// and for ad-hoc SELECTs that don't fit the structured
			// query AST. Authorization is the /v1/admin/* gate above:
			// raw SQL has no per-statement scope check (we can't
			// authorize predicates without a full SQL parser), so the
			// role gate is the entire authorization story. Non-admin
			// callers use the structured ingest path, structured
			// queries (`/v1/query?table={table}`), or named pipes.
			r.Post("/query", deps.Query.Handle)

			if deps.Policy != nil {
				r.Get("/policy", deps.Policy.Get)
				r.Put("/policy", deps.Policy.Put)
				r.Post("/policy/validate", deps.Policy.Validate)
			}
			if deps.Pipes != nil {
				r.Get("/pipes", deps.Pipes.List)
				r.Get("/pipes/{name}", deps.Pipes.Get)
				r.Put("/pipes/{name}", deps.Pipes.Put)
				r.Delete("/pipes/{name}", deps.Pipes.Delete)
			}
			if deps.LogLevel != nil {
				r.Put("/log-level", func(w http.ResponseWriter, r *http.Request) {
					levelStr := r.URL.Query().Get("level")

					var newLevel slog.Level
					if err := newLevel.UnmarshalText([]byte(levelStr)); err != nil {
						writeJSONError(w, http.StatusBadRequest, "invalid or missing level (use debug, info, warn, error)")
						return
					}

					deps.LogLevel.Set(newLevel)
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{
						"status": "success",
						"level":  newLevel.String(),
					})
				})
			}
		})
	})

	return r
}

// jsonRecoverer recovers from panics in downstream handlers and emits a
// JSON 500 via writeJSONError instead of chi/middleware.Recoverer's
// empty-bodied 500 (which leaves Content-Type at the stdlib default).
// Panics are logged with stack and request metadata. http.ErrAbortHandler
// is re-panicked per the chi convention so the server's serve loop still
// terminates the connection cleanly.
//
// If the handler had already written headers or body bytes before the
// panic, the response is not patched: the headers are committed to the
// wire and a JSON 500 appended after them would corrupt the partial
// response. In that case the panic is still logged, but the connection
// is left to terminate as-is.
func jsonRecoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		defer func() {
			rvr := recover()
			if rvr == nil {
				return
			}
			if err, ok := rvr.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(rvr)
			}
			// slog's structured key-value API and its built-in handlers
			// escape control characters in string values, so path/method/
			// err carry no log-injection risk despite the linter's taint
			// heuristic on slog calls inside an *http.Request scope.
			slog.LogAttrs(r.Context(), slog.LevelError, "panic recovered in handler",
				slog.Any("err", rvr),
				slog.String("stack", string(debug.Stack())),
				slog.String("path", r.URL.Path),
				slog.String("method", r.Method),
			)
			if ww.Status() == 0 && ww.BytesWritten() == 0 {
				writeJSONError(ww, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(ww, r)
	})
}

// RequireAdmin restricts a route to the policy admin role (policy.AdminRole —
// configurable via admin_role, "admin" by default). The role established by the
// auth middleware and the live policy are read per request, so an admin_role
// change applies without a restart, and a fresh deployment with no policy yet
// still admits an "admin" token (AdminRole defaults to "admin" for a nil
// policy) so the first policy can be written.
//
// Authentication is decoupled from this gate: a missing/invalid/expired token
// resolves to an empty (non-admin) role and is denied here. Denials go through
// writeAuthzDenied, so a present-but-invalid token fails loud (401 + token
// reason) rather than as a bare 403.
func RequireAdmin(store *policy.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var p *policy.Policy
			if store != nil {
				p = store.Get()
			}
			role := policy.ResolveRole(p, auth.RoleFromContext(r.Context()))
			if policy.IsAdmin(p, role) {
				next.ServeHTTP(w, r)
				return
			}
			writeAuthzDenied(w, r, role)
		})
	}
}

// corsMiddleware handles CORS preflight and response headers.
//
// Origin policy:
//   - allowedOrigins == nil/empty OR contains "*": any origin is accepted.
//     The response echoes "*" (no Vary), which is correct for our Bearer-auth
//     API because no credentials are required to read the response.
//   - Otherwise: only origins in the allowlist receive Access-Control-Allow-Origin
//     (echoed back, with Vary: Origin so caches key on it).
//
// Credentials: we deliberately do NOT emit Access-Control-Allow-Credentials.
// WaveHouse is a Bearer-token API (see internal/api/middleware.go) — clients
// send Authorization: Bearer <jwt> as an explicit request header, not via
// browser-managed cookies, so credentials mode is unnecessary. Sending
// Allow-Credentials: true together with Allow-Origin: "*" is also a CORS
// spec violation that browsers reject. Keeping credentials off both fixes
// the spec violation and shrinks the CSRF surface (see issue #30).
//
// Non-CORS requests (no Origin header) are passed through unchanged — we
// don't decorate same-origin responses with CORS noise.
func corsMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	allowAll := len(allowedOrigins) == 0
	allowedSet := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		if o == "*" {
			allowAll = true
			continue
		}
		allowedSet[o] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Same-origin / non-browser request: skip CORS decoration entirely.
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}

			allowed := false
			switch {
			case allowAll:
				w.Header().Set("Access-Control-Allow-Origin", "*")
				allowed = true
			default:
				// In allowlist mode the response is per-origin, so always set
				// Vary: Origin — even when the origin is rejected. Without
				// this, a shared cache could memoize the headerless reject
				// response under the URL alone and replay it to a later
				// allowed-origin request, stripping the CORS headers and
				// breaking the legitimate client.
				w.Header().Set("Vary", "Origin")
				if _, ok := allowedSet[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					allowed = true
				}
			}

			if allowed {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-Request-ID")
				w.Header().Set("Access-Control-Expose-Headers", "X-Cache, X-Request-ID")
				w.Header().Set("Access-Control-Max-Age", "3600")
			}

			if r.Method == http.MethodOptions {
				// Always short-circuit OPTIONS — disallowed origins simply get a
				// 204 with no CORS headers, which the browser will treat as a
				// preflight failure.
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
