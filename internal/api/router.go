package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

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
	AuthEnabled     bool
	JS              jetstream.JetStream // for SSE/WS gap-fill
	CORSOrigins     []string            // allowed CORS origins; ["*"] = allow all
	LogLevel        *slog.LevelVar
}

// NewRouter creates the chi router with all routes.
func NewRouter(deps Dependencies) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware(deps.CORSOrigins))

	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// BYPASS: Do not use standard HTTP tracing for long-lived streams
			if strings.HasPrefix(r.URL.Path, "/v1/stream/") {
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

	// API v1 endpoints (auth middleware may be no-op if disabled).
	r.Route("/v1", func(r chi.Router) {
		r.Use(deps.AuthMW)
		r.Post("/ingest/{table}", deps.Ingest.Handle)
		r.Post("/query", deps.Query.Handle)
		r.Get("/stream/sse", deps.SSE.Handle)
		r.Get("/stream/ws", deps.WS.Handle)

		// Schema discovery.
		r.Get("/schema", deps.Schema.List)
		r.Get("/schema/{table}", deps.Schema.Get)
		r.Post("/schema/refresh", deps.Schema.Refresh)

		// Structured query endpoint.
		if deps.StructuredQuery != nil {
			r.Post("/tables/{table}/query", deps.StructuredQuery.Handle)
		}

		// Named query pipes.
		if deps.Pipes != nil {
			r.Get("/pipes/{name}", deps.Pipes.Execute)
			r.Post("/pipes/{name}", deps.Pipes.Execute)
		}

		// DLQ stats.
		if deps.DLQ != nil {
			r.Get("/dlq/stats", deps.DLQ.Stats)
		}

		// Admin routes (require admin or service role).
		r.Route("/admin", func(r chi.Router) {
			r.Use(RequireRole(deps.AuthEnabled, "admin", "service"))
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

// RequireRole returns middleware that restricts access to the given roles.
// When no role is present in the request context, access is allowed only if
// authEnabled is false. If authEnabled is true, the request is denied (fail-closed)
// with a 401 Unauthorized response.
func RequireRole(authEnabled bool, roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role := RoleFromContext(r.Context())
			// Fail-closed: if auth is explicitly enabled, an empty role is a security failure.
			if role == "" {
				if !authEnabled {
					// Auth is disabled globally; allow for dev/testing.
					next.ServeHTTP(w, r)
					return
				}
				// FAIL-CLOSED: Auth is enabled, but no role was found in the context.
				writeJSONError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			for _, allowed := range roles {
				if role == allowed {
					next.ServeHTTP(w, r)
					return
				}
			}
			writeJSONError(w, http.StatusForbidden, "forbidden")
		})
	}
}

// corsMiddleware handles CORS preflight and response headers.
// When allowedOrigins contains "*", any origin is accepted.
// Otherwise only listed origins receive CORS headers.
func corsMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	allowAll := len(allowedOrigins) == 0
	allowedSet := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		if o == "*" {
			allowAll = true
		}
		allowedSet[o] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if _, ok := allowedSet[origin]; ok && origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
			}

			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-Request-ID")
			w.Header().Set("Access-Control-Expose-Headers", "X-Cache, X-Request-ID")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Max-Age", "3600")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
