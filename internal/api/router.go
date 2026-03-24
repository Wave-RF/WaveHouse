package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"
)

// Dependencies holds all handler dependencies.
type Dependencies struct {
	Ingest *IngestHandler
	Query  *QueryHandler
	SSE    *SSEHandler
	WS     *WSHandler
	Health *HealthHandler
	AuthMW func(http.Handler) http.Handler
	JS     jetstream.JetStream // for SSE/WS gap-fill
}

// NewRouter creates the chi router with all routes.
func NewRouter(deps Dependencies) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	// Public endpoints.
	r.Get("/health", deps.Health.Liveness)
	r.Get("/ready", deps.Health.Readiness)

	// Authenticated API v1 endpoints.
	r.Route("/v1", func(r chi.Router) {
		r.Use(deps.AuthMW)
		r.Post("/ingest", deps.Ingest.Handle)
		r.Post("/query", deps.Query.Handle)
		r.Get("/stream/sse", deps.SSE.Handle)
		r.Get("/stream/ws", deps.WS.Handle)
	})

	return r
}
