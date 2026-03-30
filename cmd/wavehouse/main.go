package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// Pre-populated build info variables, set via ldflags in the Makefile.
var (
	Version   = "dev"
	BuildTime = time.Now().Format(time.RFC3339)
	GitCommit = "unknown"
	Binary    = "standalone"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("starting WaveHouse", "version", Version, "build_time", BuildTime, "git_commit", GitCommit, "binary", Binary)

	cfgPath := "config.yaml"
	if p := os.Getenv("WH_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	// Validate auth config.
	if cfg.Auth.Enabled {
		switch cfg.Auth.JWTSecret {
		case "":
			logger.Error("FATAL: WH_AUTH_JWT_SECRET is required when auth is enabled")
			os.Exit(1)
		case "change-me-in-production":
			logger.Warn("WH_AUTH_JWT_SECRET is using default insecure value")
		}
	}

	// ClickHouse connection.
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouse.Database,
			Username: cfg.ClickHouse.Username,
			Password: cfg.ClickHouse.Password,
		},
	})
	if err != nil {
		logger.Error("clickhouse open", "error", err)
		os.Exit(1)
	}
	defer chConn.Close()

	// Schema discovery — discover ClickHouse table schemas on boot.
	refreshInterval := time.Duration(cfg.Schema.RefreshInterval) * time.Second
	registry := discovery.NewSchemaRegistry(chConn, cfg.ClickHouse.Database, refreshInterval, logger)
	if err := registry.Refresh(context.Background()); err != nil {
		logger.Error("schema discovery failed on boot", "error", err)
		os.Exit(1)
	}

	// Optional embedded dedupe (Pebble).
	var dedup dedupe.Deduplicator
	if cfg.Dedupe.Enabled {
		dedup, err = dedupe.NewEmbedded(cfg.Dedupe.EmbeddedDir)
		if err != nil {
			logger.Error("dedupe init", "error", err)
			os.Exit(1)
		}
		defer dedup.Close()
	}

	// Embedded MQ (NATS).
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	embeddedMQ, err := mq.NewEmbedded(cfg.MQ.EmbeddedDir, maxBytes)
	if err != nil {
		logger.Error("mq init", "error", err)
		os.Exit(1)
	}
	defer embeddedMQ.Close()

	// DLQ stream.
	if cfg.DLQ.Enabled {
		if err := api.EnsureDLQStream(context.Background(), embeddedMQ.JetStream(), maxBytes/10); err != nil {
			logger.Error("dlq stream init", "error", err)
			os.Exit(1)
		}
	}

	// L1 cache only in standalone mode.
	l1, err := cache.NewLocal(cfg.Cache.L1MaxCost)
	if err != nil {
		logger.Error("cache init", "error", err)
		os.Exit(1)
	}
	tiered := cache.NewTiered(l1, nil)
	defer tiered.Close()

	// Policy store (NATS KV + optional file bootstrap).
	policyStore, err := policy.NewStore(context.Background(), embeddedMQ.JetStream(), cfg.Policy.FilePath, logger)
	if err != nil {
		logger.Error("policy store init", "error", err)
		os.Exit(1)
	}

	// Pipes store (NATS KV + optional SQL file directory).
	pipesStore, err := pipes.NewStore(context.Background(), embeddedMQ.JetStream(), cfg.Pipes.Directory, logger)
	if err != nil {
		logger.Error("pipes store init", "error", err)
		os.Exit(1)
	}

	// Active sweeper — purges messages that are both written to CH and
	// older than the SSE gap window. Runs every minute.
	gapWindow := time.Duration(cfg.MQ.GapWindowMinutes) * time.Minute
	sweeper := ingest.NewSweeper(embeddedMQ.JetStream(), gapWindow, logger)

	// Hub for streaming fan-out.
	hub := api.NewHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start schema auto-refresh.
	go registry.StartAutoRefresh(ctx)

	// Start policy watch for cluster-wide updates.
	go policyStore.Watch(ctx)

	// Start batch consumer → ClickHouse.
	ingestStream, err := ingest.StartIngestWorker(
		ctx,
		embeddedMQ.NatsConn(),
		chConn,
		cfg.ClickHouse.Addr,
		cfg.ClickHouse.HTTPPort, // Uses 8123 by default
		cfg.ClickHouse.Username,
		cfg.ClickHouse.Password,
		cfg.ClickHouse.Database,
	)
	if err != nil {
		logger.Error("ingest worker init", "error", err)
		os.Exit(1)
	}
	// Start active sweeper.
	go sweeper.Start(ctx)

	// Hub bridge: MQ → broadcast to connected SSE/WS clients.
	if err := embeddedMQ.Subscribe(ctx, "ingest.>", "hub-bridge", func(msg *mq.Message) error {
		var evt ingest.EventMessage
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			msg.Ack()
			return nil
		}
		hub.Broadcast(msg.Subject, evt)
		msg.Ack()
		return nil
	}); err != nil {
		logger.Error("hub bridge start", "error", err)
		os.Exit(1)
	}

	// Build handlers.
	js := embeddedMQ.JetStream()
	ingestHandler := api.NewIngestHandler(registry, embeddedMQ)
	ingestHandler.PolicyStore = policyStore
	if dedup != nil {
		ingestHandler.Dedup = dedup
		ingestHandler.IDField = cfg.Dedupe.IDField
	}

	var dlqHandler *api.DLQHandler
	if cfg.DLQ.Enabled {
		dlqHandler = api.NewDLQHandler(js, logger)
	}

	queryHandler := api.NewQueryHandler(chConn, tiered, time.Duration(cfg.Cache.DefaultTTL)*time.Second)
	queryHandler.PolicyStore = policyStore

	sseHandler := api.NewSSEHandler(hub, js)
	sseHandler.PolicyStore = policyStore

	wsHandler := api.NewWSHandler(hub, js, cfg.Server.CORSAllowedOrigins)
	wsHandler.PolicyStore = policyStore

	deps := api.Dependencies{
		Ingest:          ingestHandler,
		Query:           queryHandler,
		SSE:             sseHandler,
		WS:              wsHandler,
		Health:          api.NewHealthHandler(chConn),
		Schema:          api.NewSchemaHandler(registry),
		DLQ:             dlqHandler,
		Policy:          api.NewPolicyHandler(policyStore),
		Pipes:           api.NewPipesHandler(pipesStore, chConn, tiered, time.Duration(cfg.Cache.DefaultTTL)*time.Second),
		StructuredQuery: api.NewStructuredQueryHandler(chConn, tiered, time.Duration(cfg.Cache.DefaultTTL)*time.Second, registry, policyStore, cfg.Cache.TimestampBucketSeconds),
		AuthMW: api.JWTAuthMiddleware(api.AuthConfig{
			Enabled:   cfg.Auth.Enabled,
			JWTSecret: cfg.Auth.JWTSecret,
			JWKSURL:   cfg.Auth.JWKSURL,
			RoleClaim: cfg.Auth.RoleClaim,
			DevMode:   cfg.Auth.DevMode,
		}),
		JS:          js,
		CORSOrigins: cfg.Server.CORSAllowedOrigins,
	}
	router := api.NewRouter(deps)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: router,
	}

	// Graceful shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
		defer shutCancel()
		cancel()
		_ = ingestStream.Stop(shutCtx)
		srv.Shutdown(shutCtx)
	}()

	logger.Info("starting server", "port", cfg.Server.Port, "mode", "standalone")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
