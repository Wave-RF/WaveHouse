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
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/google/uuid"

)

// Pre-populated build info variables, set via ldflags in the Makefile.
var (
	Version   = "dev"
	BuildTime = time.Now().Format(time.RFC3339)
	GitCommit = "unknown"
	Binary    = "clustered-api"
)

func main() {
	var level slog.Level
    switch os.Getenv("WH_LOG_LEVEL") {
    case "DEBUG":
        level = slog.LevelDebug
    case "WARN":
        level = slog.LevelWarn
    case "ERROR":
        level = slog.LevelError
    default:
        level = slog.LevelInfo 
    }

	logLevel := &slog.LevelVar{}
	logLevel.Set(level)
	
	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel, AddSource: true}))

	logger := baseLogger.With("version", Version, "build_time", BuildTime, "git_commit", GitCommit, "binary", Binary,)

	slog.SetDefault(logger)
		logger.Info("starting WaveHouse API", "version", Version, "build_time", BuildTime, "git_commit", GitCommit, "binary", Binary)

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

	ctx := context.Background()
	serviceName := "wavehouse-" + Binary
	
	if cfg.Observability.Enabled {
		otelShutdown, err := observability.InitProvider(ctx, serviceName, cfg.Observability.OTelAddr)
		if err != nil {
			logger.Error("failed to initialize observability, falling back to stdout", "error", err)
		} else {
			defer func() {
				_ = otelShutdown(context.Background())
			}()

			otelLogger := observability.NewLogger(serviceName, logLevel, true)
			logger = otelLogger.With(
				"version", Version,
				"build_time", BuildTime,
				"git_commit", GitCommit,
				"binary", Binary,
			)
			slog.SetDefault(logger)
			logger.Info("observability pipeline established", "endpoint", cfg.Observability.OTelAddr)
		}
	}

	// ClickHouse.
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

	// Schema discovery.
	refreshInterval := time.Duration(cfg.Schema.RefreshInterval) * time.Second
	registry := discovery.NewSchemaRegistry(chConn, cfg.ClickHouse.Database, refreshInterval, logger)
	if err := registry.Refresh(context.Background()); err != nil {
		logger.Error("schema discovery failed on boot", "error", err)
		os.Exit(1)
	}

	// Optional distributed dedupe (ScyllaDB).
	var dedup dedupe.Deduplicator
	if cfg.Dedupe.Enabled {
		dedup, err = dedupe.NewDistributed(cfg.Dedupe.ScyllaHosts, cfg.Dedupe.ScyllaKeyspace)
		if err != nil {
			logger.Error("dedupe init", "error", err)
			os.Exit(1)
		}
		defer dedup.Close()
	}

	// Remote MQ (NATS).
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	remoteMQ, err := mq.NewRemote(cfg.MQ.URL, maxBytes)
	if err != nil {
		logger.Error("mq init", "error", err)
		os.Exit(1)
	}
	defer remoteMQ.Close()

	// DLQ stream.
	if cfg.DLQ.Enabled {
		if err := api.EnsureDLQStream(context.Background(), remoteMQ.JetStream(), maxBytes/10); err != nil {
			logger.Error("dlq stream init", "error", err)
			os.Exit(1)
		}
	}

	// Tiered cache (L1 + L2 Redis).
	l1, err := cache.NewLocal(cfg.Cache.L1MaxCost)
	if err != nil {
		logger.Error("l1 cache init", "error", err)
		os.Exit(1)
	}
	l2, err := cache.NewShared(cfg.Cache.RedisURL)
	if err != nil {
		logger.Error("l2 cache init", "error", err)
		os.Exit(1)
	}
	tiered := cache.NewTiered(l1, l2)
	defer tiered.Close()

	// Policy store (NATS KV + optional file bootstrap).
	policyStore, err := policy.NewStore(context.Background(), remoteMQ.JetStream(), cfg.Policy.FilePath, logger)
	if err != nil {
		logger.Error("policy store init", "error", err)
		os.Exit(1)
	}

	// Pipes store (NATS KV + optional SQL file directory).
	pipesStore, err := pipes.NewStore(context.Background(), remoteMQ.JetStream(), cfg.Pipes.Directory, logger)
	if err != nil {
		logger.Error("pipes store init", "error", err)
		os.Exit(1)
	}

	hub := api.NewHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start schema auto-refresh.
	go registry.StartAutoRefresh(ctx)

	// Start policy watch for cluster-wide updates.
	go policyStore.Watch(ctx)

	// Hub bridge: MQ → broadcast to connected clients.
	consumerName := "hub-bridge-" + uuid.New().String()
	if err := remoteMQ.Subscribe(ctx, "ingest.>", consumerName, func(msg *mq.Message) error {
		var evt ingest.EventMessage
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			msg.Ack()
			return nil
		}
		hub.Broadcast(msg.Subject, msg)
		msg.Ack()
		return nil
	}); err != nil {
		logger.Error("hub bridge subscription failed", "error", err)
		os.Exit(1)
	}

	// Build handlers.
	js := remoteMQ.JetStream()
	ingestHandler := api.NewIngestHandler(registry, remoteMQ)
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

	sseHandler := api.NewSSEHandler(hub, remoteMQ.JetStream())
	sseHandler.PolicyStore = policyStore

	wsHandler := api.NewWSHandler(hub, remoteMQ.JetStream(), cfg.Server.CORSAllowedOrigins)
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
		JS:          remoteMQ.JetStream(),
		CORSOrigins: cfg.Server.CORSAllowedOrigins,
		LogLevel:    logLevel,
	}
	router := api.NewRouter(deps)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: router,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
		defer shutCancel()
		cancel()
		srv.Shutdown(shutCtx)
	}()

	logger.Info("starting api server", "port", cfg.Server.Port, "mode", "clustered")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
