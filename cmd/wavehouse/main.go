package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
)

// Pre-populated build info variables, set via ldflags in the Makefile.
var (
	Version   = "dev"
	BuildTime = time.Now().Format(time.RFC3339)
	GitCommit = "unknown"
)

func main() {
	// Subcommand dispatch. Currently only `health` exists — used by the
	// Dockerfile HEALTHCHECK to self-probe /health without needing curl
	// or wget in the (distroless) image. If we ever need more, swap to
	// a real argv router.
	if len(os.Args) > 1 && os.Args[1] == "health" {
		os.Exit(runHealthCheck())
	}
	os.Exit(run())
}

// run executes the binary and returns a process exit code. Using a
// separate function (rather than os.Exit directly in main) ensures deferred
// cleanups — especially OTEL flush — still run before the process exits.
func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("starting WaveHouse", "version", Version, "build_time", BuildTime, "git_commit", GitCommit)

	cfgPath := "config.yaml"
	if p := os.Getenv("WH_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("load config", "error", err)
		return 1
	}

	// Validate auth config.
	if cfg.Auth.Enabled {
		switch cfg.Auth.JWTSecret {
		case "":
			logger.Error("FATAL: WH_AUTH_JWT_SECRET is required when auth is enabled")
			return 1
		case "change-me-in-production":
			logger.Warn("WH_AUTH_JWT_SECRET is using default insecure value")
		}
	}

	ctx := context.Background()
	serviceName := "wavehouse"

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

	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	if cfg.Observability.Enabled {
		otelShutdown, err := observability.InitProvider(ctx, serviceName, observability.ProviderConfig{
			Endpoint:         cfg.Observability.OTelAddr,
			TracesEnabled:    cfg.Observability.Traces.Enabled,
			TracesSampleRate: cfg.Observability.Traces.SampleRate,
			MetricsEnabled:   cfg.Observability.Metrics.Enabled,
			LogsEnabled:      cfg.Observability.Logs.Enabled,
		})
		if err != nil {
			logger.Error("failed to initialize observability, falling back to stdout", "error", err)
		} else {
			defer func() {
				_ = otelShutdown(context.Background())
			}()

			otelLogger := observability.NewLogger(serviceName, logLevel, true, cfg.Observability.Logs.SampleRate)
			logger = otelLogger.With(
				"version", Version,
				"build_time", BuildTime,
				"git_commit", GitCommit,
			)
			slog.SetDefault(logger)
			logger.Info("observability pipeline established", "endpoint", cfg.Observability.OTelAddr)
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
		return 1
	}
	defer func() { _ = chConn.Close() }()

	// Schema discovery — discover ClickHouse table schemas on boot.
	refreshInterval := time.Duration(cfg.Schema.RefreshInterval) * time.Second
	registry := discovery.NewSchemaRegistry(chConn, cfg.ClickHouse.Database, refreshInterval, logger)
	if err := registry.Refresh(context.Background()); err != nil {
		logger.Error("schema discovery failed on boot", "error", err)
		return 1
	}

	// State directories — one configurable root, fixed subdir convention.
	natsDir := filepath.Join(cfg.DataDir, "nats")
	pebbleDir := filepath.Join(cfg.DataDir, "pebble")

	// Optional embedded dedupe (Pebble).
	var dedup dedupe.Deduplicator
	if cfg.Dedupe.Enabled {
		config.WarnIfFreshDataDir(logger, "pebble", pebbleDir)
		dedup, err = dedupe.NewEmbedded(pebbleDir)
		if err != nil {
			config.LogStorageInitError(logger, "dedupe", pebbleDir, err)
			return 1
		}
		defer func() { _ = dedup.Close() }()
	}

	// Embedded MQ (NATS).
	config.WarnIfFreshDataDir(logger, "nats", natsDir)
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	embeddedMQ, err := mq.NewEmbedded(natsDir, maxBytes)
	if err != nil {
		config.LogStorageInitError(logger, "mq", natsDir, err)
		return 1
	}
	defer func() { _ = embeddedMQ.Close() }()

	if err := observability.RegisterSystemMetrics(embeddedMQ.GetServer(), dedup); err != nil {
		logger.Error("failed to register system metrics", "error", err)
	}

	// DLQ stream.
	if cfg.DLQ.Enabled {
		if err := api.EnsureDLQStream(context.Background(), embeddedMQ.JetStream(), maxBytes/10); err != nil {
			logger.Error("dlq stream init", "error", err)
			return 1
		}
	}

	// L1 cache only in standalone mode.
	l1, err := cache.NewLocal(cfg.Cache.L1MaxCost)
	if err != nil {
		logger.Error("cache init", "error", err)
		return 1
	}
	tiered := cache.NewTiered(l1, nil)
	defer func() { _ = tiered.Close() }()

	// Policy store (NATS KV + optional file bootstrap).
	policyStore, err := policy.NewStore(context.Background(), embeddedMQ.JetStream(), cfg.Policy.FilePath, logger)
	if err != nil {
		logger.Error("policy store init", "error", err)
		return 1
	}

	// Pipes store (NATS KV + optional SQL file directory).
	pipesStore, err := pipes.NewStore(context.Background(), embeddedMQ.JetStream(), cfg.Pipes.Dir, logger)
	if err != nil {
		logger.Error("pipes store init", "error", err)
		return 1
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
		cfg.ClickHouse.HTTPScheme,
		cfg.ClickHouse.Username,
		cfg.ClickHouse.Password,
		cfg.ClickHouse.Database,
		func(fatalErr error) {
			slog.Error("ingest worker died, initiating graceful shutdown", "error", fatalErr)
			cancel()
		},
	)
	if err != nil {
		logger.Error("ingest worker init", "error", err)
		return 1
	}
	// Start active sweeper.
	go sweeper.Start(ctx)

	// Hub bridge: MQ → broadcast to connected SSE/WS clients.
	if err := embeddedMQ.Subscribe(ctx, "ingest.>", "hub-bridge", func(msg *mq.Message) error {
		var evt ingest.EventMessage
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			if err := msg.Ack(); err != nil {
				slog.Warn("failed to ack message from embedded hub bridge", "error", err)
			}
			return nil
		}
		hub.Broadcast(msg.Subject, msg)
		if err := msg.Ack(); err != nil {
			slog.Warn("failed to ack message from embedded hub bridge", "error", err)
		}
		return nil
	}); err != nil {
		logger.Error("hub bridge start", "error", err)
		return 1
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
		AuthEnabled: cfg.Auth.Enabled,
		JS:          js,
		CORSOrigins: cfg.Server.CORSAllowedOrigins,
		LogLevel:    logLevel,
	}
	router := api.NewRouter(deps)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
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
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}
	}()

	logger.Info("starting server", "port", cfg.Server.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "error", err)
		return 1
	}
	return 0
}
