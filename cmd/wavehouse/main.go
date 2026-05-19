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
	"strings"
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
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("WH_LOG_LEVEL"))) {
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

	var promHandler http.Handler
	// Provider init runs whenever either OTLP push or Prometheus exposition is
	// wanted — Prometheus-only operation (Alloy/scrape, no collector) is a
	// first-class mode. The OTel SDK MeterProvider is the shared substrate.
	if cfg.OTel.Enabled || cfg.Prometheus.Enabled {
		otelShutdown, ph, err := observability.InitProvider(ctx, serviceName, observability.ProviderConfig{
			Endpoint:          cfg.OTel.Addr,
			TracesEnabled:     cfg.OTel.Enabled && cfg.OTel.Traces.Enabled,
			TracesSampleRate:  cfg.OTel.Traces.SampleRate,
			MetricsEnabled:    cfg.OTel.Enabled && cfg.OTel.Metrics.Enabled,
			PrometheusEnabled: cfg.Prometheus.Enabled,
			LogsEnabled:       cfg.OTel.Enabled && cfg.OTel.Logs.Enabled,
		})
		if err != nil {
			logger.Error("failed to initialize observability, falling back to stdout", "error", err)
		} else {
			promHandler = ph
			defer func() {
				// Bound shutdown so an unreachable collector doesn't hang
				// process exit. The OTel SDK's batch processors don't fully
				// honor the context deadline during gRPC retry/backoff.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = otelShutdown(ctx)
			}()

			// Only swap to the OTLP-aware logger when OTLP logs are wired up;
			// Prometheus-only mode keeps the stdout-only handler set earlier.
			if cfg.OTel.Enabled && cfg.OTel.Logs.Enabled {
				otelLogger := observability.NewLogger(serviceName, logLevel, true, cfg.OTel.Logs.SampleRate)
				logger = otelLogger.With(
					"version", Version,
					"build_time", BuildTime,
					"git_commit", GitCommit,
				)
				slog.SetDefault(logger)
			}
			switch {
			case cfg.OTel.Enabled && cfg.Prometheus.Enabled:
				logger.Info("observability pipeline established", "otlp_endpoint", cfg.OTel.Addr, "prometheus", true)
			case cfg.OTel.Enabled:
				logger.Info("observability pipeline established", "otlp_endpoint", cfg.OTel.Addr)
			case cfg.Prometheus.Enabled:
				logger.Info("observability pipeline established", "prometheus", true)
			}
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

	// Process-lifetime context — cancelled by the SIGINT/SIGTERM handler
	// below. Created here (instead of further down) so the boot-time schema
	// discovery retry goroutine ties cleanly to shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Schema discovery — non-fatal on boot. If the first Refresh fails
	// (ClickHouse unreachable, database missing, etc.) we mark the binary
	// degraded via bootState (which /health surfaces as 503 + diagnostic)
	// and retry in the background with exponential backoff. The process
	// still binds :8080 so operators can `curl /health` instead of grepping
	// a restart-loop log. Once a Refresh succeeds, bootState flips to nil
	// and /health returns 200. The periodic auto-refresh ticker is started
	// only after the first successful Refresh (sync or retry) so it never
	// races RetryRefresh on Refresh calls or on bootState writes.
	bootState := api.NewBootState(nil)
	refreshInterval := time.Duration(cfg.Schema.RefreshInterval) * time.Second
	registry := discovery.NewSchemaRegistry(chConn, cfg.ClickHouse.Database, refreshInterval, logger)
	if err := registry.Refresh(ctx); err != nil {
		logger.Warn("schema discovery failed on boot, retrying in background", "error", err)
		bootState.Set(fmt.Errorf("schema discovery: %w", err))
		go func() {
			retryErr := registry.RetryRefresh(ctx, 2*time.Second, 60*time.Second, func(attemptErr error) {
				logger.Warn("schema discovery retry failed", "error", attemptErr)
				bootState.Set(fmt.Errorf("schema discovery: %w", attemptErr))
			})
			if retryErr != nil {
				// ctx cancelled before success — process is shutting down.
				return
			}
			logger.Info("schema discovery succeeded after retry, /health now 200")
			bootState.Set(nil)
			go registry.StartAutoRefresh(ctx)
		}()
	} else {
		go registry.StartAutoRefresh(ctx)
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

	// Only register system metric gauges when a real MeterProvider is in
	// place — otherwise `otel.GetMeterProvider()` returns the no-op SDK
	// provider and RegisterCallback silently no-ops, making this look
	// authoritative when it's actually doing nothing.
	if cfg.OTel.Enabled || cfg.Prometheus.Enabled {
		if err := observability.RegisterSystemMetrics(embeddedMQ.GetServer(), dedup); err != nil {
			logger.Error("failed to register system metrics", "error", err)
		}
	}

	// DLQ stream.
	if cfg.DLQ.Enabled {
		if err := api.EnsureDLQStream(ctx, embeddedMQ.JetStream(), maxBytes/10); err != nil {
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
	policyStore, err := policy.NewStore(ctx, embeddedMQ.JetStream(), cfg.Policy.FilePath, logger)
	if err != nil {
		logger.Error("policy store init", "error", err)
		return 1
	}

	// Pipes store (NATS KV + optional SQL file directory).
	pipesStore, err := pipes.NewStore(ctx, embeddedMQ.JetStream(), cfg.Pipes.Dir, logger)
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

	healthHandler := api.NewHealthHandler(chConn)
	healthHandler.Boot = bootState

	sseHandler := api.NewSSEHandler(hub, js)
	sseHandler.PolicyStore = policyStore

	wsHandler := api.NewWSHandler(hub, js, cfg.Server.CORSAllowedOrigins)
	wsHandler.PolicyStore = policyStore

	deps := api.Dependencies{
		Ingest:          ingestHandler,
		Query:           queryHandler,
		SSE:             sseHandler,
		WS:              wsHandler,
		Health:          healthHandler,
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

	// Prometheus /metrics routing: same-port → mount on API router,
	// dedicated port → spin a sidecar HTTP server below.
	promPath := cfg.Prometheus.Path
	promPort := cfg.Prometheus.Port
	var promSrv *http.Server
	if promHandler != nil {
		if promPort == 0 {
			deps.MetricsHandler = promHandler
			deps.MetricsPath = promPath
		} else {
			mux := http.NewServeMux()
			mux.Handle(promPath, promHandler)
			promSrv = &http.Server{
				Addr:              fmt.Sprintf(":%d", promPort),
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
				// Full Read/Write timeouts are safe here — unlike the main API
				// server (SSE/WebSocket), the Prometheus sidecar serves only
				// single-shot scrape requests, so an unbounded slow client has
				// no legitimate reason to hold a connection.
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
			}
		}
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
		if promSrv != nil {
			if err := promSrv.Shutdown(shutCtx); err != nil {
				logger.Error("prometheus server shutdown error", "error", err)
			}
		}
	}()

	if promSrv != nil {
		go func() {
			logger.Info("starting prometheus metrics server", "addr", promSrv.Addr, "path", promPath)
			if err := promSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("prometheus server error", "error", err)
			}
		}()
	}

	logger.Info("starting server", "port", cfg.Server.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "error", err)
		return 1
	}
	return 0
}
