package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// Pre-populated build info variables, set via ldflags in the Makefile.
var (
	Version   = "dev"
	BuildTime = time.Now().Format(time.RFC3339)
	GitCommit = "unknown"
	Binary    = "clustered-worker"
)

func main() {
	os.Exit(run())
}

// run executes the clustered worker binary and returns a process exit code.
// Using a separate function (rather than os.Exit directly in main) ensures
// deferred cleanups — especially OTEL flush — still run before the process exits.
func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfgPath := "config.yaml"
	if p := os.Getenv("WH_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("load config", "error", err)
		return 1
	}

	// OTEL.
	ctx := context.Background()
	serviceName := "wavehouse-" + Binary
	otelAddr := os.Getenv("WH_OTEL_ADDR")
	if otelAddr == "" {
		otelAddr = "127.0.0.1:4317"
	}

	logger.Info("initializing observability", "endpoint", otelAddr, "service", serviceName)

	otelShutdown, err := observability.InitProvider(ctx, serviceName, otelAddr)
	if err != nil {
		fmt.Printf("FATAL: failed to initialize observability: %v\n", err)
		return 1
	}

	defer func() {
		_ = otelShutdown(context.Background())
	}()

	// Slogger.
	logLevel := &slog.LevelVar{}
	logLevel.Set(slog.LevelInfo) // DEFAULT

	// Create the logger from our observability package
	logger = observability.NewLogger(serviceName, logLevel, true) // true = JSON output
	slog.SetDefault(logger)

	logger.Info("starting WaveHouse", "version", Version, "binary", Binary)

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
		return 1
	}
	defer func() { _ = chConn.Close() }()

	// Schema discovery.
	refreshInterval := time.Duration(cfg.Schema.RefreshInterval) * time.Second
	registry := discovery.NewSchemaRegistry(chConn, cfg.ClickHouse.Database, refreshInterval, logger)
	if err := registry.Refresh(context.Background()); err != nil {
		logger.Error("schema discovery failed on boot", "error", err)
		return 1
	}

	// Remote MQ (NATS).
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	remoteMQ, err := mq.NewRemote(cfg.MQ.URL, cfg.MQ.StreamName, maxBytes)
	if err != nil {
		logger.Error("mq init", "error", err)
		return 1
	}
	defer func() { _ = remoteMQ.Close() }()

	// DLQ stream.
	if cfg.DLQ.Enabled {
		if err := api.EnsureDLQStream(context.Background(), remoteMQ.JetStream(), cfg.MQ.StreamName, maxBytes/10); err != nil {
			logger.Error("dlq stream init", "error", err)
			return 1
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start schema auto-refresh.
	go registry.StartAutoRefresh(ctx)

	// Batch consumer → ClickHouse.
	ingestStream, err := ingest.StartIngestWorker(
		ctx,
		remoteMQ.NatsConn(),
		cfg.MQ.StreamName,
		chConn,
		cfg.ClickHouse.Addr,
		cfg.ClickHouse.HTTPPort, // Uses 8123 by default
		cfg.ClickHouse.Username,
		cfg.ClickHouse.Password,
		cfg.ClickHouse.Database,
	)
	if err != nil {
		logger.Error("ingest worker init", "error", err)
		return 1
	}

	// Active sweeper — purges processed + expired messages every minute.
	gapWindow := time.Duration(cfg.MQ.GapWindowMinutes) * time.Minute
	sweeper := ingest.NewSweeper(remoteMQ.JetStream(), cfg.MQ.StreamName, gapWindow, logger)
	go sweeper.Start(ctx)

	logger.Info("worker started", "mode", "clustered")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down worker")
	cancel()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
	defer shutCancel()
	if err := ingestStream.Stop(shutCtx); err != nil {
		logger.Error("ingest worker drain error", "error", err)
	}
	return 0
}
