package main

import (
	"context"
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
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfgPath := "config.yaml"
	if p := os.Getenv("WH_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start schema auto-refresh.
	go registry.StartAutoRefresh(ctx)

	// Batch consumer → ClickHouse.
	ingest.StartIngestWorker(
		remoteMQ.NatsConn(),
		cfg.ClickHouse.Addr,
		cfg.ClickHouse.HTTPPort, // Uses 8123 by default
		cfg.ClickHouse.Username,
		cfg.ClickHouse.Password,
		cfg.ClickHouse.Database,
	)

	// Active sweeper — purges processed + expired messages every minute.
	gapWindow := time.Duration(cfg.MQ.GapWindowMinutes) * time.Minute
	sweeper := ingest.NewSweeper(remoteMQ.JetStream(), gapWindow, logger)
	go sweeper.Start(ctx)

	logger.Info("worker started", "mode", "clustered")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down worker")
	cancel()
}
