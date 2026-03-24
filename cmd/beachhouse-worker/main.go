package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Wave-RF/BeachHouse/internal/config"
	"github.com/Wave-RF/BeachHouse/internal/ingest"
	"github.com/Wave-RF/BeachHouse/internal/mq"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfgPath := "config.yaml"
	if p := os.Getenv("BH_CONFIG"); p != "" {
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

	// Auto-migrate ClickHouse schema.
	if *cfg.ClickHouse.AutoMigrate {
		if err := ingest.EnsureSchema(context.Background(), chConn); err != nil {
			logger.Error("clickhouse schema migration", "error", err)
			os.Exit(1)
		}
		logger.Info("clickhouse schema ensured")
	}

	// Remote MQ (NATS).
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	remoteMQ, err := mq.NewRemote(cfg.MQ.URL, maxBytes)
	if err != nil {
		logger.Error("mq init", "error", err)
		os.Exit(1)
	}
	defer remoteMQ.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Batch consumer → ClickHouse.
	bufferConsumer := ingest.NewBufferConsumer(remoteMQ, chConn, 1000, 5*time.Second, logger)
	if err := bufferConsumer.Start(ctx); err != nil {
		logger.Error("buffer consumer start", "error", err)
		os.Exit(1)
	}

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
