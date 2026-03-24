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
	"github.com/Wave-RF/BeachHouse/internal/api"
	"github.com/Wave-RF/BeachHouse/internal/cache"
	"github.com/Wave-RF/BeachHouse/internal/config"
	"github.com/Wave-RF/BeachHouse/internal/dedupe"
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

	// SECURITY: Hard stop if JWT secret is missing or warn but continue if using default
	switch cfg.Auth.JWTSecret {
	case "":
		logger.Error("FATAL: BH_AUTH_JWT_SECRET is missing")
		os.Exit(1)
	case "change-me-in-production":
		logger.Warn("BH_AUTH_JWT_SECRET is using default insecure value")
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

	// Auto-migrate ClickHouse schema.
	if *cfg.ClickHouse.AutoMigrate {
		if err := ingest.EnsureSchema(context.Background(), chConn); err != nil {
			logger.Error("clickhouse schema migration", "error", err)
			os.Exit(1)
		}
		logger.Info("clickhouse schema ensured")
	}

	// Embedded dedupe (Pebble).
	dedup, err := dedupe.NewEmbedded(cfg.Dedupe.EmbeddedDir)
	if err != nil {
		logger.Error("dedupe init", "error", err)
		os.Exit(1)
	}
	defer dedup.Close()

	// Embedded MQ (NATS).
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	embeddedMQ, err := mq.NewEmbedded(cfg.MQ.EmbeddedDir, maxBytes)
	if err != nil {
		logger.Error("mq init", "error", err)
		os.Exit(1)
	}
	defer embeddedMQ.Close()

	// L1 cache only in standalone mode.
	l1, err := cache.NewLocal(cfg.Cache.L1MaxCost)
	if err != nil {
		logger.Error("cache init", "error", err)
		os.Exit(1)
	}
	tiered := cache.NewTiered(l1, nil)
	defer tiered.Close()

	// Active sweeper — purges messages that are both written to CH and
	// older than the SSE gap window. Runs every minute.
	gapWindow := time.Duration(cfg.MQ.GapWindowMinutes) * time.Minute
	sweeper := ingest.NewSweeper(embeddedMQ.JetStream(), gapWindow, logger)

	// Hub for streaming fan-out.
	hub := api.NewHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start batch consumer → ClickHouse.
	bufferConsumer := ingest.NewBufferConsumer(embeddedMQ, chConn, 1000, 5*time.Second, logger)
	if err := bufferConsumer.Start(ctx); err != nil {
		logger.Error("buffer consumer start", "error", err)
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
		hub.Broadcast("ingest.events", evt)
		msg.Ack()
		return nil
	}); err != nil {
		logger.Error("hub bridge start", "error", err)
		os.Exit(1)
	}

	// Build router with all dependencies.
	js := embeddedMQ.JetStream()
	deps := api.Dependencies{
		Ingest: api.NewIngestHandler(dedup, embeddedMQ),
		Query:  api.NewQueryHandler(chConn, tiered, time.Duration(cfg.Cache.DefaultTTL)*time.Second),
		SSE:    api.NewSSEHandler(hub, js),
		WS:     api.NewWSHandler(hub, js),
		Health: api.NewHealthHandler(chConn),
		AuthMW: api.JWTAuthMiddleware(cfg.Auth.JWTSecret),
		JS:     js,
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
		srv.Shutdown(shutCtx)
	}()

	logger.Info("starting server", "port", cfg.Server.Port, "mode", "standalone")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
