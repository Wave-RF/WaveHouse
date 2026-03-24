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
	"github.com/google/uuid"
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

	// Auto-migrate ScyllaDB schema.
	if *cfg.Dedupe.AutoMigrate {
		if err := dedupe.EnsureSchema(cfg.Dedupe.ScyllaHosts, cfg.Dedupe.ScyllaKeyspace); err != nil {
			logger.Error("scylladb schema migration", "error", err)
			os.Exit(1)
		}
		logger.Info("scylladb schema ensured")
	}

	// Distributed dedupe (ScyllaDB).
	dedup, err := dedupe.NewDistributed(cfg.Dedupe.ScyllaHosts, cfg.Dedupe.ScyllaKeyspace)
	if err != nil {
		logger.Error("dedupe init", "error", err)
		os.Exit(1)
	}
	defer dedup.Close()

	// Remote MQ (NATS).
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	remoteMQ, err := mq.NewRemote(cfg.MQ.URL, maxBytes)
	if err != nil {
		logger.Error("mq init", "error", err)
		os.Exit(1)
	}
	defer remoteMQ.Close()

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

	hub := api.NewHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Hub bridge: MQ → broadcast to connected clients.
	consumerName := "hub-bridge-" + uuid.New().String()
	// TODO: ^ may want to create using hostname or PID or something for easier debugging and monitoring, but needs to be unique to avoid collisions in clustered mode
	_ = remoteMQ.Subscribe(ctx, "ingest.>", consumerName, func(msg *mq.Message) error {
		var evt ingest.EventMessage
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			msg.Ack()
			return nil
		}
		hub.Broadcast("ingest.events", evt)
		msg.Ack()
		return nil
	})

	deps := api.Dependencies{
		Ingest: api.NewIngestHandler(dedup, remoteMQ),
		Query:  api.NewQueryHandler(chConn, tiered, time.Duration(cfg.Cache.DefaultTTL)*time.Second),
		SSE:    api.NewSSEHandler(hub, remoteMQ.JetStream()),
		WS:     api.NewWSHandler(hub, remoteMQ.JetStream()),
		Health: api.NewHealthHandler(chConn),
		AuthMW: api.JWTAuthMiddleware(cfg.Auth.JWTSecret),
		JS:     remoteMQ.JetStream(),
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
