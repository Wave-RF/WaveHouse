//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestClickHouseContainer(t *testing.T) {
	ctx := context.Background()

	chReq := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		WaitingFor:   wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer chContainer.Terminate(ctx)

	host, err := chContainer.Host(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, host)

	port, err := chContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)
	assert.NotEmpty(t, port)

	t.Logf("ClickHouse available at %s:%s", host, port.Port())
}

func TestNATSContainer(t *testing.T) {
	ctx := context.Background()

	natsReq := testcontainers.ContainerRequest{
		Image:        "nats:latest",
		Cmd:          []string{"--jetstream"},
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(30 * time.Second),
	}
	natsContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: natsReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer natsContainer.Terminate(ctx)

	host, err := natsContainer.Host(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, host)

	t.Logf("NATS available at %s", host)
}

// testEnv holds shared infrastructure for DLQ integration tests.
type testEnv struct {
	chContainer  testcontainers.Container
	chHost       string
	chNativePort string
	chHTTPPort   string
	chConn       driver.Conn
	embeddedMQ   *mq.EmbeddedNATS
	router       http.Handler
	server       *httptest.Server
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()
	logger := slog.Default()

	// Start ClickHouse container.
	chReq := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env:          map[string]string{"CLICKHOUSE_PASSWORD": "test"},
		WaitingFor:   wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	require.NoError(t, err)

	chHost, err := chContainer.Host(ctx)
	require.NoError(t, err)
	nativePort, err := chContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)
	httpPort, err := chContainer.MappedPort(ctx, "8123")
	require.NoError(t, err)

	chAddr := fmt.Sprintf("%s:%s", chHost, nativePort.Port())
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{Database: "default", Username: "default", Password: "test"},
	})
	require.NoError(t, err)

	// Create test table.
	err = chConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_events (
			user_id String,
			event_type String,
			value Float64
		) ENGINE = MergeTree()
		ORDER BY user_id
	`)
	require.NoError(t, err)

	// Start embedded NATS.
	embeddedMQ, err := mq.NewEmbedded(t.TempDir(), 10*1024*1024, testutil.NopLogger()) // 10 MB
	require.NoError(t, err)

	js := embeddedMQ.JetStream()

	// Create DLQ stream.
	require.NoError(t, api.EnsureDLQStream(ctx, js, 1024*1024))

	// Schema registry.
	registry := discovery.NewSchemaRegistry(chConn, "default", time.Minute, logger)
	require.NoError(t, registry.Refresh(ctx))

	// Start Bento ingest worker.
	_, err = ingest.StartIngestWorker(
		ctx,
		embeddedMQ.NatsConn(),
		chConn,
		chAddr,
		httpPort.Port(),
		"default",
		"test",
		"default",
	)
	require.NoError(t, err)

	// Hub for SSE/WS.
	hub := api.NewHub()

	// Build cache.
	l1, err := cache.NewLocal(1024 * 1024)
	require.NoError(t, err)
	tiered := cache.NewTiered(l1, nil)

	// Wire handlers.
	ingestHandler := api.NewIngestHandler(registry, embeddedMQ)
	queryHandler := api.NewQueryHandler(chConn, tiered, 5*time.Second)
	sseHandler := api.NewSSEHandler(hub, js)
	wsHandler := api.NewWSHandler(hub, js, nil)
	dlqHandler := api.NewDLQHandler(js, logger)

	deps := api.Dependencies{
		Ingest: ingestHandler,
		Query:  queryHandler,
		SSE:    sseHandler,
		WS:     wsHandler,
		Health: api.NewHealthHandler(chConn),
		Schema: api.NewSchemaHandler(registry),
		DLQ:    dlqHandler,
		AuthMW: api.JWTAuthMiddleware(api.AuthConfig{Enabled: false}),
		JS:     js,
	}

	router := api.NewRouter(deps)
	server := httptest.NewServer(router)

	t.Cleanup(func() {
		server.Close()
		tiered.Close()
		embeddedMQ.Close()
		chConn.Close()
		chContainer.Terminate(ctx)
	})

	return &testEnv{
		chContainer:  chContainer,
		chHost:       chHost,
		chNativePort: nativePort.Port(),
		chHTTPPort:   httpPort.Port(),
		chConn:       chConn,
		embeddedMQ:   embeddedMQ,
		router:       router,
		server:       server,
	}
}

// TestDLQIntegration runs all DLQ-related integration subtests using shared
// infrastructure. Bento's service.RegisterInput/RegisterBatchOutput are global
// and can only be called once per process, so we use a single parent test.
func TestDLQIntegration(t *testing.T) {
	env := setupTestEnv(t)

	t.Run("StatsEmptyOnFreshStart", func(t *testing.T) {
		resp, err := http.Get(env.server.URL + "/v1/dlq/stats")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

		tables, ok := body["tables"].(map[string]any)
		require.True(t, ok)
		assert.Empty(t, tables, "DLQ should have no entries on fresh start")
		assert.Equal(t, float64(0), body["total"])
	})

	t.Run("DLQPopulatedOnBentoFailure", func(t *testing.T) {
		ctx := context.Background()

		// Publish directly to NATS with a table that doesn't exist in ClickHouse.
		// This bypasses the API's schema validation but Bento will try to INSERT
		// into a non-existent table, fail, and route to DLQ.
		evt := map[string]any{
			"table_name":         "nonexistent_table",
			"received_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"data":               map[string]any{"key": "value"},
		}
		payload, err := json.Marshal(evt)
		require.NoError(t, err)

		_, err = env.embeddedMQ.JetStream().Publish(ctx, "ingest.nonexistent_table", payload)
		require.NoError(t, err)
		t.Log("Published event for nonexistent_table directly to NATS")

		// Wait for Bento batch window (5s) + processing overhead.
		assert.Eventually(t, func() bool {
			resp, err := http.Get(env.server.URL + "/v1/dlq/stats")
			if err != nil {
				return false
			}
			defer resp.Body.Close()

			var body map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				return false
			}

			total, _ := body["total"].(float64)
			return total > 0
		}, 30*time.Second, 500*time.Millisecond, "DLQ should receive failed events within timeout")

		// Verify the DLQ subject is correct.
		resp, err := http.Get(env.server.URL + "/v1/dlq/stats")
		require.NoError(t, err)
		defer resp.Body.Close()

		var body map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

		tables := body["tables"].(map[string]any)
		count, ok := tables["dlq.nonexistent_table"]
		assert.True(t, ok, "DLQ should contain entries for nonexistent_table")
		assert.GreaterOrEqual(t, count, float64(1))
		t.Logf("DLQ stats: %v", body)
	})

	t.Run("SuccessfulIngestNoDLQ", func(t *testing.T) {
		// Ingest a valid event via the API.
		payload := `{"user_id":"alice","event_type":"click","value":42.5}`
		resp, err := http.Post(
			env.server.URL+"/v1/ingest/test_events",
			"application/json",
			strings.NewReader(payload),
		)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var ingestResp map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&ingestResp))
		assert.Equal(t, true, ingestResp["ok"])

		// Wait for Bento to flush the batch to ClickHouse.
		assert.Eventually(t, func() bool {
			var count uint64
			err := env.chConn.QueryRow(context.Background(),
				"SELECT count() FROM test_events WHERE user_id = 'alice'").Scan(&count)
			return err == nil && count > 0
		}, 30*time.Second, 500*time.Millisecond, "event should appear in ClickHouse")

		// Verify no DLQ entries for test_events.
		dlqResp, err := http.Get(env.server.URL + "/v1/dlq/stats")
		require.NoError(t, err)
		defer dlqResp.Body.Close()

		var body map[string]any
		require.NoError(t, json.NewDecoder(dlqResp.Body).Decode(&body))

		tables := body["tables"].(map[string]any)
		_, hasDLQ := tables["dlq.test_events"]
		assert.False(t, hasDLQ, "successful inserts should not produce DLQ entries")
		t.Logf("DLQ stats after successful ingest: %v", body)
	})
}
