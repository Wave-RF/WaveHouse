//go:build integration

// Package tests contains integration tests for WaveHouse. The package brings
// up a single ClickHouse testcontainer + in-process embedded NATS + wired
// API server in TestMain, then exposes that environment to every test in the
// package via env(). Each test creates its own ClickHouse table for data
// isolation; the shared infra avoids the per-test container churn that drove
// flakes and slow runs in the previous monolithic file.
//
// The single-process constraint is non-negotiable: Bento's
// service.RegisterInput / service.RegisterBatchOutput are package-globals
// behind a sync.Once, so only one StartIngestWorker can be wired per
// process. TestMain wires it once.
package tests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

const (
	testCHPassword = "test"
	testCHDatabase = "default"
	testCHUser     = "default"
)

// testEnv holds the shared infrastructure available to every test.
type testEnv struct {
	chConn     driver.Conn
	chHTTPURL  string
	embeddedMQ *mq.EmbeddedNATS
	server     *httptest.Server
	registry   *discovery.SchemaRegistry
}

var sharedEnv *testEnv

// env returns the package-shared environment. Tests must call this rather
// than build their own — Bento's global registration only allows one
// IngestWorker per process.
func env(t *testing.T) *testEnv {
	t.Helper()
	if sharedEnv == nil {
		t.Fatal("sharedEnv not initialized — TestMain must run first")
	}
	return sharedEnv
}

// tableCounter generates unique table-name suffixes per test so concurrent
// (or sequentially-run) tests don't collide on table state.
var tableCounter atomic.Uint64

// createTable creates a uniquely-named ClickHouse table for the calling test
// and registers cleanup to drop it. The schema registry is refreshed after
// creation so the API discovers the new table. Returns the table name.
//
// Pass the column DDL fragment without the wrapping `()` — for example:
//
//	createTable(t, "user_id String, value Float64", "ORDER BY user_id")
func createTable(t *testing.T, columns, tableOpts string) string {
	t.Helper()

	// Sanitize the test name into a valid CH identifier.
	safe := strings.NewReplacer("/", "_", " ", "_", "-", "_").Replace(t.Name())
	name := fmt.Sprintf("it_%s_%d", strings.ToLower(safe), tableCounter.Add(1))

	ctx := context.Background()
	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() %s",
		name, columns, tableOpts,
	)
	if err := sharedEnv.chConn.Exec(ctx, stmt); err != nil {
		t.Fatalf("create test table %s: %v", name, err)
	}

	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = sharedEnv.chConn.Exec(dropCtx, "DROP TABLE IF EXISTS "+name)
	})

	if err := sharedEnv.registry.Refresh(ctx); err != nil {
		t.Fatalf("refresh schema registry: %v", err)
	}
	return name
}

func TestMain(m *testing.M) {
	code, cleanup := setup()
	if code != 0 {
		cleanup()
		os.Exit(code)
	}

	exit := m.Run()
	cleanup()
	os.Exit(exit)
}

// setup brings up the shared testcontainer + embedded NATS + wired server.
// Returns a non-zero code on any failure plus a cleanup func that is always
// safe to call (it tracks which resources actually started).
func setup() (int, func()) {
	ctx := context.Background()
	logger := slog.Default()

	cleanups := newCleanupStack()
	cleanup := func() { cleanups.run() }

	ch, err := startClickHouse(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: clickhouse: %v\n", err)
		return 1, cleanup
	}
	cleanups.push(func() {
		if ch.conn != nil {
			_ = ch.conn.Close()
		}
		_ = ch.container.Terminate(context.Background())
	})

	// Embedded NATS lives in-process — no testcontainer needed. Production
	// uses the same in-process server, so the integration tests exercise
	// the real wiring rather than a NATS Docker image we never ship with.
	embeddedMQ, err := mq.NewEmbedded(mustTempDir(), 10*1024*1024, testutil.NopLogger())
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: embedded nats: %v\n", err)
		return 1, cleanup
	}
	cleanups.push(func() { _ = embeddedMQ.Close() })

	js := embeddedMQ.JetStream()
	if err := api.EnsureDLQStream(ctx, js, 1024*1024); err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: ensure dlq: %v\n", err)
		return 1, cleanup
	}

	registry := discovery.NewSchemaRegistry(ch.conn, testCHDatabase, time.Minute, logger)
	if err := registry.Refresh(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: schema refresh: %v\n", err)
		return 1, cleanup
	}

	localCache, err := cache.NewLocal(1 << 30) // 1 GB
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: cache initialization: %v\n", err)
		return 1, cleanup
	}
	cleanups.push(func() { _ = localCache.Close() })

	if _, err := ingest.StartIngestWorker(
		ctx,
		embeddedMQ.NatsConn(),
		localCache,
		ch.nativeAddr(),
		ch.httpPort,
		"http",
		testCHUser,
		testCHPassword,
		testCHDatabase,
	); err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: ingest worker: %v\n", err)
		return 1, cleanup
	}

	server, err := buildServer(ch, embeddedMQ, registry, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: build server: %v\n", err)
		return 1, cleanup
	}
	cleanups.push(func() { server.Close() })

	sharedEnv = &testEnv{
		chConn:     ch.conn,
		chHTTPURL:  ch.httpURL(),
		embeddedMQ: embeddedMQ,
		server:     server,
		registry:   registry,
	}
	return 0, cleanup
}

// chInstance bundles a ClickHouse testcontainer with the connection + ports
// the rest of setup needs. Keeping this as a struct keeps the setup() flow
// readable instead of threading five return values around.
type chInstance struct {
	container  testcontainers.Container
	conn       driver.Conn
	host       string
	nativePort string
	httpPort   string
}

func (c *chInstance) nativeAddr() string { return fmt.Sprintf("%s:%s", c.host, c.nativePort) }
func (c *chInstance) httpURL() string    { return fmt.Sprintf("http://%s:%s", c.host, c.httpPort) }

// startClickHouse starts a ClickHouse testcontainer and returns it plus a
// connected native-protocol driver, the host:port the driver dials, and the
// mapped HTTP port (Bento talks to ClickHouse over HTTP for INSERTs).
//
// ClickHouse opens 9000/tcp early in startup — before it can accept native
// queries. Waiting only on the listening port produced flakes where the
// next chConn call hit "connection reset by peer" mid-handshake. We wait on
// both 9000/tcp AND /ping returning 200, then explicitly Ping the native
// connection before returning. Belt-and-suspenders against the readiness
// race; the dominant flake mode tracked in #70.
func startClickHouse(ctx context.Context) (*chInstance, error) {
	chReq := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env:          map[string]string{"CLICKHOUSE_PASSWORD": testCHPassword},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9000/tcp"),
			wait.ForHTTP("/ping").WithPort("8123/tcp").WithStatusCodeMatcher(func(status int) bool {
				return status == http.StatusOK
			}),
		).WithDeadline(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}
	ch := &chInstance{container: container}

	if ch.host, err = container.Host(ctx); err != nil {
		return ch, fmt.Errorf("host: %w", err)
	}
	nativePort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return ch, fmt.Errorf("native port: %w", err)
	}
	ch.nativePort = nativePort.Port()
	httpPort, err := container.MappedPort(ctx, "8123")
	if err != nil {
		return ch, fmt.Errorf("http port: %w", err)
	}
	ch.httpPort = httpPort.Port()

	ch.conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{ch.nativeAddr()},
		Auth: clickhouse.Auth{Database: testCHDatabase, Username: testCHUser, Password: testCHPassword},
	})
	if err != nil {
		return ch, fmt.Errorf("open driver: %w", err)
	}

	// `clickhouse.Open` is lazy. Force the dial here with retries so the
	// first real Exec can't be the one that meets a half-ready server.
	// Surface the last meaningful (non-context) error on timeout rather
	// than the generic context.DeadlineExceeded from the final Ping.
	if err := waitForNativeReady(ctx, ch.conn, 30*time.Second); err != nil {
		return ch, fmt.Errorf("native ping: %w", err)
	}
	return ch, nil
}

func waitForNativeReady(ctx context.Context, conn driver.Conn, timeout time.Duration) error {
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastReal error
	for {
		err := conn.Ping(pingCtx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			lastReal = err
		}
		if pingCtx.Err() != nil {
			if lastReal != nil {
				return lastReal
			}
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// buildServer wires the same handler set as cmd/wavehouse/main.go but
// against the test ClickHouse + embedded NATS, with auth disabled so tests
// can hit endpoints without minting JWTs. Auth-enforcement coverage lives
// in the unit tests for middleware.go and the e2e SDK suite.
func buildServer(ch *chInstance, embeddedMQ *mq.EmbeddedNATS, registry *discovery.SchemaRegistry, logger *slog.Logger) (*httptest.Server, error) {
	js := embeddedMQ.JetStream()

	hub := api.NewHub()

	deps := api.Dependencies{
		Ingest: api.NewIngestHandler(registry, embeddedMQ),
		// /v1/admin/query proxies straight to ClickHouse's HTTP interface,
		// so the handler needs the HTTP URL + creds rather than the
		// native-protocol driver.Conn other handlers use.
		Query:  api.NewQueryHandler(ch.httpURL(), testCHUser, testCHPassword, testCHDatabase, time.Second*time.Duration(30)),
		SSE:    api.NewSSEHandler(hub, js),
		WS:     api.NewWSHandler(hub, js, nil),
		Health: api.NewHealthHandler(ch.conn),
		Schema: api.NewSchemaHandler(registry),
		DLQ:    api.NewDLQHandler(js, logger),
		AuthMW: api.JWTAuthMiddleware(api.AuthConfig{Enabled: false}),
		JS:     js,
	}

	server := httptest.NewServer(api.NewRouter(deps))
	return server, nil
}

func mustTempDir() string {
	dir, err := os.MkdirTemp("", "wavehouse-it-nats-")
	if err != nil {
		panic(fmt.Sprintf("integration setup: temp dir: %v", err))
	}
	return dir
}

// cleanupStack is a LIFO list of cleanup funcs. Pushing during setup and
// running on exit gives a deterministic teardown order regardless of which
// step failed.
type cleanupStack struct{ fns []func() }

func newCleanupStack() *cleanupStack { return &cleanupStack{} }

func (c *cleanupStack) push(fn func()) { c.fns = append(c.fns, fn) }

func (c *cleanupStack) run() {
	for i := len(c.fns) - 1; i >= 0; i-- {
		c.fns[i]()
	}
	c.fns = nil
}
