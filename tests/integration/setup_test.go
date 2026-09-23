//go:build integration

// Package tests contains integration tests for WaveHouse. The package brings
// up a single ClickHouse testcontainer and, against it, the production wiring
// (app.New: embedded NATS, ingest worker, sweeper, hub, the API server) in
// TestMain, then exposes that environment to every test in the package via
// env(). Each test creates its own ClickHouse table for data
// isolation; the shared infra avoids the per-test container churn that drove
// flakes and slow runs in the previous monolithic file.
package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

const (
	testCHPassword = "test"
	testCHDatabase = "default"
	testCHUser     = "default"
)

// testEnv holds the shared infrastructure available to every test.
type testEnv struct {
	ch         *chInstance
	chConn     driver.Conn
	chHTTPURL  string
	embeddedMQ mq.Broker
	baseURL    string // the wired API server, e.g. http://127.0.0.1:41234
	registry   *discovery.SchemaRegistry
}

var sharedEnv *testEnv

// env returns the package-shared environment. Tests must call this rather
// than build their own.
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

// setup brings up the shared testcontainer and runs the wired app against
// it. Returns a non-zero code on any failure plus a cleanup func that is
// always safe to call (it tracks which resources actually started).
func setup() (int, func()) {
	ctx := context.Background()

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

	settingsDir, err := writeTestSettings(ch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: settings: %v\n", err)
		return 1, cleanup
	}
	cleanups.push(func() { _ = os.RemoveAll(settingsDir) })

	// The wired app on a harness listener: the same construction the binary
	// uses (embedded NATS in-process, the ingest worker, sweeper, hub bridge,
	// every handler), so the suite exercises the real wiring rather than a
	// hand-built subset that drifts from it.
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: listen: %v\n", err)
		return 1, cleanup
	}
	// Scratch directories are removed after the app closes (LIFO).
	dataDir := mustTempDir()
	cleanups.push(func() { _ = os.RemoveAll(dataDir) })
	cfg := &config.Config{
		DataDir:    dataDir,
		Server:     config.Server{ShutdownTimeout: 10},
		ClickHouse: config.ClickHouse{Password: testCHPassword},
		Cache:      config.Cache{L1MaxCost: 1 << 30}, // 1 GB
		Settings:   config.Settings{Dir: settingsDir},
	}
	a, err := app.New(ctx, app.Options{Config: cfg, Listener: ln})
	if err != nil {
		_ = ln.Close()
		fmt.Fprintf(os.Stderr, "integration setup: app: %v\n", err)
		return 1, cleanup
	}
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(runCtx) }()
	cleanups.push(func() {
		stop()
		if err := <-runDone; err != nil {
			fmt.Fprintf(os.Stderr, "integration teardown: run: %v\n", err)
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Close(closeCtx)
	})

	baseURL := "http://" + ln.Addr().String()
	// Boot tolerates an unreachable ClickHouse (degraded, retrying), but the
	// suite must not: wait for the sticky /livez, which turns 200 only once
	// schema discovery has succeeded.
	if err := waitForLive(ctx, baseURL, 30*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "integration setup: %v\n", err)
		return 1, cleanup
	}

	sharedEnv = &testEnv{
		ch:         ch,
		chConn:     ch.conn,
		chHTTPURL:  ch.httpURL(),
		embeddedMQ: a.MQ(),
		baseURL:    baseURL,
		registry:   a.Registry(),
	}
	return 0, cleanup
}

// writeTestSettings materializes the embedded seed with the ClickHouse block
// pointed at the testcontainer and a dev-style policy: default_role is the
// admin role, so the suite's plain unauthenticated requests exercise
// functionality as a privileged caller and can hit admin-gated endpoints
// without minting JWTs. Auth enforcement is covered by the internal/auth
// unit tests and the e2e SDK suite. The stream budget is shrunk to 1 GiB
// like the e2e fixture so the scratch directory stays small.
func writeTestSettings(ch *chInstance) (string, error) {
	files, err := tenantSettings(ch, testCHDatabase)
	if err != nil {
		return "", err
	}
	dir := mustTempDir()
	if err := writeSettingsFiles(dir, files); err != nil {
		return "", err
	}
	return dir, nil
}

// tenantSettings is one tenant's four files: the seed with the ClickHouse
// block pointed at the testcontainer's database, and the dev-style policy.
func tenantSettings(ch *chInstance, database string) (map[string][]byte, error) {
	files, err := settings.Seed()
	if err != nil {
		return nil, err
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(files[settings.FileConfig], &doc); err != nil {
		return nil, fmt.Errorf("seed config.json: %w", err)
	}
	patch := map[string]any{
		"clickhouse": map[string]any{
			"addr": ch.nativeAddr(), "http_port": mustAtoi(ch.httpPort), "http_scheme": "http",
			"database": database, "username": testCHUser, "query_timeout": 30,
			"tls":     map[string]any{"enabled": false, "ca_file": "", "cert_file": "", "key_file": "", "insecure_skip_verify": false, "server_name": ""},
			"headers": map[string]any{}, "max_open_conns": 10, "max_idle_conns": 5,
		},
		"mq": map[string]any{"max_bytes_gb": 1},
	}
	for key, val := range patch {
		if doc[key], err = json.Marshal(val); err != nil {
			return nil, err
		}
	}
	if files[settings.FileConfig], err = json.MarshalIndent(doc, "", "  "); err != nil {
		return nil, err
	}
	files[settings.FileRoles] = []byte(`{"roles": ["admin"]}`)
	files[settings.FilePolicies] = []byte(`{"default_role": "admin"}`)
	return files, nil
}

// writeSettingsFiles writes one tenant's files into dir.
func writeSettingsFiles(dir string, files map[string][]byte) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// waitForLive polls /livez until it returns 200 or the timeout elapses,
// reporting the last response so a degraded boot's diagnostic surfaces.
func waitForLive(ctx context.Context, baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := "no response"
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/livez", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		} else {
			last = err.Error()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("server not live after %s: %s", timeout, last)
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("integration setup: port %q is not numeric: %v", s, err))
	}
	return n
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
// mapped HTTP port.
//
// ClickHouse opens 9000/tcp early in startup — before it can accept native
// queries. Waiting only on the listening port produced flakes where the
// next chConn call hit "connection reset by peer" mid-handshake. We wait on
// both 9000/tcp AND /ping returning 200, then explicitly Ping the native
// connection before returning. Belt-and-suspenders against the readiness
// race; the dominant flake mode tracked in #70.
func startClickHouse(ctx context.Context) (*chInstance, error) {
	chReq := testcontainers.ContainerRequest{
		// Pinned: 26.8 reads bare numbers in DateTime64 columns as epoch seconds,
		// not ticks at column precision — CanonicalizeTimestamps still models the
		// pre-26.8 rule (TestTimestampCanonicalization_DifferentialAgainstClickHouse
		// catches the divergence). Bump the pin together with the canonicalizer (#536).
		Image:        "clickhouse/clickhouse-server:26.6.3.62",
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

func mustTempDir() string {
	dir, err := os.MkdirTemp("", "wavehouse-it-")
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
