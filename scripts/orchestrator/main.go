// E2E orchestrator — drives a clean, isolated E2E test session against
// one ClickHouse + one WaveHouse, then runs the vitest suite once.
//
// Lifecycle:
//
//  1. Start ClickHouse via testcontainers-go (random host ports — no
//     conflict with `make dev` or other compose stacks).
//  2. Pick a random free TCP port on 127.0.0.1 and start bin/wavehouse-cov
//     bound to it (WH_SERVER_PORT) with auth enabled. Random port avoids
//     conflicts with `make dev`, dev servers, and previous runs that may
//     have leaked sockets.
//  3. Run the vitest harness against the running stack. CLICKHOUSE_URL
//     and WAVEHOUSE_URL come in via env.
//  4. SIGINT the cover binary (flushes coverage), terminate the
//     testcontainer.
//
// WaveHouse subprocess output is captured to tmp/wavehouse-cov.log
// rather than streamed to the orchestrator's console — it's noisy JSON
// and flooded the test output. On vitest failure the last lines are
// surfaced to stderr with a banner so a CI failure is debuggable
// without grepping. Set V=1 to stream live for interactive debugging.
//
// Invoked by the Makefile's `test-e2e` recipe via:
//
//	go run ./scripts/orchestrator
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("e2e orchestrator: %v", err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repoRoot, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("getwd: %w", err)
	}

	binPath := filepath.Join(repoRoot, "bin", "wavehouse-cov")
	if _, err := os.Stat(binPath); err != nil {
		return fmt.Errorf("%s missing — run `make build-cover` first", binPath)
	}

	coverDir := filepath.Join(repoRoot, "tmp", "coverage", "e2e", "data")
	if err := os.MkdirAll(coverDir, 0o750); err != nil {
		return fmt.Errorf("mkdir coverdir: %w", err)
	}
	dataDir := filepath.Join(repoRoot, "tmp", "data")
	_ = os.RemoveAll(dataDir)

	// Capture WH output to a file rather than the orchestrator's console.
	// V=1 reverts to streaming for interactive debugging.
	verbose := os.Getenv("V") == "1"
	whLogPath := filepath.Join(repoRoot, "tmp", "wavehouse-cov.log")
	if err := os.MkdirAll(filepath.Dir(whLogPath), 0o750); err != nil {
		return fmt.Errorf("mkdir tmp: %w", err)
	}
	// #nosec G304 — whLogPath is filepath.Join(repoRoot, "tmp",
	// "wavehouse-cov.log") with constant components, not user input.
	whLog, err := os.Create(whLogPath)
	if err != nil {
		return fmt.Errorf("open wh log: %w", err)
	}
	defer func() { _ = whLog.Close() }()

	log.Println("→ starting ClickHouse testcontainer (clean state per run)...")
	ch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "clickhouse/clickhouse-server:latest",
			ExposedPorts: []string{"9000/tcp", "8123/tcp"},
			WaitingFor:   wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
			Env: map[string]string{
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
				"CLICKHOUSE_PASSWORD":                  "",
			},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("clickhouse start: %w", err)
	}
	defer func() {
		log.Println("→ terminating ClickHouse testcontainer...")
		if err := ch.Terminate(context.Background()); err != nil {
			log.Printf("  clickhouse terminate: %v", err)
		}
	}()

	chHost, err := ch.Host(ctx)
	if err != nil {
		return fmt.Errorf("clickhouse host: %w", err)
	}
	chNativePort, err := ch.MappedPort(ctx, "9000")
	if err != nil {
		return fmt.Errorf("clickhouse native port: %w", err)
	}
	chHTTPPort, err := ch.MappedPort(ctx, "8123")
	if err != nil {
		return fmt.Errorf("clickhouse http port: %w", err)
	}
	chAddr := fmt.Sprintf("%s:%s", chHost, chNativePort.Port())
	chHTTPURL := fmt.Sprintf("http://%s:%s", chHost, chHTTPPort.Port())
	log.Printf("✓ ClickHouse ready: native=%s http=%s", chAddr, chHTTPURL)

	whPort, err := pickFreePort(ctx)
	if err != nil {
		return fmt.Errorf("pick free port: %w", err)
	}
	whURL := fmt.Sprintf("http://127.0.0.1:%d", whPort)
	log.Printf("→ starting wavehouse-cov on %s (logs: %s)", whURL, whLogPath)

	// #nosec G204 — binPath is filepath.Join(repoRoot, "bin", "wavehouse-cov"),
	// not user-controlled. The test harness must launch the cover binary.
	whCmd := exec.CommandContext(ctx, binPath)
	whCmd.Env = append(filterEnv(os.Environ(),
		"GOCOVERDIR", "WH_SERVER_PORT", "WH_CH_ADDR", "WH_CH_HTTP_PORT",
		"WH_DATA_DIR", "WH_MQ_MAX_BYTES_GB", "WH_AUTH_ENABLED",
		"WH_AUTH_JWT_SECRET", "WH_AUTH_DEV_MODE", "WH_AUTH_ROLE_CLAIM",
		"WH_DEDUPE_ENABLED", "WH_SCHEMA_REFRESH_INTERVAL", "WH_DLQ_ENABLED",
		"WH_SERVER_CORS_ALLOWED_ORIGINS", "WH_OTEL_ENABLED", "WH_OTEL_ADDR",
	),
		"GOCOVERDIR="+coverDir,
		"WH_SERVER_PORT="+strconv.Itoa(whPort),
		"WH_CH_ADDR="+chAddr,
		// Without WH_CH_HTTP_PORT, Bento's HTTP path falls back to the
		// default :8123 and writes silently land on whatever CH is sitting
		// on that port (e.g. a `make dev` instance), while tests verify
		// against the testcontainer's CH which has zero rows.
		"WH_CH_HTTP_PORT="+chHTTPPort.Port(),
		"WH_DATA_DIR="+dataDir,
		"WH_MQ_MAX_BYTES_GB=1",
		"WH_AUTH_ENABLED=true",
		"WH_AUTH_JWT_SECRET=sdk-dev-secret",
		"WH_AUTH_DEV_MODE=false",
		"WH_AUTH_ROLE_CLAIM=role",
		"WH_DEDUPE_ENABLED=false",
		"WH_SCHEMA_REFRESH_INTERVAL=5",
		"WH_DLQ_ENABLED=true",
		"WH_SERVER_CORS_ALLOWED_ORIGINS=*",
		// Exercise the OTel branch in coverage. gRPC exporters are lazy
		// (no collector needs to be reachable for init to succeed), so
		// this is safe in the e2e harness even without a SigNoz instance.
		"WH_OTEL_ENABLED=true",
		"WH_OTEL_ADDR=127.0.0.1:4317",
	)
	if verbose {
		whCmd.Stdout = io.MultiWriter(whLog, os.Stdout)
		whCmd.Stderr = io.MultiWriter(whLog, os.Stderr)
	} else {
		whCmd.Stdout = whLog
		whCmd.Stderr = whLog
	}
	if err := whCmd.Start(); err != nil {
		return fmt.Errorf("wavehouse-cov start: %w", err)
	}
	whDone := make(chan error, 1)
	go func() { whDone <- whCmd.Wait() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	log.Printf("→ waiting for WaveHouse /health at %s ...", whURL)
	if err := waitForHealth(ctx, whURL+"/health", 30*time.Second); err != nil {
		_ = whCmd.Process.Signal(syscall.SIGINT)
		<-whDone
		dumpLogTail(whLogPath, "wavehouse never became healthy")
		return fmt.Errorf("wavehouse not healthy: %w", err)
	}
	log.Println("✓ WaveHouse healthy")

	// /ready exercises the readiness probe (BootState check + ClickHouse
	// ping). It's the kubelet/k8s contract — separate from /health which
	// is liveness-only. CH ping may need a moment after the testcontainer
	// reports listening, hence the retry budget.
	if err := waitForHealth(ctx, whURL+"/ready", 10*time.Second); err != nil {
		_ = whCmd.Process.Signal(syscall.SIGINT)
		<-whDone
		dumpLogTail(whLogPath, "wavehouse never became ready")
		return fmt.Errorf("wavehouse not ready: %w", err)
	}
	log.Println("✓ WaveHouse ready")

	// Self-probe via the `wavehouse health` subcommand — same code path the
	// distroless Dockerfile HEALTHCHECK uses. Running it here confirms the
	// probe binary itself stays in sync with the in-process /health route.
	if err := runSelfHealthProbe(ctx, binPath, whPort, coverDir); err != nil {
		_ = whCmd.Process.Signal(syscall.SIGINT)
		<-whDone
		dumpLogTail(whLogPath, "wavehouse health subcommand probe failed")
		return fmt.Errorf("wavehouse health subcommand: %w", err)
	}
	log.Println("✓ wavehouse health subcommand OK")

	log.Println("→ running vitest harness...")
	vitest := exec.CommandContext(ctx, "pnpm", "run", "test")
	vitest.Dir = filepath.Join(repoRoot, "tests", "e2e", "sdk")
	vitest.Env = append(filterEnv(os.Environ(), "WAVEHOUSE_URL", "CLICKHOUSE_URL"),
		"WAVEHOUSE_URL="+whURL,
		"CLICKHOUSE_URL="+chHTTPURL,
	)
	vitest.Stdout = os.Stdout
	vitest.Stderr = os.Stderr

	vitestDone := make(chan error, 1)
	go func() { vitestDone <- vitest.Run() }()

	var vitestErr error
	select {
	case sig := <-sigCh:
		log.Printf("→ signal %s received — aborting", sig)
		if vitest.Process != nil {
			_ = vitest.Process.Signal(syscall.SIGTERM)
		}
		vitestErr = fmt.Errorf("interrupted by %s", sig)
	case vitestErr = <-vitestDone:
		// fall through to teardown
	}

	// Always SIGINT the cover binary so coverage flushes regardless of
	// how vitest ended (success / failure / interrupted).
	log.Println("→ SIGINT-ing WaveHouse (flushes coverage)...")
	if err := whCmd.Process.Signal(syscall.SIGINT); err != nil && !errors.Is(err, os.ErrProcessDone) {
		log.Printf("  signal wavehouse: %v", err)
	}
	select {
	case err := <-whDone:
		if err != nil && !isExpectedExit(err) {
			log.Printf("  wavehouse exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		log.Println("  wavehouse did not exit within 10s — killing")
		_ = whCmd.Process.Kill()
		<-whDone
	}

	if vitestErr != nil && !verbose {
		dumpLogTail(whLogPath, "vitest failed; tailing wavehouse logs for context")
	}
	return vitestErr
}

// pickFreePort asks the OS for an available port on 127.0.0.1, then
// closes the listener so wavehouse-cov can bind to it. There's a brief
// TOCTOU window where another process could grab the port, but on a
// dedicated CI runner (or a developer's laptop) the race is vanishingly
// rare; if it ever bites we can retry.
//
// Uses (*net.ListenConfig).Listen so the orchestrator's context can
// abort a hung syscall on shutdown — `noctx` (golangci-lint) flags the
// bare net.Listen for this reason.
func pickFreePort(ctx context.Context) (int, error) {
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// dumpLogTail prints the last N lines of `path` to stderr so a CI
// failure is debuggable without re-running with V=1.
func dumpLogTail(path, banner string) {
	const lines = 80
	f, err := os.Open(path) // #nosec G304 — path is the orchestrator's own log file.
	if err != nil {
		fmt.Fprintf(os.Stderr, "  (could not read %s: %v)\n", path, err)
		return
	}
	defer func() { _ = f.Close() }()

	// Slurp lines into a ring buffer so the tail comes out in order.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	ring := make([]string, 0, lines)
	for scanner.Scan() {
		if len(ring) == lines {
			ring = ring[1:]
		}
		ring = append(ring, scanner.Text())
	}

	fmt.Fprintf(os.Stderr, "\n──── %s (last %d lines of %s) ────\n", banner, len(ring), path)
	for _, line := range ring {
		fmt.Fprintln(os.Stderr, line)
	}
	fmt.Fprintln(os.Stderr, "──── end of wavehouse log tail ────")
}

func waitForHealth(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("not reachable within %s", timeout)
}

// runSelfHealthProbe execs `bin/wavehouse-cov health` against the running
// server. Mirrors the distroless Dockerfile HEALTHCHECK invocation. Coverage
// from the short-lived subprocess flushes into the same GOCOVERDIR as the
// long-running daemon.
func runSelfHealthProbe(ctx context.Context, binPath string, port int, coverDir string) error {
	// #nosec G204 — binPath is the cover binary the orchestrator already
	// launched; port/coverDir are locally constructed.
	cmd := exec.CommandContext(ctx, binPath, "health")
	cmd.Env = append(filterEnv(os.Environ(), "GOCOVERDIR", "WH_SERVER_PORT"),
		"GOCOVERDIR="+coverDir,
		"WH_SERVER_PORT="+strconv.Itoa(port),
	)
	return cmd.Run()
}

// filterEnv returns env with any KEY=VALUE entries for the given keys
// removed. Needed because os.Exec inherits the parent's environment, and
// `append(os.Environ(), "K=v")` is silently wrong when K already exists:
// glibc/darwin getenv() returns the FIRST match, so the appended value is
// shadowed by whatever the developer's shell already had set.
func filterEnv(env []string, keys ...string) []string {
	out := make([]string, 0, len(env))
	for _, e := range env {
		keep := true
		for _, k := range keys {
			if strings.HasPrefix(e, k+"=") {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, e)
		}
	}
	return out
}

// SIGINT-shutdown of a Go program returns nil (clean exit) but the
// subprocess.Wait wrapper sometimes surfaces an exit error of "signal: interrupt"
// — treat that as expected.
func isExpectedExit(err error) bool {
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		return ee.ProcessState.Sys().(syscall.WaitStatus).Signal() == syscall.SIGINT
	}
	return false
}
