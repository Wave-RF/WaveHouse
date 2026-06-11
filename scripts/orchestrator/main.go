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
	// Pin cwd so the fixture's relative paths (policy.file_path, etc.)
	// resolve from the repo root regardless of where the orchestrator
	// itself was launched from.
	whCmd.Dir = repoRoot
	// Static config knobs (auth, dedupe, otel, mq, schema, dlq, cors, ...) live
	// in tests/e2e/fixtures/config.yaml — edit them there, not here. The vars
	// below are the per-run dynamic overrides (ports, addresses, scratch
	// paths) plus GOCOVERDIR and WH_CONFIG, which can't live in YAML.
	whCmd.Env = append(os.Environ(),
		"GOCOVERDIR="+coverDir,
		"WH_CONFIG="+filepath.Join(repoRoot, "tests", "e2e", "fixtures", "config.yaml"),
		"WH_SERVER_PORT="+strconv.Itoa(whPort),
		"WH_CH_ADDR="+chAddr,
		// Without WH_CH_HTTP_PORT, ingest worker's HTTP path falls back to the
		// default :8123 and writes silently land on whatever CH is sitting
		// on that port (e.g. a `make dev` instance), while tests verify
		// against the testcontainer's CH which has zero rows.
		"WH_CH_HTTP_PORT="+chHTTPPort.Port(),
		"WH_DATA_DIR="+dataDir,
	)

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		whCmd.Env = append(whCmd.Env, "OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317")
	}
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

	log.Printf("→ waiting for WaveHouse /livez at %s ...", whURL)
	if err := waitForHealth(ctx, whURL+"/livez", 30*time.Second); err != nil {
		_ = whCmd.Process.Signal(syscall.SIGINT)
		<-whDone
		dumpLogHeadTail(whLogPath, "wavehouse never became healthy")
		return fmt.Errorf("wavehouse not healthy: %w", err)
	}
	log.Println("✓ WaveHouse healthy")

	log.Println("→ running vitest harness...")
	// Always run with --coverage — same pattern as `make test` / `make
	// test-integration` (which always use Go's -cover). TS_E2E_COVERAGE_DIR
	// is forwarded from the calling environment so reports land at
	// tmp/coverage/ts-e2e/ for `cov ts-merge` to pick up.
	//
	// Call the vitest binary directly via `pnpm exec` rather than `pnpm run
	// test -- --coverage`. The npm-style `--` separator does NOT survive pnpm
	// 11: it forwards the literal `--` to the script, so vitest receives
	// `vitest run -- --coverage` and parses --coverage as a trailing operand
	// (coverage stays OFF, tests pass, no report is written — silently). Going
	// straight to `pnpm exec vitest run --coverage` skips the script-arg
	// forwarding layer entirely, matching how scripts/cov invokes `pnpm exec
	// nyc`.
	// #nosec G204 — args are a fixed string slice, not user input.
	vitest := exec.CommandContext(ctx, "pnpm", "exec", "vitest", "run", "--coverage")
	vitest.Dir = filepath.Join(repoRoot, "tests", "e2e", "sdk")
	vitest.Env = append(os.Environ(),
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
	case <-time.After(30 * time.Second):
		// 30s, not 10: a clean graceful exit with no OTel collector running
		// (the e2e default) serializes the traces/metrics/logs exporter
		// shutdowns — each individually bounded but ~5s apiece during gRPC
		// dial backoff — and lands around 15s (#288). The old 10s budget was
		// calibrated when the embedded NATS signal handler os.Exit(0)'d the
		// process early (#287). Fast exits are unaffected (whDone fires).
		log.Println("  wavehouse did not exit within 30s — killing")
		_ = whCmd.Process.Kill()
		<-whDone
	}

	if vitestErr != nil && !verbose {
		dumpLogHeadTail(whLogPath, "vitest failed; showing some wavehouse logs for context")
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

// dumpLogHeadTail prints the last N lines of `path` to stderr so a CI
// failure is debuggable without re-running with V=1.
func dumpLogHeadTail(path, banner string) {
	const lines = 40
	f, err := os.Open(path) // #nosec G304 — path is the orchestrator's own log file.
	if err != nil {
		fmt.Fprintf(os.Stderr, "  (could not read %s: %v)\n", path, err)
		return
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	head := make([]string, 0, lines)
	ring := make([]string, 0, lines)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		text := scanner.Text()

		// Capture the first N lines
		if len(head) < lines {
			head = append(head, text)
		}

		// Slurp lines into a ring buffer for the tail
		if len(ring) == lines {
			ring = ring[1:]
		}
		ring = append(ring, text)
	}

	// Meta Header: Always prints total lines and the retrieval path first
	fmt.Fprintf(os.Stderr, "\n==== %s ====\n", banner)
	fmt.Fprintf(os.Stderr, "Total Log Lines: %d\n", lineCount)
	fmt.Fprintf(os.Stderr, "Full Log Path:   %s\n", path)
	fmt.Fprintln(os.Stderr, "=========================================")

	if lineCount > lines*2 {
		// Case 1: Large file (Lines are actually skipped)
		fmt.Fprintf(os.Stderr, "\n>>> Printing the first %d lines:\n", lines)
		for _, line := range head {
			fmt.Fprintln(os.Stderr, line)
		}

		// Heavy middle break highlighting skipped content and location
		skipped := lineCount - (lines * 2)
		fmt.Fprintf(os.Stderr, "\n\n[... SKIPPED %d LINES ...]\n[... View full artifact at: %s ...]\n\n\n", skipped, path)

		fmt.Fprintf(os.Stderr, ">>> Printing the last %d lines:\n", lines)
		for _, line := range ring {
			fmt.Fprintln(os.Stderr, line)
		}

	} else {
		// Case 2: The entire file fits completely. Seamlessly print everything.
		fmt.Fprintf(os.Stderr, "\n>>> Printing the entire file (%d lines total):\n", lineCount)

		// Print the first chunk
		for _, line := range head {
			fmt.Fprintln(os.Stderr, line)
		}

		// If the file was larger than a single `lines` buffer but smaller than the max budget,
		// append the remaining unique lines from the ring buffer without any visual break.
		if lineCount > lines {
			toPrint := lineCount - lines
			for _, line := range ring[len(ring)-toPrint:] {
				fmt.Fprintln(os.Stderr, line)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "\n==== End of %s log ====\n", banner)
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

// SIGINT-shutdown of a Go program returns nil (clean exit) but the
// subprocess.Wait wrapper sometimes surfaces an exit error of "signal: interrupt"
// — treat that as expected.
func isExpectedExit(err error) bool {
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		return ee.ProcessState.Sys().(syscall.WaitStatus).Signal() == syscall.SIGINT
	}
	return false
}
