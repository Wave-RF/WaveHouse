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
//  3. Run the vitest harness against the running stack. CLICKHOUSE_URL,
//     WAVEHOUSE_URL, and WAVEHOUSE_SETTINGS_DIR come in via env.
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
	"encoding/json"
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

	// Clear any server left over from a previous run before touching shared
	// state. Both would use the JetStream/pebble state under tmp/data and both
	// would write tmp/wavehouse-cov.log, so a survivor corrupts this run and
	// interleaves its output into this run's log — which surfaces as a dozen
	// unrelated tests failing to see their rows, in a log that blames the wrong
	// ClickHouse. `make test-e2e` cleans up after itself; a leftover means the
	// previous run was killed (a harness timeout, a stop button, an impatient
	// SIGKILL) rather than interrupted.
	//
	// Kill rather than refuse: a match is by construction this repo's own cover
	// binary from a dead run, the very next statement wipes the data dir out
	// from under it anyway, and refusing would wedge every subsequent run on a
	// shared CI runner until someone got shell access. Loud, because silently
	// killing processes should never be a surprise.
	if stale, err := staleServerPIDs(ctx, binPath); err != nil {
		log.Printf("  (could not check for leftover servers: %v)", err)
	} else if len(stale) > 0 {
		log.Printf("! killing %d leftover wavehouse-cov process(es) from a previous run: %s",
			len(stale), strings.Join(stale, " "))
		log.Printf("  (they share tmp/data and tmp/wavehouse-cov.log with this run)")
		for _, pid := range stale {
			n, convErr := strconv.Atoi(pid)
			if convErr != nil {
				continue
			}
			proc, findErr := os.FindProcess(n)
			if findErr != nil {
				continue
			}
			// ErrProcessDone means it exited between pgrep and here — the
			// common case being a Ctrl-C'd run still flushing coverage. That
			// is the outcome we wanted, not a failure.
			if killErr := proc.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				return fmt.Errorf(
					"leftover wavehouse-cov (pid %s) could not be killed: %w\n"+
						"  it will corrupt this run — kill it manually with: kill -9 %s",
					pid, killErr, pid)
			}
		}
	}

	coverDir := filepath.Join(repoRoot, "tmp", "coverage", "e2e", "data")
	if err := os.MkdirAll(coverDir, 0o750); err != nil {
		return fmt.Errorf("mkdir coverdir: %w", err)
	}
	dataDir := filepath.Join(repoRoot, "tmp", "data")
	_ = os.RemoveAll(dataDir)
	// The settings directory is copied per run so the testcontainer's dynamic
	// ClickHouse ports can be patched into config.json's clickhouse block —
	// that wiring is a settings key, not env, so there is no env override
	// for it. Everything else in the fixture is used as-is.
	settingsDir := filepath.Join(repoRoot, "tmp", "settings")
	_ = os.RemoveAll(settingsDir)

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
			// Pinned to match tests/integration/setup_test.go (26.8 changed
			// numeric DateTime64 parsing; see the comment there).
			Image:        "clickhouse/clickhouse-server:26.6.3.62",
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
	if err := writeRunSettings(filepath.Join(repoRoot, "tests", "e2e", "fixtures", "settings"), settingsDir, chAddr, mustAtoi(chHTTPPort.Port())); err != nil {
		return fmt.Errorf("settings dir: %w", err)
	}

	whPort, err := pickFreePort(ctx)
	if err != nil {
		return fmt.Errorf("pick free port: %w", err)
	}
	whURL := fmt.Sprintf("http://127.0.0.1:%d", whPort)
	log.Printf("→ starting wavehouse-cov on %s (logs: %s)", whURL, whLogPath)

	// #nosec G204 — binPath is filepath.Join(repoRoot, "bin", "wavehouse-cov"),
	// not user-controlled. The test harness must launch the cover binary.
	whCmd := exec.CommandContext(ctx, binPath)
	// Pin cwd so any relative path in the fixture resolves from the repo
	// root regardless of where the orchestrator itself was launched from.
	whCmd.Dir = repoRoot
	// Static boot config (secrets, otel, mq sizing) lives in
	// tests/e2e/fixtures/config.yaml and the tunables, policy, roles, and
	// pipes in tests/e2e/fixtures/settings — edit them there, not here. The
	// vars below are the per-run dynamic overrides (port, scratch paths, the
	// patched settings copy) plus GOCOVERDIR and WH_CONFIG, which can't live
	// in YAML.
	whCmd.Env = append(os.Environ(),
		"GOCOVERDIR="+coverDir,
		"WH_CONFIG="+filepath.Join(repoRoot, "tests", "e2e", "fixtures", "config.yaml"),
		"WH_SERVER_PORT="+strconv.Itoa(whPort),
		"WH_SETTINGS_DIR="+settingsDir,
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
	//
	// E2E_NO_COVERAGE=1 drops it for local debugging only, and is ignored under
	// the gating targets. Left unconditional, an exported-and-forgotten var
	// would let `make ci` write a green push marker with the TS e2e report
	// missing: test-e2e wipes tmp/coverage/ts-e2e first, ts-e2e's own threshold
	// is informational, and ts-total then gates on ts-unit alone — an `n/a` row
	// in the table, but a pass. COV_DEFER is exported by exactly the targets
	// that gate (ci / test-all), so it is the right thing to key off.
	args := []string{"exec", "vitest", "run", "--coverage"}
	if os.Getenv("E2E_NO_COVERAGE") == "1" {
		if os.Getenv("COV_DEFER") != "" {
			log.Println("  (E2E_NO_COVERAGE=1 ignored — this run feeds a coverage gate)")
		} else {
			args = args[:len(args)-1]
			log.Println("  (E2E_NO_COVERAGE=1 — running without coverage; no report will be written)")
		}
	}
	// #nosec G204 — args are a fixed string slice, not user input.
	vitest := exec.CommandContext(ctx, "pnpm", args...)
	vitest.Dir = filepath.Join(repoRoot, "tests", "e2e", "sdk")
	// WAVEHOUSE_SETTINGS_DIR is the per-run settings copy the server reads:
	// the suite edits policies.json / roles.json / pipes.json there and
	// triggers POST /v1/ops/settings/reload (files are the only write path).
	vitest.Env = append(os.Environ(),
		"WAVEHOUSE_URL="+whURL,
		"CLICKHOUSE_URL="+chHTTPURL,
		"WAVEHOUSE_SETTINGS_DIR="+settingsDir,
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
		// A clean graceful exit with no OTel collector running (the e2e
		// default) bounds its telemetry-provider shutdown to a ~3s deadline
		// (see cmd/wavehouse + observability.InitProvider), so SIGINT→exit
		// lands well inside this budget. It was ~15s while those flushes ran
		// serially and unbounded, which is why this was temporarily bumped to
		// 30s. Fast exits are unaffected (whDone fires). Killing here would
		// SIGKILL the cover binary before it flushes GOCOVERDIR, zeroing e2e
		// coverage — the budget must stay above the bounded shutdown time.
		log.Println("  wavehouse did not exit within 10s — killing")
		_ = whCmd.Process.Kill()
		<-whDone
	}

	if vitestErr != nil && !verbose {
		dumpLogHeadTail(whLogPath, "vitest failed; showing some wavehouse logs for context")
	}
	return vitestErr
}

// staleServerPIDs returns the PIDs of any wavehouse-cov left over from an
// earlier run. Called before this run starts its own, so every match is stale.
// A missing/failed pgrep is reported as an error and treated as "unknown" by
// the caller — this is a guard rail, not a gate.
func staleServerPIDs(ctx context.Context, binPath string) ([]string, error) {
	// #nosec G204 — binPath is filepath.Join(repoRoot, "bin", "wavehouse-cov")
	// with constant components, not user input; it is only ever a search
	// pattern here, never executed.
	out, err := exec.CommandContext(ctx, "pgrep", "-f", binPath).Output()
	if err != nil {
		// pgrep exits 1 with no output when nothing matches — the common case.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil, nil
		}
		return nil, err
	}
	return strings.Fields(string(out)), nil
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

// writeRunSettings copies the fixture settings directory to dst and rewrites
// config.json's clickhouse.addr / clickhouse.http_port to the
// testcontainer's mapped ports. Without the http_port patch the ingest
// worker's HTTP path would land writes on whatever ClickHouse sits on
// :8123 (e.g. a `make dev` instance) while the tests verify against the
// testcontainer, which has zero rows.
func writeRunSettings(src, dst, chAddr string, chHTTPPort int) error {
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(src, e.Name())) //nolint:gosec // G304/G703: entries of the fixture directory under the repo root, not user input
		if err != nil {
			return err
		}
		if e.Name() == "config.json" {
			var doc map[string]json.RawMessage
			if err := json.Unmarshal(data, &doc); err != nil {
				return fmt.Errorf("config.json: %w", err)
			}
			var ch map[string]any
			if err := json.Unmarshal(doc["clickhouse"], &ch); err != nil {
				return fmt.Errorf("config.json clickhouse: %w", err)
			}
			ch["addr"], ch["http_port"] = chAddr, chHTTPPort
			if doc["clickhouse"], err = json.Marshal(ch); err != nil {
				return err
			}
			if data, err = json.MarshalIndent(doc, "", "  "); err != nil {
				return err
			}
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil { //nolint:gosec // G703: dst is tmp/settings under the repo root; names come from the fixture directory
			return err
		}
	}
	return nil
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("port %q is not numeric: %v", s, err))
	}
	return n
}
