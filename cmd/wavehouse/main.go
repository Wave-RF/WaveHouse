package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
)

// Pre-populated build info variables, set via ldflags by the Makefile and by
// GoReleaser at release time. The defaults here are the unstamped-build
// fallbacks; buildInfoFallback below fills them in for `go install` builds.
//
// All three MUST keep constant string initializers: `go build -ldflags -X`
// silently does nothing to a variable whose initializer isn't a constant.
// BuildTime was `time.Now().Format(time.RFC3339)`, so every `-X
// main.BuildTime=...` the Makefile and .goreleaser.yaml have ever passed was
// dropped on the floor and /version reported the process's start time as its
// build time.
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func init() { buildInfoFallback() }

// buildInfoFallback recovers the build stamps from the metadata the Go
// toolchain embeds on its own, for binaries built without our ldflags.
//
// `go install github.com/Wave-RF/WaveHouse/cmd/wavehouse@v0.1.0` — the install
// path the README documents — passes no ldflags, so a perfectly good tagged
// build would otherwise report itself as "dev"/"unknown"/"unknown". The module
// version and VCS stamps the toolchain records cover exactly that case.
//
// ldflags always win, and each field is gated on ITS OWN default rather than on
// Version alone: a build that stamps only -X main.GitCommit would otherwise have
// that value overwritten here, since Version would still read "dev". The
// Makefile and GoReleaser both stamp all three today, but the contract should
// hold for a partial link too.
func buildInfoFallback() {
	if Version != "dev" && GitCommit != "unknown" && BuildTime != "unknown" {
		return
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	// "(devel)" is what a build with no VCS metadata reports (`-buildvcs=false`,
	// or building outside a repo) — no better than the "dev" we already have.
	// A normal `go build` in a checkout gets a real pseudo-version, and
	// `go install pkg@vX.Y.Z` gets the module version.
	if v := info.Main.Version; Version == "dev" && v != "" && v != "(devel)" {
		Version = strings.TrimPrefix(v, "v")
	}

	// Present only when the binary was built from a VCS checkout; `go install
	// module@version` builds from the module cache and carries neither.
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if GitCommit == "unknown" && s.Value != "" {
				GitCommit = s.Value
			}
		case "vcs.time":
			if BuildTime == "unknown" && s.Value != "" {
				BuildTime = s.Value
			}
		}
	}
}

func main() {
	// Subcommand dispatch. `health` self-probes /livez for the distroless
	// Dockerfile HEALTHCHECK; `validate` checks a settings directory without
	// starting the server; `bootstrap` writes a starter one. An unknown command is a usage error — it must
	// never fall through and silently start the server (`wavehouse validat`
	// booting a listener is not a typo anyone wants). The switch only routes;
	// each subcommand owns a stdlib flag.FlagSet, so `wavehouse <command> -h`
	// prints command-specific help and a stray flag or argument is a usage
	// error. If the command surface outgrows this (nested subcommands, shared
	// persistent flags), port the switch to cobra or kong.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "health":
			os.Exit(runHealthCheck(os.Args[2:]))
		case "validate":
			os.Exit(runValidate(os.Args[2:]))
		case "bootstrap":
			os.Exit(runBootstrap(os.Args[2:]))
		case "version", "--version", "-v":
			fmt.Printf("wavehouse %s (commit %s, built %s)\n", Version, GitCommit, BuildTime)
			os.Exit(0)
		case "help", "--help", "-h":
			printUsage(os.Stdout)
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "wavehouse: unknown command %q\n\n", os.Args[1])
			printUsage(os.Stderr)
			os.Exit(2)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go stopOnSignals(sigs, cancel, os.Exit)
	os.Exit(run(ctx))
}

// stopOnSignals cancels the run context on the first SIGINT/SIGTERM, which
// begins the graceful stop, and exits on the second: a repeat while the
// drain runs means "stop waiting" — the conventional escape hatch — and
// abandons it with a non-zero exit. The registration is held for the whole
// process so the second signal reaches here rather than a channel nobody
// reads (or, unregistered, the default disposition).
func stopOnSignals(sigs <-chan os.Signal, cancel context.CancelFunc, exit func(int)) {
	s := <-sigs
	slog.Info("shutdown signal received; a second one exits immediately", "signal", s.String())
	cancel()
	s = <-sigs
	slog.Error("second shutdown signal received; exiting without finishing the stop", "signal", s.String())
	exit(1)
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintf(w, `usage:
  wavehouse                 start the server
  wavehouse validate [dir]  validate a settings directory (dir falls back to %[1]s)
  wavehouse bootstrap [dir] write a starter settings directory, every key at its default (dir falls back to %[1]s)
  wavehouse health          liveness self-probe against the local server (container HEALTHCHECK)
  wavehouse version         print version, commit, and build time
  wavehouse help            show this help

Run 'wavehouse <command> -h' for command-specific help.
`, config.EnvSettingsDir)
}

// run boots the server and blocks until ctx is cancelled (SIGINT/SIGTERM)
// or a component fails, returning the process exit code. A separate function
// (rather than os.Exit directly in main) so the deferred cleanup — especially
// the OTel flush — still runs before the process exits. A stop is two
// phases, each bounded by server.shutdown_timeout: Run drains the in-flight
// work, then Close releases the stores and flushes telemetry.
func run(ctx context.Context) int {
	logLevel := &slog.LevelVar{}
	logLevel.Set(logLevelFromEnv())
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	logger.Info("starting WaveHouse", "version", Version, "build_time", BuildTime, "git_commit", GitCommit)

	cfgPath := "config.yaml"
	if p := os.Getenv(config.EnvConfig); p != "" {
		cfgPath = p
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("load config", "error", err)
		return 1
	}

	// data_dir must be writable before anything dials out, so the refusal
	// (and, for the typical cause — a bind mount owned by root rather than
	// UID 65532 — the remediation) lands at the top of the log rather than
	// after ClickHouse discovery. NATS and Pebble still fail loud on their
	// own if the directory changes underneath us.
	if err := config.CheckDataDir(cfg.DataDir); err != nil {
		logger.Error("check data_dir", "error", err)
		return 1
	}

	a, err := app.New(ctx, app.Options{
		Config:   cfg,
		Build:    app.BuildInfo{Version: Version, GitCommit: GitCommit, BuildTime: BuildTime},
		LogLevel: logLevel,
	})
	if err != nil {
		logger.Error("boot", "error", err)
		return 1
	}
	// slog.Default from here on: app.New swaps in the OTLP-aware logger when
	// OTLP logs are enabled, and that is the one every later line should hit.
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
		defer cancel()
		if err := a.Close(closeCtx); err != nil {
			slog.Warn("cleanup", "error", err)
		}
	}()

	if err := a.Run(ctx); err != nil {
		slog.Error("run", "error", err)
		return 1
	}
	return 0
}

// logLevelFromEnv reads WH_LOG_LEVEL; anything unrecognized is Info.
func logLevelFromEnv() slog.Level {
	switch strings.ToUpper(strings.TrimSpace(os.Getenv(config.EnvLogLevel))) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
