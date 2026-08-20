package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// runHealthCheck performs a liveness probe by GETting /livez on the local
// HTTP listener. Returns 0 if the server responds with 200, 1 otherwise.
//
// Liveness (/livez), deliberately not readiness (/readyz): a container
// HEALTHCHECK drives the daemon's (healthy)/(unhealthy) state — and, under
// orchestrators that act on it, restart/recreate decisions. We want those
// tied to "is the WaveHouse process itself alive," not "is ClickHouse
// reachable this second." Probing /readyz would flap the container to
// (unhealthy) on every transient ClickHouse blip even though WaveHouse is
// fine and recovers on its own; /livez is sticky once boot completes, so it
// reflects process liveness only. This is also the right target for compose
// `depends_on: service_healthy` waits. (cmd/wavehouse/health_test.go pins the
// choice so a refactor can't silently repoint the probe at /readyz.)
//
// This exists so the distroless `Dockerfile`s can ship a self-probing
// HEALTHCHECK without bundling curl/wget. The standard distroless pattern
// is to embed the probe in the same binary the container runs (Cilium,
// Trivy, Coredns and many others use this approach).
//
// Port resolution mirrors the server's: WH_SERVER_PORT env var (set by
// `ENV WH_SERVER_PORT=8080` in the Dockerfile, overridable by compose env
// or `docker run -e`), defaulting to 8080.
func runHealthCheck(args []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: wavehouse health

Liveness self-probe against the local server's /livez endpoint (the container
HEALTHCHECK). The port comes from WH_SERVER_PORT, defaulting to 8080.

Exit codes: 0 alive, 1 not alive, 2 usage.
`)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2 // fs already printed the flag error and usage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "wavehouse health: unexpected argument %q\n", fs.Arg(0))
		fs.Usage()
		return 2
	}

	port := 8080
	if p := os.Getenv("WH_SERVER_PORT"); p != "" {
		parsed, err := strconv.Atoi(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "health: WH_SERVER_PORT %q is not a valid integer: %v\n", p, err)
			return 1
		}
		port = parsed
	}

	// URL components are all locally controlled: literal `127.0.0.1`,
	// integer-parsed port, literal `/livez`. Gosec's G704 taint
	// analysis flags the env-var flow regardless of validation, so the
	// nosec annotations are explicit acknowledgements. Uses the canonical
	// /livez name rather than a deprecated alias (/healthz, /health) so our
	// own tooling doesn't break when those aliases are removed in v0.2.0.
	url := fmt.Sprintf("http://127.0.0.1:%d/livez", port)

	// Short timeout. The /livez handler is in-memory only — no DB, no
	// network — so a healthy server responds in microseconds. 3s is the
	// HEALTHCHECK --timeout value; we want to fail well before that so
	// the docker daemon's timeout never trips.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G704 -- url built from hardcoded host/path + int-parsed port
	if err != nil {
		fmt.Fprintf(os.Stderr, "health: build request: %v\n", err)
		return 1
	}

	resp, err := http.DefaultClient.Do(req) // #nosec G704 -- see above; req's URL is locally constrained
	if err != nil {
		fmt.Fprintf(os.Stderr, "health: request failed: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health: unexpected status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}
