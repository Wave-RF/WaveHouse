package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// runHealthCheck performs a liveness probe by GETting /health on the local
// HTTP listener. Returns 0 if the server responds with 200, 1 otherwise.
//
// This exists so the distroless `Dockerfile`s can ship a self-probing
// HEALTHCHECK without bundling curl/wget. The standard distroless pattern
// is to embed the probe in the same binary the container runs (Cilium,
// Trivy, Coredns and many others use this approach).
//
// Port resolution mirrors the server's: WH_SERVER_PORT env var (set by
// `ENV WH_SERVER_PORT=8080` in the Dockerfile, overridable by compose env
// or `docker run -e`), defaulting to 8080.
func runHealthCheck() int {
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
	// integer-parsed port, literal `/health`. Gosec's G704 taint
	// analysis flags the env-var flow regardless of validation, so the
	// nosec annotations are explicit acknowledgements.
	url := fmt.Sprintf("http://127.0.0.1:%d/health", port)

	// Short timeout. The /health handler is in-memory only — no DB, no
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
