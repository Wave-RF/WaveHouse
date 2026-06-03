package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runHealthCheck is the Dockerfile HEALTHCHECK probe — the bridge between
// the in-process /healthz endpoint and the docker daemon's container-health
// state. These tests pin the contract that holds the boot-non-fatal promise
// together: when wavehouse is boot-degraded, /healthz returns 503 and the
// probe must return 1 so Docker marks the container unhealthy. Without this
// coverage, a regression that swallowed non-200 responses would silently
// re-create the "stuck container" failure mode the PR is solving.
//
// Tests don't run in parallel because they all mutate WH_SERVER_PORT.
// t.Setenv handles save/restore.

// portFromListener pulls the integer port off a net.Listener. We use the
// listener directly (rather than parsing httptest.Server.URL) so the same
// helper works for both started-server cases and the connection-refused
// case where we listen-then-close to claim a known-unused port.
func portFromListener(t *testing.T, ln net.Listener) string {
	t.Helper()
	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok, "listener should be a TCP listener")
	return strconv.Itoa(addr.Port)
}

func TestRunHealthCheck_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The probe targets /healthz specifically — pin that contract here
		// so a refactor that points the probe at /readyz (which requires
		// ClickHouse) doesn't slip through silently.
		assert.Equal(t, "/healthz", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WH_SERVER_PORT", portFromListener(t, srv.Listener))
	assert.Equal(t, 0, runHealthCheck(), "200 OK should map to exit 0")
}

func TestRunHealthCheck_BootDegraded503(t *testing.T) {
	// This is the user-facing contract under boot-degraded state: the
	// in-process /healthz returns 503 with a diagnostic body, the probe
	// sees the non-200, returns 1, and Docker's HEALTHCHECK marks the
	// container unhealthy. The K8s startupProbe path in deployment.md
	// relies on exactly this signal.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"degraded","error":"schema discovery: connection refused"}`))
	}))
	defer srv.Close()

	t.Setenv("WH_SERVER_PORT", portFromListener(t, srv.Listener))
	assert.Equal(t, 1, runHealthCheck(), "503 should map to exit 1 — Docker marks unhealthy")
}

func TestRunHealthCheck_ConnectionRefused(t *testing.T) {
	// Wavehouse hasn't bound :8080 yet (e.g., still in the pre-listen
	// phase of `run()`, or crashed entirely). Bind+close a TCP listener
	// to claim a port that's now guaranteed unused, so the probe's GET
	// gets a real connect-refused rather than a slow timeout.
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := portFromListener(t, ln)
	require.NoError(t, ln.Close())

	t.Setenv("WH_SERVER_PORT", port)
	assert.Equal(t, 1, runHealthCheck(), "connection refused should map to exit 1")
}

func TestRunHealthCheck_InvalidPortEnv(t *testing.T) {
	// Garbage env var should fail fast at the parse step, not propagate
	// a confusing dial error. Catches the case where an operator
	// fat-fingers WH_SERVER_PORT in a deployment manifest.
	t.Setenv("WH_SERVER_PORT", "not-a-number")
	assert.Equal(t, 1, runHealthCheck(), "non-integer WH_SERVER_PORT should exit 1")
}
