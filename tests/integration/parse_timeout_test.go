//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
)

// TestIngest_ParseTimeLimitIsWired drives the boot config's
// ingest.parse_timeout through app.New to the handler: a process whose limit
// is 1ns refuses a body longer than one parse window with 422 once its first
// window is read, while the shared process, at the 30s default, accepts the
// same body. A tenant's own value cannot go under 1s; the boot default can.
func TestIngest_ParseTimeLimitIsWired(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	table := createTable(t, "id UInt32", "ORDER BY id")
	var b strings.Builder
	for i := range 5000 { // more than one window (4,096 records)
		fmt.Fprintf(&b, "{\"id\":%d}\n", i)
	}
	body := b.String()

	post := func(baseURL string) (int, string, string) {
		resp, err := httpPost(ctx, baseURL+"/v1/ingest?table="+url.QueryEscape(table), "application/x-ndjson", strings.NewReader(body))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, resp.Header.Get("Retry-After"), string(raw)
	}

	status, _, raw := post(e.baseURL)
	require.Equal(t, http.StatusOK, status, "the body is valid under the default limit: %s", raw)

	settingsDir, err := writeTestSettings(e.ch)
	require.NoError(t, err)
	t.Cleanup(func() { removeTestSettings(settingsDir) })
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:  t.TempDir(),
		Server:   config.Server{ShutdownTimeout: 10},
		MQ:       embeddedMQ(),
		Cache:    config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:   config.Dedupe{Backend: config.DedupePebble},
		Coord:    config.Coord{Backend: config.CoordLocal},
		Ingest:   config.Ingest{ParseTimeout: time.Nanosecond, MaxParseTimeout: time.Minute},
		Roles:    config.AllRoles(),
		Settings: config.Settings{Dir: settingsDir},
	}
	a, err := app.New(ctx, app.Options{Config: cfg, Listener: ln})
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		assert.NoError(t, <-runDone)
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		assert.NoError(t, a.Close(closeCtx))
	})
	baseURL := "http://" + ln.Addr().String()
	require.NoError(t, waitForLive(ctx, baseURL, 30*time.Second))

	status, retryAfter, raw := post(baseURL)
	require.Equal(t, http.StatusUnprocessableEntity, status, raw)
	assert.Empty(t, retryAfter, "a retry would take as long")
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Equal(t, "ingest validation took longer than the time limit (0.000000001s)", got["error"])
}
