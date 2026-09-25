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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/config"
)

// queryError is the error envelope a failed ClickHouse query answers with.
type queryError struct {
	status     int
	retryAfter string
	Error      string `json:"error"`
	Code       string `json:"code"`
	Retryable  *bool  `json:"retryable"`
}

func postJSON(t *testing.T, url, body string) queryError {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := queryError{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After")}
	if resp.StatusCode != http.StatusOK {
		require.NoError(t, json.Unmarshal(raw, &got), string(raw))
	}
	return got
}

func assertQueryError(t *testing.T, got queryError, status int, code string, retryable bool) {
	t.Helper()
	require.Equal(t, status, got.status, got.Error)
	assert.Equal(t, code, got.Code, got.Error)
	require.NotNil(t, got.Retryable, got.Error)
	assert.Equal(t, retryable, *got.Retryable, got.Error)
}

// TestQueryErrors_CallerFault: statements a live ClickHouse judges and
// refuses are the caller's — 4xx, not retryable — on both query paths, even
// though ClickHouse answers each with HTTP 500 (#403, #271).
func TestQueryErrors_CallerFault(t *testing.T) {
	e := env(t)

	t.Run("raw SQL syntax error", func(t *testing.T) {
		got := postJSON(t, e.baseURL+"/v1/ops/query", `{"sql":"SELEC 1"}`)
		assertQueryError(t, got, http.StatusBadRequest, "clickhouse.rejected", false)
		assert.Contains(t, got.Error, "SYNTAX_ERROR")
	})

	// The #403 repro: a statement the configured ClickHouse user lacks the
	// grant for (the test container's default user cannot manage users).
	t.Run("raw SQL missing grant", func(t *testing.T) {
		got := postJSON(t, e.baseURL+"/v1/ops/query", `{"sql":"CREATE USER it_query_errors_denied"}`)
		assertQueryError(t, got, http.StatusForbidden, "clickhouse.access_denied", false)
		assert.Contains(t, got.Error, "ACCESS_DENIED")
	})

	// A column dropped behind the schema registry's back reaches ClickHouse,
	// which refuses it: the SDK-bad-column shape of #271.
	t.Run("structured query on a dropped column", func(t *testing.T) {
		table := createTable(t, "id String, page String", "ORDER BY id")
		require.NoError(t, e.chConn.Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s DROP COLUMN page", table)))
		got := postJSON(t, e.baseURL+"/v1/query?table="+url.QueryEscape(table), `{"columns":["page"]}`)
		assertQueryError(t, got, http.StatusBadRequest, "clickhouse.rejected", false)
		assert.Contains(t, got.Error, "code: 47")
	})
}

// TestQueryErrors_ClickHouseDown stops ClickHouse under a running server:
// both query paths answer 503, retryable, with Retry-After. Its own
// container and app, like the outage tests: the shared env assumes
// ClickHouse stays up.
func TestQueryErrors_ClickHouseDown(t *testing.T) {
	ctx := context.Background()

	ch, err := startClickHouse(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if ch.conn != nil {
			_ = ch.conn.Close()
		}
		_ = ch.container.Terminate(context.Background())
	})
	const table = "down_events"
	require.NoError(t, ch.conn.Exec(ctx, "CREATE TABLE "+table+" (id String) ENGINE = MergeTree ORDER BY id"))

	settingsDir, err := writeTestSettings(ch)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(settingsDir) })

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:    t.TempDir(),
		Server:     config.Server{ShutdownTimeout: 10},
		ClickHouse: config.ClickHouse{Password: testCHPassword},
		Cache:      config.Cache{L1MaxCost: 1 << 20},
		Settings:   config.Settings{Dir: settingsDir},
	}
	a, err := app.New(ctx, app.Options{Config: cfg, Listener: ln})
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		<-runDone
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Close(closeCtx)
	})
	baseURL := "http://" + ln.Addr().String()
	require.NoError(t, waitForLive(ctx, baseURL, 30*time.Second))

	structured := baseURL + "/v1/query?table=" + table
	require.Equal(t, http.StatusOK, postJSON(t, structured, `{"columns":["id"]}`).status, "the table must be served before the outage")

	stopTimeout := 10 * time.Second
	require.NoError(t, ch.container.Stop(ctx, &stopTimeout))

	for name, call := range map[string]func() queryError{
		"raw SQL": func() queryError { return postJSON(t, baseURL+"/v1/ops/query", `{"sql":"SELECT 1"}`) },
		// A filter the first query did not have, so the cache cannot answer.
		"structured query": func() queryError {
			return postJSON(t, structured, `{"columns":["id"],"filters":[{"column":"id","op":"eq","value":"x"}]}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := call()
			assertQueryError(t, got, http.StatusServiceUnavailable, "clickhouse.unavailable", true)
			assert.Equal(t, "5", got.retryAfter)
		})
	}
}

// TestQueryErrors_TimeCapReachesClickHouse pins the driver behaviour the
// role time cap depends on: with a context deadline over 1s, clickhouse-go
// overwrites max_execution_time with deadline+5s, so an overrun ends as a
// bare DeadlineExceeded; with no deadline the cap reaches ClickHouse, which
// reports TIMEOUT_EXCEEDED — the code /v1/query answers as the caller's.
func TestQueryErrors_TimeCapReachesClickHouse(t *testing.T) {
	e := env(t)
	capped := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"max_execution_time": 1}))
	const slow = "SELECT sleep(2) SETTINGS function_sleep_max_microseconds_per_block = 3000000"

	withDeadline, cancel := context.WithTimeout(capped, 1500*time.Millisecond)
	defer cancel()
	err := e.chConn.Exec(withDeadline, slow)
	require.Error(t, err)
	_, hasCode := chconn.ExceptionCode(err)
	assert.False(t, hasCode, "a deadline over 1s must still override the cap: %v", err)

	noDeadline, cancel2 := context.WithCancel(capped)
	defer cancel2()
	err = e.chConn.Exec(noDeadline, slow)
	require.Error(t, err)
	code, _ := chconn.ExceptionCode(err)
	assert.Equal(t, int32(159), code, "%v", err)
}
