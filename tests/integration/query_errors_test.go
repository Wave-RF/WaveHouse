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
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil/storedir"
)

// queryError is a query's answer: the status and raw body, and the error
// envelope a failed ClickHouse query answers with.
type queryError struct {
	status     int
	retryAfter string
	raw        string
	Error      string `json:"error"`
	Code       string `json:"code"`
	Retryable  *bool  `json:"retryable"`
}

func postJSON(t *testing.T, url, body string) queryError {
	t.Helper()
	return postJSONAs(t, url, body, "")
}

// postJSONAs is postJSON as the role an Authorization value names (bearer);
// "" is the suite's default caller.
func postJSONAs(t *testing.T, url, body, authorization string) queryError {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := queryError{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), raw: string(raw)}
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
		assert.Contains(t, strings.ToLower(got.Error), "code: 47")
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
	t.Cleanup(func() { removeTestSettings(settingsDir) })

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:    storedir.New(t),
		Server:     config.Server{ShutdownTimeout: 10},
		ClickHouse: config.ClickHouse{Password: testCHPassword},
		MQ:         config.MQ{Backend: config.MQEmbedded},
		Cache:      config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:     config.Dedupe{Backend: config.DedupePebble},
		Coord:      config.Coord{Backend: config.CoordLocal},
		Roles:      config.AllRoles(),
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

// TestQueryErrors_RoleTimeCapIsTheCallers: a role's max_execution_time
// reaches ClickHouse as the query's own setting — the HTTP reader sends one on
// every read — and a query that outruns it is the caller's: 400
// clickhouse.limit_exceeded, not retryable, where an overrun of the tenant's
// query_timeout alone would read as ClickHouse's state. The slow table is a
// view that sleeps half a second per row; measured on 26.8, a 1 s cap ends it
// with TIMEOUT_EXCEEDED (159) where the uncapped query returns its four rows.
func TestQueryErrors_RoleTimeCapIsTheCallers(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	view := fmt.Sprintf("it_slow_view_%d", tableCounter.Add(1))
	require.NoError(t, e.chConn.Exec(ctx, "CREATE VIEW "+view+" AS SELECT number + sleepEachRow(0.5) AS n FROM numbers(4)"))
	t.Cleanup(func() { _ = e.chConn.Exec(context.Background(), "DROP VIEW IF EXISTS "+view) })
	require.NoError(t, e.registry.Refresh(ctx))
	withPolicy(t, policy.Policy{Tables: map[string]policy.TablePolicy{view: {
		"capped": {Select: &policy.SelectPermissions{AllowColumns: []string{"*"}, MaxExecutionTime: 1000 /* ms */}},
	}}})

	got := postJSONAs(t, e.baseURL+"/v1/query?table="+view, `{"columns":["n"]}`, bearer(t, "capped", nil))
	assertQueryError(t, got, http.StatusBadRequest, "clickhouse.limit_exceeded", false)
	assert.Contains(t, got.Error, "TIMEOUT_EXCEEDED")
}
