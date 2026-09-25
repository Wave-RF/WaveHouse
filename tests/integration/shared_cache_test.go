//go:build integration

package tests

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
)

// Pinned, as internal/cache's integration suite pins it.
const redisImage = "redis:8.10.2-alpine"

// minCacheTTL is cache.QueryTimeToTTL's floor: a fill made less than this
// long ago cannot have expired, so a miss inside it is an invalidation.
const minCacheTTL = 10 * time.Second

var cachePrefixes atomic.Uint64

// startRedis runs a throwaway Redis with no persistence, its /data on tmpfs
// so the image's VOLUME leaves no anonymous volume behind.
func startRedis(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        redisImage,
			Cmd:          []string{"redis-server", "--save", "", "--appendonly", "no"},
			ExposedPorts: []string{"6379/tcp"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Tmpfs = map[string]string{"/data": ""}
			},
			WaitingFor: wait.ForLog("Ready to accept connections").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)
	return ctr, net.JoinHostPort(host, port.Port())
}

// bootRedisApp runs a second, independent WaveHouse — its own embedded NATS,
// ingest worker and data_dir — against the suite's ClickHouse, with
// cache.backend=redis. It returns the instance's base URL.
func bootRedisApp(t *testing.T, redisAddr, prefix string, timeout time.Duration) string {
	t.Helper()
	e := env(t)
	ctx := context.Background()
	settingsDir, err := writeTestSettings(e.ch)
	require.NoError(t, err)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:    t.TempDir(),
		Server:     config.Server{ShutdownTimeout: 10},
		ClickHouse: config.ClickHouse{Password: testCHPassword},
		MQ:         config.MQ{Backend: config.MQEmbedded},
		Cache: config.Cache{Backend: config.CacheRedis, Redis: config.CacheRedisConfig{
			Addrs: []string{redisAddr}, Mode: config.RedisStandalone, KeyPrefix: prefix,
			Timeout: timeout, DialTimeout: 2 * time.Second,
			MaxValueBytes: 1 << 20, CompressMinBytes: 1 << 10, VersionTTL: time.Hour,
		}},
		Dedupe:   config.Dedupe{Backend: config.DedupePebble},
		Coord:    config.Coord{Backend: config.CoordLocal},
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
	return baseURL
}

// structuredQuery posts a select-all structured query and returns the
// status, the X-Cache header and the body.
func structuredQuery(t *testing.T, baseURL, table string) (int, string, string) {
	t.Helper()
	status, xc, body, err := tryStructuredQuery(baseURL, table)
	require.NoError(t, err)
	return status, xc, body
}

// tryStructuredQuery is structuredQuery for an Eventually condition, which
// runs off the test goroutine and so must not call require.
func tryStructuredQuery(baseURL, table string) (int, string, string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		baseURL+"/v1/query?table="+url.QueryEscape(table), strings.NewReader(`{"select_all":true}`))
	if err != nil {
		return 0, "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("X-Cache"), string(body), err
}

func ingestRow(t *testing.T, baseURL, table, user string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/v1/ingest?table="+url.QueryEscape(table), "application/json",
		strings.NewReader(fmt.Sprintf(`{"user_id":%q,"value":1}`, user)))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// Two WaveHouse processes share one Redis and one ClickHouse. A result one
// fills is a hit for the other, and an insert one process's worker makes
// invalidates what the other cached: the other's next query is a miss that
// returns the new row, well inside the TTL the stale entry was filed with.
func TestSharedCache_IngestOnOneInstanceInvalidatesAnother(t *testing.T) {
	table := createTable(t, "user_id String, value Float64", "ORDER BY user_id")
	_, redisAddr := startRedis(t)
	prefix := fmt.Sprintf("it%d", cachePrefixes.Add(1))
	a := bootRedisApp(t, redisAddr, prefix, 2*time.Second)
	b := bootRedisApp(t, redisAddr, prefix, 2*time.Second)

	rc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{redisAddr}, DisableCache: true, ForceSingleClient: true})
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	tableToken := func() string {
		v, err := rc.Do(context.Background(), rc.B().Get().Key(prefix+":{0}:B:"+table).Build()).ToString()
		if rueidis.IsRedisNil(err) {
			return ""
		}
		require.NoError(t, err)
		return v
	}

	status, xc, body := structuredQuery(t, b, table)
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, "MISS", xc)
	status, xc, _ = structuredQuery(t, a, table)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", xc, "a fills, b hits: one cache")

	// An insert's worker and its invalidation run on whichever process took
	// the ingest, so the discriminating window is the stale entry's TTL: if
	// the batch window and load push the new row past it, the round proves
	// nothing and runs again with a fresh fill.
	for round := 1; ; round++ {
		user := fmt.Sprintf("user-%d", round)
		status, xc, body = structuredQuery(t, b, table)
		require.Equal(t, http.StatusOK, status, body)
		require.NotContains(t, body, user)
		filled := time.Now()
		if xc == "HIT" {
			// The previous round's fill: refresh it so the TTL window starts now.
			_, err := rc.Do(context.Background(), rc.B().Flushdb().Build()).ToString()
			require.NoError(t, err)
			status, xc, body = structuredQuery(t, b, table)
			require.Equal(t, http.StatusOK, status, body)
			require.Equal(t, "MISS", xc)
			filled = time.Now()
		}
		before := tableToken()

		ingestRow(t, a, table, user)
		var seenAt time.Time
		require.Eventually(t, func() bool {
			var err error
			status, xc, body, err = tryStructuredQuery(b, table)
			if err == nil && status == http.StatusOK && strings.Contains(body, user) {
				seenAt = time.Now()
				return true
			}
			return false
		}, 30*time.Second, 100*time.Millisecond, "b never served the row ingested through a")
		assert.NotEqual(t, before, tableToken(), "a's worker bumped the table token in the shared server")
		if seenAt.Sub(filled) < minCacheTTL-time.Second {
			assert.Equal(t, "MISS", xc, "the first answer carrying the new row is b's refill")
			status, xc, _ = structuredQuery(t, b, table)
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, "HIT", xc, "b's refill is cached again")
			return
		}
		require.Less(t, round, 3, "b served the new row only once its stale entry could have expired, in every round: the invalidation never reached it, or ingest is too slow here to tell")
		t.Logf("round %d: row landed %s after the fill, past the TTL floor; retrying", round, seenAt.Sub(filled))
	}
}

// A Redis that stops answering costs queries nothing but the cache: they
// keep succeeding, straight from ClickHouse, each a miss; an ingest made
// meanwhile is visible at once. Once it answers again, the cache serves hits.
func TestSharedCache_RedisDownQueriesBypass(t *testing.T) {
	ctx := context.Background()
	table := createTable(t, "user_id String, value Float64", "ORDER BY user_id")
	ctr, redisAddr := startRedis(t)
	const timeout = 200 * time.Millisecond
	a := bootRedisApp(t, redisAddr, fmt.Sprintf("it%d", cachePrefixes.Add(1)), timeout)

	status, xc, _ := structuredQuery(t, a, table)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "MISS", xc)
	status, xc, _ = structuredQuery(t, a, table)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", xc)

	d, err := testcontainers.NewDockerClientWithOpts(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	_, err = d.ContainerPause(ctx, ctr.GetContainerID(), client.ContainerPauseOptions{})
	require.NoError(t, err)
	paused := true
	unpause := func() {
		if paused {
			paused = false
			_, err := d.ContainerUnpause(ctx, ctr.GetContainerID(), client.ContainerUnpauseOptions{})
			require.NoError(t, err)
		}
	}
	t.Cleanup(unpause)

	for range 8 {
		start := time.Now()
		status, xc, body := structuredQuery(t, a, table)
		require.Equal(t, http.StatusOK, status, body)
		assert.Equal(t, "MISS", xc)
		// A lookup and a fill each wait at most the timeout; the query itself
		// is a few ms. Generous for -race under load.
		assert.Less(t, time.Since(start), 5*timeout+2*time.Second)
	}

	ingestRow(t, a, table, "while-down")
	require.Eventually(t, func() bool {
		status, _, body, err := tryStructuredQuery(a, table)
		return err == nil && status == http.StatusOK && strings.Contains(body, "while-down")
	}, 30*time.Second, 200*time.Millisecond, "a row ingested while the cache is down is served")

	unpause()
	require.Eventually(t, func() bool {
		_, xc, body, err := tryStructuredQuery(a, table)
		return err == nil && xc == "HIT" && strings.Contains(body, "while-down")
	}, 30*time.Second, 200*time.Millisecond, "the cache serves hits again once the server answers")
}
