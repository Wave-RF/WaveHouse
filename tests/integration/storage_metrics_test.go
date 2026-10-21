//go:build integration

// Storage-metric integration test.
//
// The unit tests in internal/app stub the system.parts read, so a misspelled
// column, a syntax error, or an aggregate whose type no longer matches the
// Scan targets would pass them, and a failed scrape only leaves the series
// absent. Only a real round-trip through the wired pools catches it.

package tests

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/testutil/storedir"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestStorageMetrics_PerTenantDatabases boots the real wiring with three
// tenants over two databases (acme and initech share one) and checks that
// /metrics reports each table's bytes summed over the databases, each
// database read once however many tenants are on it.
func TestStorageMetrics_PerTenantDatabases(t *testing.T) {
	// No t.Parallel: app.New with Prometheus on sets the global meter
	// provider, which guardOTelGlobals restores.
	guardOTelGlobals(t)
	e := env(t)
	ctx := context.Background()

	name := strings.ToLower(strings.NewReplacer("/", "_", " ", "_", "-", "_").Replace(t.Name()))
	db1, db2 := "it_storage_a_"+name, "it_storage_b_"+name
	for db, rows := range map[string]string{db1: "('1'), ('2'), ('3')", db2: "('4')"} {
		require.NoError(t, e.chConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db))
		t.Cleanup(func() { _ = e.chConn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
		require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.events (id String) ENGINE = MergeTree() ORDER BY id", db)))
		require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.events VALUES %s", db, rows)))
	}
	// db1 also has a table db2 lacks.
	require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.clicks (id String) ENGINE = MergeTree() ORDER BY id", db1)))
	require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.clicks VALUES ('c')", db1)))

	root := t.TempDir()
	for id, db := range map[tenant.ID]string{"acme": db1, "globex": db2, "initech": db1} {
		files, err := tenantSettings(e.ch, db)
		require.NoError(t, err)
		require.NoError(t, writeSettingsFiles(filepath.Join(root, id.String()), files))
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:    storedir.New(t),
		Server:     config.Server{ShutdownTimeout: 10},
		MQ:         embeddedMQ(),
		Cache:      config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:     config.Dedupe{Backend: config.DedupePebble},
		Coord:      config.Coord{Backend: config.CoordLocal},
		Roles:      config.AllRoles(),
		Settings:   config.Settings{Dir: root},
		Prometheus: config.Prometheus{Enabled: true, Path: "/metrics", PerTableMetrics: true},
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

	// What system.parts holds, read directly: one part per insert, so
	// nothing merges under the test.
	want := func(column, table string) int64 {
		var v uint64
		require.NoError(t, e.chConn.QueryRow(ctx, fmt.Sprintf(
			"SELECT sum(%s) FROM system.parts WHERE active AND database IN (?, ?) AND table = ?", column),
			db1, db2, table).Scan(&v))
		require.Positive(t, v)
		return int64(v)
	}

	var got map[string]int64
	require.Eventually(t, func() bool {
		got = scrapeStorageMetrics(t, baseURL+"/metrics")
		return len(got) == 4
	}, 30*time.Second, 200*time.Millisecond, "storage series never appeared: %v", got)
	assert.Equal(t, map[string]int64{
		`wavehouse_clickhouse_uncompressed_bytes{table="events"}`: want("data_uncompressed_bytes", "events"),
		`wavehouse_clickhouse_bytes_on_disk{table="events"}`:      want("bytes_on_disk", "events"),
		`wavehouse_clickhouse_uncompressed_bytes{table="clicks"}`: want("data_uncompressed_bytes", "clicks"),
		`wavehouse_clickhouse_bytes_on_disk{table="clicks"}`:      want("bytes_on_disk", "clicks"),
	}, got, "each table summed over both databases, db1 counted once though two tenants are on it")
}

// storageSeries matches a storage series in the Prometheus text format and
// captures its name, its table label and its value.
var storageSeries = regexp.MustCompile(`^(wavehouse_clickhouse_(?:uncompressed_bytes|bytes_on_disk))\{.*table="([^"]+)".*\} (\S+)$`)

// scrapeStorageMetrics reads the storage series at url, keyed
// name{table="…"}, for the tables this test created.
func scrapeStorageMetrics(t *testing.T, url string) map[string]int64 {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	out := map[string]int64{}
	for line := range strings.SplitSeq(string(body), "\n") {
		m := storageSeries.FindStringSubmatch(line)
		if m == nil || (m[2] != "events" && m[2] != "clicks") {
			continue
		}
		v, err := strconv.ParseFloat(m[3], 64)
		require.NoError(t, err, line)
		out[fmt.Sprintf(`%s{table=%q}`, m[1], m[2])] = int64(v)
	}
	return out
}
