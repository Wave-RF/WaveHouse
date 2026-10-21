package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

func TestScrapeStorage(t *testing.T) {
	t.Parallel()
	// acme and initech share a database; globex has its own.
	keys := map[tenant.ID]string{"acme": "db1", "globex": "db2", "initech": "db1"}
	data := map[string]map[string]tableBytes{
		"db1": {"events": {uncompressed: 100, onDisk: 10}, "clicks": {uncompressed: 7, onDisk: 3}},
		"db2": {"events": {uncompressed: 50, onDisk: 5}},
	}
	var mu sync.Mutex
	var reads []tenant.ID
	read := func(ctx context.Context, id tenant.ID) (map[string]tableBytes, error) {
		deadline, ok := ctx.Deadline()
		assert.True(t, ok, "the scrape is bounded")
		assert.WithinDuration(t, time.Now().Add(storageScrapeTimeout), deadline, time.Second)
		mu.Lock()
		reads = append(reads, id)
		mu.Unlock()
		return data[keys[id]], nil
	}

	got, err := scrapeStorage(t.Context(), []tenant.ID{"acme", "globex", "initech"},
		func(id tenant.ID) string { return keys[id] }, read)
	require.NoError(t, err)
	assert.ElementsMatch(t, []tenant.ID{"acme", "globex"}, reads, "a database two tenants share is read once")
	assert.Equal(t, map[string]tableBytes{
		"events": {uncompressed: 150, onDisk: 15},
		"clicks": {uncompressed: 7, onDisk: 3},
	}, got, "a table name in several databases reports their sum")

	failing := func(_ context.Context, id tenant.ID) (map[string]tableBytes, error) {
		if id == "globex" {
			return nil, errors.New("clickhouse down")
		}
		return data[keys[id]], nil
	}
	got, err = scrapeStorage(t.Context(), []tenant.ID{"acme", "globex"},
		func(id tenant.ID) string { return keys[id] }, failing)
	require.ErrorContains(t, err, "tenant globex: clickhouse down")
	assert.Nil(t, got, "one database failing fails the whole scrape")

	got, err = scrapeStorage(t.Context(), nil, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, got, "no tenants, no tables")
}

func TestRegisterStorageMetrics(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tables := map[string]tableBytes{
		"events": {uncompressed: 75_000_000_000, onDisk: 1_400_000_000},
		"clicks": {uncompressed: 10, onDisk: 3},
	}
	var scrapeErr error
	reg, err := registerStorageMetrics(mp.Meter("test"), func(context.Context) (map[string]tableBytes, error) {
		if scrapeErr != nil {
			return nil, scrapeErr
		}
		return tables, nil
	}, 0) // no reuse: every collection scrapes
	require.NoError(t, err)

	got := collectSettingsMetrics(t, reader)
	assert.Equal(t, map[string]int64{"table=events": 75_000_000_000, "table=clicks": 10}, got["wavehouse_clickhouse_uncompressed_bytes"])
	assert.Equal(t, map[string]int64{"table=events": 1_400_000_000, "table=clicks": 3}, got["wavehouse_clickhouse_bytes_on_disk"])
	assertNonMonotonicSums(t, reader)

	scrapeErr = errors.New("clickhouse down")
	assert.Empty(t, collectSettingsMetrics(t, reader), "a failed scrape leaves the series absent, not stale")

	scrapeErr = nil
	got = collectSettingsMetrics(t, reader)
	assert.Equal(t, map[string]int64{"table=events": 1_400_000_000, "table=clicks": 3}, got["wavehouse_clickhouse_bytes_on_disk"], "the next good scrape reports again")

	require.NoError(t, reg.Unregister())
	assert.Empty(t, collectSettingsMetrics(t, reader), "Close unregisters the callback")
}

// assertNonMonotonicSums checks the storage instruments are up-down
// counters: the table label may only sit on a sum, which
// prometheus.per_table_metrics: false folds into a total (#538).
func assertNonMonotonicSums(t *testing.T, reader *sdkmetric.ManualReader) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	seen := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is a sum, not a gauge", m.Name)
			assert.False(t, sum.IsMonotonic, "%s can go down", m.Name)
			seen++
		}
	}
	assert.Equal(t, 2, seen)
}

func TestReadParts(t *testing.T) {
	t.Parallel()
	q := &stubPartsQuerier{rows: &stubPartsRows{rows: []partsRow{
		{"events", 100, 10},
		{"clicks", 7, 3},
	}}}
	got, err := readParts(t.Context(), q)
	require.NoError(t, err)
	assert.Equal(t, partsQuery, q.query)
	assert.Equal(t, map[string]tableBytes{
		"events": {uncompressed: 100, onDisk: 10},
		"clicks": {uncompressed: 7, onDisk: 3},
	}, got)

	_, err = readParts(t.Context(), &stubPartsQuerier{err: errors.New("clickhouse down")})
	require.ErrorContains(t, err, "clickhouse down")

	_, err = readParts(t.Context(), &stubPartsQuerier{rows: &stubPartsRows{
		rows: []partsRow{{"events", 1, 1}}, err: errors.New("stream aborted"),
	}})
	require.ErrorContains(t, err, "stream aborted", "a mid-stream failure fails the read")

	_, err = readParts(t.Context(), &stubPartsQuerier{rows: &stubPartsRows{
		rows: []partsRow{{"events", 1, 1}}, scanErr: errors.New("bad column"),
	}})
	require.ErrorContains(t, err, "bad column")
}

type stubPartsQuerier struct {
	rows  driver.Rows
	err   error
	query string
}

func (q *stubPartsQuerier) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	q.query = query
	if q.err != nil {
		return nil, q.err
	}
	return q.rows, nil
}

type partsRow struct {
	table                string
	uncompressed, onDisk uint64
}

// stubPartsRows serves partsRow tuples; the embedded nil driver.Rows panics
// on any method readParts should not call.
type stubPartsRows struct {
	driver.Rows
	rows    []partsRow
	err     error
	scanErr error
	i       int
}

func (r *stubPartsRows) Next() bool { r.i++; return r.i <= len(r.rows) }

func (r *stubPartsRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	*(dest[0].(*string)) = row.table
	*(dest[1].(*uint64)) = row.uncompressed
	*(dest[2].(*uint64)) = row.onDisk
	return nil
}

func (r *stubPartsRows) Err() error { return r.err }

func (*stubPartsRows) Close() error { return nil }

func TestTablesKey(t *testing.T) {
	t.Parallel()
	base := chconn.Params{
		Addr: "ch:9000", HTTPPort: 8123, HTTPScheme: "http", Database: "db1",
		Username: "u", QueryTimeout: 30, MaxOpenConns: 1, MaxIdleConns: 1,
	}
	otherHTTP, otherUser, otherPort, otherDB := base, base, base, base
	otherHTTP.HTTPPort, otherHTTP.HTTPScheme = 8443, "https"
	otherUser.Username = "v"
	otherPort.Addr = "ch:9440"
	otherDB.Database = "db2"
	pools, err := chconn.NewPools(100, []chconn.Member{
		{Tenant: "a", Params: base},
		{Tenant: "b", Params: otherHTTP},
		{Tenant: "c", Params: otherUser},
		{Tenant: "d", Params: otherPort},
		{Tenant: "e", Params: otherDB},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pools.Close() })
	key := func(id tenant.ID) string { return tablesKey(pools.For(id)) }

	assert.Equal(t, key("a"), key("b"), "another HTTP port or scheme reads the same tables")
	assert.Equal(t, key("a"), key("c"), "another user reads the same tables")
	assert.NotEqual(t, key("a"), key("d"), "another native address is another server")
	assert.NotEqual(t, key("a"), key("e"), "another database is other tables")
}

func TestRegisterStorageMetrics_ReusesScrape(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	var scrapes int
	var scrapeErr error
	_, err := registerStorageMetrics(mp.Meter("test"), func(context.Context) (map[string]tableBytes, error) {
		scrapes++
		if scrapeErr != nil {
			return nil, scrapeErr
		}
		return map[string]tableBytes{"events": {uncompressed: int64(scrapes), onDisk: 1}}, nil
	}, time.Hour)
	require.NoError(t, err)

	scrapeErr = errors.New("clickhouse down")
	assert.Empty(t, collectSettingsMetrics(t, reader))
	assert.Empty(t, collectSettingsMetrics(t, reader))
	assert.Equal(t, 2, scrapes, "a failed scrape is not reused: the next collection tries again")

	scrapeErr = nil
	for range 3 {
		got := collectSettingsMetrics(t, reader)
		assert.Equal(t, map[string]int64{"table=events": 3}, got["wavehouse_clickhouse_uncompressed_bytes"])
	}
	assert.Equal(t, 3, scrapes, "a successful scrape is reused within the ttl")
}

func TestScrapeStorage_CapsConcurrency(t *testing.T) {
	t.Parallel()
	var ids []tenant.ID
	for i := range 3 * storageScrapeConcurrency {
		ids = append(ids, tenant.ID(fmt.Sprintf("t%d", i)))
	}
	var inFlight, peak atomic.Int64
	_, err := scrapeStorage(t.Context(), ids,
		func(id tenant.ID) string { return string(id) },
		func(context.Context, tenant.ID) (map[string]tableBytes, error) {
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return nil, nil
		})
	require.NoError(t, err)
	assert.LessOrEqual(t, peak.Load(), int64(storageScrapeConcurrency), "at most storageScrapeConcurrency databases are read at once")
}
