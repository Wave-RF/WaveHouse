package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// storageScrapeTimeout bounds one collection's ClickHouse storage scrape, so
// a hung server cannot stall the whole collection (the driver's default read
// timeout is minutes).
const storageScrapeTimeout = 5 * time.Second

// storageScrapeTTL is how long a successful scrape is reused. Storage size
// moves slowly, and without it every collection (each OTLP export and each
// GET /metrics) would query every tenant database again.
const storageScrapeTTL = 15 * time.Second

// storageScrapeConcurrency caps the databases one scrape reads at once, so a
// process serving many tenant databases on one server does not open a burst
// of queries against it.
const storageScrapeConcurrency = 8

// partsQuery sums the active parts of the connection's own database per
// table: what is stored now, so TTL expiry and merges shrink it.
const partsQuery = `SELECT table, sum(data_uncompressed_bytes), sum(bytes_on_disk)
FROM system.parts
WHERE active AND database = currentDatabase()
GROUP BY table`

// tableBytes is one table's live data before and after compression.
type tableBytes struct {
	uncompressed, onDisk int64
}

// wireStorageMetrics exports how much data each table holds in ClickHouse,
// before and after compression (#417). A file of its own so wire.go is left
// alone (#777). Each served tenant's database is read once per collection
// over its own pool; tenants on one address and database read the same
// tables and are read once. No series carries a tenant (#538): a table name
// several databases share reports their sum.
func (a *App) wireStorageMetrics() {
	if a.pools == nil || (!a.cfg.OTel.Enabled && !a.cfg.Prometheus.Enabled) {
		return
	}
	reg, err := registerStorageMetrics(otel.Meter("wavehouse-storage"), a.scrapeStorage, storageScrapeTTL)
	if err != nil {
		slog.Error("failed to register storage metrics", "error", err)
		return
	}
	a.add(component{name: "storage metrics", close: withoutContext(reg.Unregister)})
}

// scrapeStorage reads every served tenant's database over its pool, once per
// address and database: the pool's own, as chconn.Pools.SharingTables groups
// tenants, so the grouping matches the connection read. A tenant on no pool
// (its tuple could not be opened) has no tables to report and is skipped,
// which under-reports nothing.
func (a *App) scrapeStorage(ctx context.Context) (map[string]tableBytes, error) {
	var ids []tenant.ID
	pools := map[tenant.ID]*chconn.Manager{}
	for id := range a.tenants.All() {
		if m := a.pools.For(id); m != nil {
			ids = append(ids, id)
			pools[id] = m
		}
	}
	return scrapeStorage(ctx, ids,
		func(id tenant.ID) string { return tablesKey(pools[id]) },
		func(ctx context.Context, id tenant.ID) (map[string]tableBytes, error) {
			return readParts(ctx, pools[id])
		})
}

// tablesKey names the tables m reads: its address and database, whatever its
// user or tls block.
func tablesKey(m *chconn.Manager) string {
	id := m.Identity()
	return id.Addr + "\x00" + id.Database
}

// partsQuerier is the slice of chconn.Manager that readParts uses.
type partsQuerier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// readParts runs partsQuery over q.
func readParts(ctx context.Context, q partsQuerier) (map[string]tableBytes, error) {
	rows, err := q.Query(ctx, partsQuery)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]tableBytes{}
	for rows.Next() {
		var table string
		var uncompressed, onDisk uint64
		if err := rows.Scan(&table, &uncompressed, &onDisk); err != nil {
			return nil, err
		}
		// #nosec G115 -- byte totals from system.parts can't exceed MaxInt64 (~9.2 EB).
		out[table] = tableBytes{uncompressed: int64(uncompressed), onDisk: int64(onDisk)}
	}
	return out, rows.Err()
}

// scrapeStorage reads the tables of ids' databases with read, at once, once
// per distinct key (the address and database the tenant's tables are on),
// and sums each table name over them. Any failure fails the whole scrape:
// with one database missing, a table's sum would drop as if it had shrunk.
func scrapeStorage(
	ctx context.Context,
	ids []tenant.ID,
	key func(tenant.ID) string,
	read func(context.Context, tenant.ID) (map[string]tableBytes, error),
) (map[string]tableBytes, error) {
	seen := map[string]bool{}
	var reps []tenant.ID
	for _, id := range ids {
		if k := key(id); !seen[k] {
			seen[k] = true
			reps = append(reps, id)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, storageScrapeTimeout)
	defer cancel()
	results := make([]map[string]tableBytes, len(reps))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(storageScrapeConcurrency)
	for i, id := range reps {
		g.Go(func() error {
			r, err := read(gctx, id)
			if err != nil {
				return fmt.Errorf("tenant %s: %w", id, err)
			}
			results[i] = r
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	sum := map[string]tableBytes{}
	for _, r := range results {
		for table, b := range r {
			s := sum[table]
			s.uncompressed += b.uncompressed
			s.onDisk += b.onDisk
			sum[table] = s
		}
	}
	return sum, nil
}

// registerStorageMetrics registers the storage instruments on meter, read
// from scrape on a collection more than ttl after the last successful one,
// and from that one's result before. They are observable up-down counters, not
// gauges, so prometheus.per_table_metrics: false sums them over every table
// (observability's tableView). A failed scrape observes nothing, so the
// series go absent for the collection rather than stale or partial, and the
// next collection scrapes again. It is
// logged when the scrape starts failing, not on every collection after.
func registerStorageMetrics(meter metric.Meter, scrape func(context.Context) (map[string]tableBytes, error), ttl time.Duration) (metric.Registration, error) {
	var failing atomic.Bool
	// The last successful scrape, reused for ttl. mu also serializes scrapes,
	// so two readers collecting at once (OTLP and Prometheus) share one.
	var (
		mu     sync.Mutex
		last   map[string]tableBytes
		lastAt time.Time
	)
	cached := func(ctx context.Context) (map[string]tableBytes, error) {
		mu.Lock()
		defer mu.Unlock()
		if last != nil && time.Since(lastAt) < ttl {
			return last, nil
		}
		tables, err := scrape(ctx)
		if err != nil {
			return nil, err
		}
		last, lastAt = tables, time.Now()
		return tables, nil
	}
	var errs [3]error
	var uncompressed, onDisk metric.Int64ObservableUpDownCounter
	uncompressed, errs[0] = meter.Int64ObservableUpDownCounter("wavehouse_clickhouse_uncompressed_bytes",
		metric.WithDescription("Uncompressed bytes of the live data in each table (active parts in system.parts): the closest analogue to bytes ingested"))
	onDisk, errs[1] = meter.Int64ObservableUpDownCounter("wavehouse_clickhouse_bytes_on_disk",
		metric.WithDescription("Bytes each table occupies on disk (active parts in system.parts): compressed data plus marks and index files"))
	var reg metric.Registration
	reg, errs[2] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		tables, err := cached(ctx)
		if err != nil {
			if failing.CompareAndSwap(false, true) {
				slog.Warn("clickhouse storage scrape failed; its metrics are absent until it succeeds", "error", err)
			}
			return nil
		}
		if failing.CompareAndSwap(true, false) {
			slog.Info("clickhouse storage scrape succeeded again")
		}
		for _, table := range slices.Sorted(maps.Keys(tables)) {
			b := tables[table]
			attrs := metric.WithAttributes(attribute.String("table", table))
			o.ObserveInt64(uncompressed, b.uncompressed, attrs)
			o.ObserveInt64(onDisk, b.onDisk, attrs)
		}
		return nil
	}, uncompressed, onDisk)
	if err := errors.Join(errs[:]...); err != nil {
		return nil, err
	}
	return reg, nil
}
