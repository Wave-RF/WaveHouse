package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// storageScrapeTimeout bounds one collection's ClickHouse storage scrape, so
// a hung server cannot stall the whole collection (the driver's default read
// timeout is minutes).
const storageScrapeTimeout = 5 * time.Second

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
	reg, err := registerStorageMetrics(otel.Meter("wavehouse-storage"), a.scrapeStorage)
	if err != nil {
		slog.Error("failed to register storage metrics", "error", err)
		return
	}
	a.add(component{name: "storage metrics", close: withoutContext(reg.Unregister)})
}

// scrapeStorage reads every served tenant's database, once per address and
// database.
func (a *App) scrapeStorage(ctx context.Context) (map[string]tableBytes, error) {
	var ids []tenant.ID
	for id := range a.tenants.All() {
		ids = append(ids, id)
	}
	return scrapeStorage(ctx, ids,
		func(id tenant.ID) string {
			t := a.pools.Target(id)
			return t.URL + "\x00" + t.Database
		},
		func(ctx context.Context, id tenant.ID) (map[string]tableBytes, error) {
			m := a.pools.For(id)
			if m == nil {
				return nil, fmt.Errorf("tenant %s has no ClickHouse pool", id)
			}
			rows, err := m.Query(ctx, partsQuery)
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
		})
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
// from scrape on every collection. They are observable up-down counters, not
// gauges, so prometheus.per_table_metrics: false sums them over every table
// (observability's tableView). A failed scrape observes nothing, so the
// series go absent for the collection rather than stale or partial. It is
// logged when the scrape starts failing, not on every collection after.
func registerStorageMetrics(meter metric.Meter, scrape func(context.Context) (map[string]tableBytes, error)) (metric.Registration, error) {
	var failing atomic.Bool
	var errs [3]error
	var uncompressed, onDisk metric.Int64ObservableUpDownCounter
	uncompressed, errs[0] = meter.Int64ObservableUpDownCounter("wavehouse_clickhouse_uncompressed_bytes",
		metric.WithDescription("Uncompressed bytes of the live data in each table (active parts in system.parts): the closest analogue to bytes ingested"))
	onDisk, errs[1] = meter.Int64ObservableUpDownCounter("wavehouse_clickhouse_bytes_on_disk",
		metric.WithDescription("Bytes each table occupies on disk (active parts in system.parts): compressed data plus marks and index files"))
	var reg metric.Registration
	reg, errs[2] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		tables, err := scrape(ctx)
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
