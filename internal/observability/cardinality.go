package observability

import (
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// TableAttr is the metric attribute naming a ClickHouse table. Its values come
// from the operator's schema, which has no ceiling on table count (Key Design
// Decision #2), so the provider bounds it: see metricsView.
const TableAttr attribute.Key = "table"

// TableWarnThreshold is how many distinct table values the metrics may carry
// before the provider logs, once, that per-table series can be turned off.
const TableWarnThreshold = 500

// TenantAttr is the metric attribute naming the tenant a measurement is
// about. Its values come from the settings directory, which has no ceiling
// on tenant count either, so a process labels at most TenantLimit tenants:
// see TenantLabel.
const TenantAttr attribute.Key = TenantKey

// viewOptions are the operator's choices the metrics view applies.
type viewOptions struct {
	// aggregateTables drops the table attribute (prometheus.per_table_metrics:
	// false); tableThreshold is when the process warns that it may.
	aggregateTables bool
	tableThreshold  int
	// aggregateTenants drops the tenant attribute
	// (prometheus.per_tenant_metrics: false).
	aggregateTenants bool
	// httpBodySize keeps otelhttp's request and response body size
	// histograms (prometheus.http_body_size_metrics), dropped by default.
	httpBodySize bool
}

// otelhttpScope is the instrumentation scope of otelhttp's instruments.
const otelhttpScope = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

// metricsView is the one view every instrument passes through (#891):
//   - on a WaveHouse instrument it bounds the table and tenant attributes:
//     each is dropped when the operator aggregates it, so a metric reports
//     one series per remaining label set summed over every table or tenant,
//     and otherwise kept, a tableCounter watching how many distinct tables
//     the metrics have carried (TenantLabel caps the tenants itself). Only
//     sum-like instruments may carry either: a gauge folded this way keeps
//     one value, not the total.
//   - on otelhttp's server instruments it drops server.address and
//     server.port, both read from the request's Host header: with a hostname
//     per tenant, server.address is a tenant label with no cap. The request
//     and response body size histograms, the largest families a replica
//     exports, are dropped unless the operator keeps them.
//
// Every other instrument (Go runtime, library ones) passes unchanged.
func metricsView(o viewOptions) metric.View {
	tables := (&tableCounter{threshold: o.tableThreshold}).keep
	wavehouse := func(kv attribute.KeyValue) bool {
		switch kv.Key {
		case TableAttr:
			return !o.aggregateTables && tables(kv)
		case TenantAttr:
			return !o.aggregateTenants
		}
		return true
	}
	httpServer := attribute.NewDenyKeysFilter(semconv.ServerAddressKey, semconv.ServerPortKey)
	return func(i metric.Instrument) (metric.Stream, bool) {
		s := metric.Stream{Name: i.Name, Description: i.Description, Unit: i.Unit}
		switch {
		case strings.HasPrefix(i.Name, "wavehouse_"):
			s.AttributeFilter = wavehouse
		case i.Scope.Name == otelhttpScope && strings.HasPrefix(i.Name, "http.server."):
			if !o.httpBodySize && strings.HasSuffix(i.Name, ".body.size") {
				s.Aggregation = metric.AggregationDrop{}
			}
			s.AttributeFilter = httpServer
		default:
			return metric.Stream{}, false
		}
		return s, true
	}
}

// tableCounter counts the distinct table values the view has seen and warns
// once past threshold. The SDK runs the filter on every measurement, so a
// table already seen costs one map read and, after the warning, one atomic
// load; the set is dropped then, so it never holds more than threshold names.
type tableCounter struct {
	threshold int
	warned    atomic.Bool
	n         atomic.Int64
	seen      sync.Map
}

func (c *tableCounter) keep(kv attribute.KeyValue) bool {
	if kv.Key != TableAttr || c.warned.Load() {
		return true
	}
	if _, loaded := c.seen.LoadOrStore(kv.Value.AsString(), struct{}{}); loaded {
		return true
	}
	if c.n.Add(1) > int64(c.threshold) && c.warned.CompareAndSwap(false, true) {
		c.seen.Clear()
		slog.Warn("metrics carry a table label for more distinct tables than the threshold; each one is a series per metric in your TSDB. Set prometheus.per_table_metrics: false (WH_METRICS_PER_TABLE=false) to report every table as one series",
			"threshold", c.threshold)
	}
	return true
}
