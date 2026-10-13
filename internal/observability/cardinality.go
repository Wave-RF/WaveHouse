package observability

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
)

// TableAttr is the metric attribute naming a ClickHouse table. Its values come
// from the operator's schema, which has no ceiling on table count (Key Design
// Decision #2), so the provider bounds it: see tableView.
const TableAttr attribute.Key = "table"

// TableWarnThreshold is how many distinct table values the metrics may carry
// before the provider logs, once, that per-table series can be turned off.
const TableWarnThreshold = 500

// tableView is the view every WaveHouse instrument passes through (the
// library ones, otelhttp and runtime, carry no table). aggregate drops the
// table attribute, so each metric reports one series per remaining label set
// summed over every table; otherwise the attribute is kept and a tableCounter
// watches how many distinct tables it has carried.
func tableView(aggregate bool, threshold int) metric.View {
	filter := attribute.NewDenyKeysFilter(TableAttr)
	if !aggregate {
		filter = (&tableCounter{threshold: threshold}).keep
	}
	return metric.NewView(metric.Instrument{Name: "wavehouse_*"}, metric.Stream{AttributeFilter: filter})
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
