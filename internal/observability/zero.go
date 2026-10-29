package observability

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Carries names the labels a counter records beyond its closed set that an
// operator setting can drop: table (prometheus.per_table_metrics) and tenant
// (prometheus.per_tenant_metrics).
type Carries uint32

const (
	// CarriesTable marks a counter recorded with TableAttr.
	CarriesTable Carries = 1 << iota
	// CarriesTenant marks a counter recorded through TenantLabel or
	// MetricAttrs.
	CarriesTenant
)

// labelsDropped is which of table and tenant the meter provider InitProvider
// last built drops; zero before it has built one.
var labelsDropped atomic.Uint32

// ZeroCounter adds 0 to c under each of sets, so each series is exported from
// the next collection on and an alert on increase() sees the first event
// rather than the series' appearance (#905). Call it once the counter is
// built, after InitProvider, since the global provider drops what is added
// before it is set.
//
// A set names only the counter's closed labels. A counter that also records
// table or tenant (carries) is zero-published only while every one of those
// is dropped: then the set is exactly the series the counter exports, where
// with the label kept a set without it would be a stray series no
// measurement ever adds to.
func ZeroCounter(c metric.Int64Counter, carries Carries, sets ...attribute.Set) {
	if c == nil || Carries(labelsDropped.Load())&carries != carries {
		return
	}
	if len(sets) == 0 {
		sets = []attribute.Set{*attribute.EmptySet()}
	}
	for _, s := range sets {
		c.Add(context.Background(), 0, metric.WithAttributeSet(s))
	}
}

// LabelSets is every combination of one value per dimension, each dimension
// one label's closed set of values: the sets ZeroCounter publishes.
func LabelSets(dims ...[]attribute.KeyValue) []attribute.Set {
	combos := [][]attribute.KeyValue{nil}
	for _, dim := range dims {
		next := make([][]attribute.KeyValue, 0, len(combos)*len(dim))
		for _, c := range combos {
			for _, kv := range dim {
				next = append(next, append(append([]attribute.KeyValue(nil), c...), kv))
			}
		}
		combos = next
	}
	sets := make([]attribute.Set, len(combos))
	for i, c := range combos {
		sets[i] = attribute.NewSet(c...)
	}
	return sets
}

// Values is one label's closed set of values, a LabelSets dimension.
func Values(key string, values ...string) []attribute.KeyValue {
	kvs := make([]attribute.KeyValue, len(values))
	for i, v := range values {
		kvs[i] = attribute.String(key, v)
	}
	return kvs
}
