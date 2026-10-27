package mqtest

import (
	"context"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// publishIsTimed: every publish records one wavehouse_mq_publish_duration_seconds
// value under its kind and result (#896). It swaps the package's histogram, so
// Run runs it before the parallel cases, alone.
func publishIsTimed(t *testing.T, h Harness) {
	reader := sdkmetric.NewManualReader()
	restore, err := mq.UsePublishMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	require.NoError(t, err)
	t.Cleanup(restore)
	counts := func() map[string]uint64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		out := map[string]uint64{}
		for _, sm := range rm.ScopeMetrics {
			for _, md := range sm.Metrics {
				hist, ok := md.Data.(metricdata.Histogram[float64])
				if md.Name != "wavehouse_mq_publish_duration_seconds" || !ok {
					continue
				}
				for _, dp := range hist.DataPoints {
					kind, _ := dp.Attributes.Value(attribute.Key("kind"))
					result, _ := dp.Attributes.Value(attribute.Key("result"))
					out[kind.AsString()+"/"+result.AsString()] += dp.Count
				}
			}
		}
		return out
	}

	b := h.New(t)
	publish(t, b, mq.Topic{Tenant: Acme, Table: "timed"}, "x")
	publish(t, b, mq.Topic{Tenant: Acme, Table: "timed"}, "y")
	assert.Equal(t, map[string]uint64{"ingest/ok": 2}, counts())

	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, func(m *mq.Message) {
		assert.NoError(t, b.DeadLetter(m.Ctx, m))
		assert.NoError(t, m.DoubleAck(m.Ctx))
	})
	next(t, got, 2)
	assert.Equal(t, map[string]uint64{"ingest/ok": 2, "dlq/ok": 2}, counts())

	if h.Fill == nil {
		return
	}
	full := h.New(t)
	h.Fill(t, full, Acme)
	before := counts()
	require.ErrorIs(t, full.Publish(ctx(t), mq.Topic{Tenant: Acme, Table: "full"}, []byte("x")), mq.ErrQueueFull)
	assert.Equal(t, before["ingest/queue_full"]+1, counts()["ingest/queue_full"])
}
