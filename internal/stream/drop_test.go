package stream

import (
	"context"
	"sync"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// meterReader points the global meter provider at a manual reader until the
// test ends. NewMetrics binds the global provider, so its callers must not
// call t.Parallel.
func meterReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	saved := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(saved)
	})
	return reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

// droppedWhere totals wavehouse_sse_dropped_frames_total over the datapoints
// carrying every attribute in want.
func droppedWhere(rm metricdata.ResourceMetrics, want map[string]string) int64 {
	md, ok := collectByName(rm)["wavehouse_sse_dropped_frames_total"]
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range md.Data.(metricdata.Sum[int64]).DataPoints {
		match := true
		for k, v := range want {
			if got, ok := dp.Attributes.Value(attribute.Key(k)); !ok || got.AsString() != v {
				match = false
			}
		}
		if match {
			total += dp.Value
		}
	}
	return total
}

func TestSubscriber_BufferFullDropIsCountedAndSignaled(t *testing.T) {
	reader := meterReader(t)
	sub := newSubscriber(2, NewMetrics())
	sub.table = "clicks"
	frame := Frame{Kind: KindEvent, Data: []byte("data: x\n\n")}

	require.True(t, sub.Send(frame))
	select {
	case <-sub.Dropped():
		t.Fatal("no drop yet")
	default:
	}
	_, depth, peak, _ := sub.QueueStats()
	assert.Equal(t, 1, depth)
	assert.Equal(t, 1, peak)

	require.True(t, sub.Send(frame))
	require.False(t, sub.Send(frame))
	require.False(t, sub.Send(frame))

	select {
	case <-sub.Dropped():
	default:
		t.Fatal("a buffer_full drop signals the handler")
	}
	dropped, depth, peak, capacity := sub.QueueStats()
	assert.Equal(t, int64(2), dropped)
	assert.Equal(t, 2, depth)
	assert.Equal(t, 2, peak)
	assert.Equal(t, 2, capacity)

	<-sub.Frames()
	_, depth, peak, _ = sub.QueueStats()
	assert.Equal(t, 1, depth, "draining lowers the depth")
	assert.Equal(t, 2, peak, "but not the peak")

	rm := collect(t, reader)
	assert.Equal(t, int64(2), droppedWhere(rm, map[string]string{"table": "clicks", "kind": KindEvent, "reason": DropBufferFull}))
}

func TestSubscriber_CloseCountsQueuedAndLateFrames(t *testing.T) {
	reader := meterReader(t)
	sub := newSubscriber(4, NewMetrics())
	sub.table = "clicks"
	require.True(t, sub.Send(Frame{Kind: KindSchema}))
	require.True(t, sub.Send(Frame{Kind: KindEvent}))

	assert.Equal(t, 2, sub.Close(DropClientGone), "Close reports what was still queued")
	assert.False(t, sub.Send(Frame{Kind: KindKeepalive}), "a frame sent after Close is not queued")

	dropped, depth, _, _ := sub.QueueStats()
	assert.Zero(t, depth)
	assert.Zero(t, dropped, "frames lost to a closed stream are not buffer_full drops")
	select {
	case <-sub.Dropped():
		t.Fatal("only a buffer_full drop signals the slow-consumer log")
	default:
	}

	rm := collect(t, reader)
	for _, kind := range []string{KindSchema, KindEvent, KindKeepalive} {
		assert.Equal(t, int64(1), droppedWhere(rm, map[string]string{"table": "clicks", "kind": kind, "reason": DropClientGone}), kind)
	}
	assert.Zero(t, droppedWhere(rm, map[string]string{"reason": DropBufferFull}))
}

// Every frame offered to a subscriber that is closed mid-fan-out is counted
// once — delivered to nobody, so dropped under one reason or the other — and
// none is left stranded in the queue.
func TestSubscriber_CloseRacingSendsLosesNoCount(t *testing.T) {
	reader := meterReader(t)
	sub := newSubscriber(8, NewMetrics())

	const senders, each = 4, 500
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range senders {
		wg.Go(func() {
			<-start
			for range each {
				sub.Send(Frame{Kind: KindEvent})
			}
		})
	}
	close(start)
	sub.Close(DropStreamClosed)
	wg.Wait()

	_, depth, _, _ := sub.QueueStats()
	assert.Zero(t, depth, "nothing is left queued after Close")
	rm := collect(t, reader)
	assert.Equal(t, int64(senders*each), droppedWhere(rm, nil))
}

func TestHub_QueuePeakGauge(t *testing.T) {
	reader := meterReader(t)
	hub := NewHub(nil, nil, NewMetrics())
	peak := func() int64 {
		md, ok := collectByName(collect(t, reader))["wavehouse_sse_queue_peak_frames"]
		require.True(t, ok)
		dps := md.Data.(metricdata.Gauge[int64]).DataPoints
		require.Len(t, dps, 1)
		return dps[0].Value
	}
	assert.Zero(t, peak(), "no streams")

	topic := topicOf("clicks")
	busy, idle := newSubscriber(8, nil), newSubscriber(8, nil)
	hub.Add(topic, "viewer", busy)
	hub.Add(topic, "admin", idle)
	for range 3 {
		busy.Send(Frame{Kind: KindEvent})
	}
	<-busy.Frames()
	assert.Equal(t, int64(3), peak(), "the deepest queue, as deep as it has been")

	hub.Remove(topic, "viewer", busy)
	assert.Zero(t, peak(), "a closed stream no longer counts")
}

func TestHub_LabelTable(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{{Name: "n", Type: "Int64"}}},
	})
	hub := NewHub(nil, fixedRegistry(reg), nil)
	assert.Equal(t, "clicks", hub.LabelTable(tenant.Default, "clicks"))
	assert.Empty(t, hub.LabelTable(tenant.Default, "minted_by_a_client"), "a table the schema lacks is no label")
	assert.Empty(t, NewHub(nil, nil, nil).LabelTable(tenant.Default, "clicks"), "nor is any table without a registry")
}
