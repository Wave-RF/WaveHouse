package mq

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestPublishResult_EveryOutcome: each way a publish ends is recorded under
// its result, for an ingest publish inside a batch and a dead-letter park
// alike. Not parallel: it swaps the package's histogram.
func TestPublishResult_EveryOutcome(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	restore, err := UsePublishMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	require.NoError(t, err)
	t.Cleanup(restore)

	batchCtx, endBatch := StartPublishBatch(context.Background())
	for _, err := range []error{
		nil,
		fmt.Errorf("%w: maximum bytes exceeded", ErrQueueFull),
		fmt.Errorf("%w: no answer", ErrUnavailable),
		errors.New("nats: invalid subject"),
	} {
		_, end := beginPublish(batchCtx, publishKindIngest)
		end(1, err)
	}
	endBatch(4, nil)
	_, end := beginPublish(context.Background(), publishKindDLQ)
	end(2, fmt.Errorf("%w: no answer", ErrUnavailable))
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	_, end = beginPublish(gone, publishKindDLQ)
	end(1, fmt.Errorf("publish: %w", context.Canceled))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			for _, dp := range md.Data.(metricdata.Histogram[float64]).DataPoints {
				kind, _ := dp.Attributes.Value(attribute.Key("kind"))
				result, _ := dp.Attributes.Value(attribute.Key("result"))
				got[kind.AsString()+"/"+result.AsString()] += dp.Count
			}
		}
	}
	assert.Equal(t, map[string]uint64{
		"ingest/ok": 1, "ingest/queue_full": 1, "ingest/unavailable": 1, "ingest/error": 1,
		"dlq/unavailable": 1, "dlq/canceled": 1,
	}, got)
}
