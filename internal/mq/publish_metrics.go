package mq

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// publishBuckets run from 1 ms to 20 s: each attempt is bounded by the
// topology's PublishTimeout (5 s by default) and a publish makes up to three,
// 250 ms apart, so 20 s covers the worst case.
var publishBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20}

// publishSeconds is where every publish's duration is recorded (#896): one
// value per Publish or DeadLetter, retries included, by kind and result. It
// is swapped only by UsePublishMeter.
var publishSeconds atomic.Pointer[metric.Float64Histogram]

func init() {
	h, _ := newPublishHistogram(otel.Meter("wavehouse-mq"))
	publishSeconds.Store(&h)
}

func newPublishHistogram(m metric.Meter) (metric.Float64Histogram, error) {
	return m.Float64Histogram(
		"wavehouse_mq_publish_duration_seconds",
		metric.WithDescription("Duration of each publish to the queue, retries included, by kind (ingest, or dlq for a row the worker parked) and result (ok, queue_full, unavailable, error, or canceled when the caller went away)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(publishBuckets...),
	)
}

// UsePublishMeter records publish durations on m until restore is called, so
// a test can read them from a meter of its own: the global provider binds
// only once per process.
func UsePublishMeter(m metric.Meter) (restore func(), err error) {
	h, err := newPublishHistogram(m)
	if err != nil {
		return nil, err
	}
	prev := publishSeconds.Swap(&h)
	return func() { publishSeconds.Store(prev) }, nil
}

// recordPublish records one publish of kind that began at start and ended
// with err.
func recordPublish(ctx context.Context, kind string, start time.Time, err error) {
	(*publishSeconds.Load()).Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("result", publishResult(ctx, err)),
	))
}

// publishResult is the result label of a publish that ended with err. One
// whose ctx was canceled, the caller gone, is canceled rather than a
// failure, as its span is not marked failed.
func publishResult(ctx context.Context, err error) string {
	switch {
	case err == nil:
		return "ok"
	case ctx.Err() != nil:
		return "canceled"
	case errors.Is(err, ErrQueueFull):
		return "queue_full"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	}
	return "error"
}
