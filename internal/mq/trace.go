package mq

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Kinds of publish, the "kind" attribute on an mq.publish span.
const (
	publishKindIngest = "ingest" // a row onto its tenant's ingest queue
	publishKindDLQ    = "dlq"    // a row parked on the dead-letter stream
)

// publishBatchKey is the context key of the publishBatch StartPublishBatch
// opened.
type publishBatchKey struct{}

// publishBatch is the attempts the Publish calls of one batch made, summed
// onto its span when it ends.
type publishBatch struct{ attempts atomic.Int64 }

// StartPublishBatch opens one mq.publish span for every Publish made under the
// returned context, so a request of many rows is one span in its trace rather
// than one per row, which could crowd the request's own spans out of the
// exporter's queue (#897). end ends it with the messages handed to Publish,
// the one the batch stopped at included, and that error, if any: a batch cut
// short by a failure counts only the messages it reached.
func StartPublishBatch(ctx context.Context) (context.Context, func(messages int, err error)) {
	batch := &publishBatch{}
	spanCtx, span := otel.Tracer(tracerName).Start(ctx, publishSpanName, publishSpanOptions(publishKindIngest)...)
	return context.WithValue(spanCtx, publishBatchKey{}, batch), func(messages int, err error) {
		span.SetAttributes(semconv.MessagingBatchMessageCount(messages))
		setPublishResult(ctx, span, int(batch.attempts.Load()), err)
		span.End()
	}
}

// beginPublish opens the producer span of one publish, retries included, so
// the headers the publish injects carry it and the worker's flush links to it
// rather than to the request's span (#897). An ingest publish under
// StartPublishBatch opens none: it rides on the batch's span and adds its
// attempts to it. end takes the attempts the publish made and its error, and
// records the publish's duration either way (recordPublish).
func beginPublish(ctx context.Context, kind string) (context.Context, func(attempts int, err error)) {
	start := time.Now()
	if batch, ok := ctx.Value(publishBatchKey{}).(*publishBatch); ok && kind == publishKindIngest {
		return ctx, func(attempts int, err error) {
			batch.attempts.Add(int64(attempts))
			recordPublish(ctx, kind, start, err)
		}
	}
	spanCtx, span := otel.Tracer(tracerName).Start(ctx, publishSpanName, publishSpanOptions(kind)...)
	return spanCtx, func(attempts int, err error) {
		setPublishResult(ctx, span, attempts, err)
		span.End()
		recordPublish(ctx, kind, start, err)
	}
}

// The name and tracer of a publish's span, described by publishSpanOptions.
const (
	publishSpanName = "mq.publish"
	tracerName      = "wavehouse-mq"
)

// publishSpanOptions describes an mq.publish span of kind. The embedded
// broker is NATS too.
func publishSpanOptions(kind string, attrs ...attribute.KeyValue) []trace.SpanStartOption {
	attrs = append(attrs,
		semconv.MessagingSystemKey.String("nats"),
		semconv.MessagingOperationPublish,
		attribute.String("kind", kind),
	)
	return []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...)}
}

// setPublishResult sets on span the attempts the publish made and its
// failure, if any. One whose ctx was canceled, the caller gone, is not a
// failure.
func setPublishResult(ctx context.Context, span trace.Span, attempts int, err error) {
	span.SetAttributes(attribute.Int("attempts", attempts))
	if err != nil && ctx.Err() == nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
