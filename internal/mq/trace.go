package mq

import (
	"context"
	"sync/atomic"

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
// returned context, messages of them, so a request of many rows is one span
// in its trace rather than one per row, which could crowd the request's own
// spans out of the exporter's queue (#897). end ends it with the error the
// batch stopped at, if any.
func StartPublishBatch(ctx context.Context, messages int) (context.Context, func(err error)) {
	batch := &publishBatch{}
	spanCtx, span := startPublishSpan(ctx, publishKindIngest, semconv.MessagingBatchMessageCount(messages))
	return context.WithValue(spanCtx, publishBatchKey{}, batch), func(err error) {
		endPublishSpan(ctx, span, int(batch.attempts.Load()), err)
	}
}

// beginPublish opens the producer span of one publish, retries included, so
// the headers the publish injects carry it and the worker's flush links to it
// rather than to the request's span (#897). An ingest publish under
// StartPublishBatch opens none: it rides on the batch's span and adds its
// attempts to it. end takes the attempts the publish made and its error.
func beginPublish(ctx context.Context, kind string) (context.Context, func(attempts int, err error)) {
	if batch, ok := ctx.Value(publishBatchKey{}).(*publishBatch); ok && kind == publishKindIngest {
		return ctx, func(attempts int, _ error) { batch.attempts.Add(int64(attempts)) }
	}
	spanCtx, span := startPublishSpan(ctx, kind)
	return spanCtx, func(attempts int, err error) { endPublishSpan(ctx, span, attempts, err) }
}

// startPublishSpan opens an mq.publish span of kind. The embedded broker is
// NATS too.
func startPublishSpan(ctx context.Context, kind string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(attrs,
		semconv.MessagingSystemKey.String("nats"),
		semconv.MessagingOperationPublish,
		attribute.String("kind", kind),
	)
	return otel.GetTracerProvider().Tracer("wavehouse-mq").Start(ctx, "mq.publish",
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
}

// endPublishSpan ends span with the attempts the publish made and its failure,
// if any. One whose ctx was canceled, the caller gone, is not a failure.
func endPublishSpan(ctx context.Context, span trace.Span, attempts int, err error) {
	span.SetAttributes(attribute.Int("attempts", attempts))
	if err != nil && ctx.Err() == nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
