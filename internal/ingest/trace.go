package ingest

import (
	"context"
	"net/url"
	"strconv"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope of the worker's spans.
const tracerName = "wavehouse-ingest"

// maxFlushLinks bounds how many producer spans one flush span links to. A
// batch holds up to defaultMaxBatch rows and each may come from its own
// request; a span carrying thousands of links is more than any backend
// renders, so the first ones stand for the rest and `producers` counts them
// all.
const maxFlushLinks = 128

// startFlushSpan starts the span of one group's flush — one (tenant, table,
// column list) batch, written as one INSERT — in ctx, which the worker's flush
// context never carries a span of its own into. The rows were published by
// many requests, so the span is a root of its own, linked to each distinct
// producer span the rows' queue headers carried (the handler's span of the
// request that published the row, which internal/mq extracts onto the
// delivered message's context): a request's trace reaches the flush that
// wrote its rows, and a slow or failed insert reads back to every request it
// held. A row published outside a span links nothing.
func startFlushSpan(ctx context.Context, tableName string, group []parsedMsg) (context.Context, trace.Span) {
	type spanKey struct {
		trace trace.TraceID
		span  trace.SpanID
	}
	seen := make(map[spanKey]struct{}, min(len(group), maxFlushLinks))
	links := make([]trace.Link, 0, min(len(group), maxFlushLinks))
	for _, pm := range group {
		if pm.msg == nil {
			continue
		}
		sc := trace.SpanContextFromContext(pm.msg.Ctx)
		if !sc.IsValid() {
			continue
		}
		key := spanKey{trace: sc.TraceID(), span: sc.SpanID()}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if len(links) < maxFlushLinks {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, "IngestWorker.Flush",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links...),
		trace.WithAttributes(
			attribute.String(observability.TenantKey, string(group[0].tenant)),
			attribute.String("table", tableName),
			attribute.Int("rows", len(group)),
			attribute.Int("producers", len(seen)),
		),
	)
	return ctx, span
}

// startInsertSpan starts the client span of one INSERT into t, a child of the
// flush span in ctx, so the statement's own latency and failure stand apart
// from the batch's bookkeeping around it.
func startInsertSpan(ctx context.Context, t chconn.Target, tableName string, rows int) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		semconv.DBSystemKey.String("clickhouse"),
		semconv.DBName(t.Database),
		semconv.DBOperation("INSERT"),
		semconv.DBSQLTable(tableName),
		attribute.Int("rows", rows),
	}
	if u, err := url.Parse(t.URL); err == nil && u.Hostname() != "" {
		attrs = append(attrs, semconv.ServerAddress(u.Hostname()))
		if port, err := strconv.Atoi(u.Port()); err == nil {
			attrs = append(attrs, semconv.ServerPort(port))
		}
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, "clickhouse.insert",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	return ctx, span
}

// endSpan ends span, recording err as its failure when there is one.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
