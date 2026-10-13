package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// recordSpans installs a recording tracer provider as the global one for the
// test, the provider the worker's spans go through, and returns the recorder.
// A test that calls it mutates globals, so it must not run in parallel.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

// spanNamed returns the one recorded span called name.
func spanNamed(t *testing.T, rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() != name {
			continue
		}
		require.Nil(t, found, "more than one span named %q", name)
		found = s
	}
	require.NotNil(t, found, "no span named %q among %d", name, len(rec.Ended()))
	return found
}

func attr(s sdktrace.ReadOnlySpan, key string) attribute.Value {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	return attribute.Value{}
}

// producerContext is a remote span context as the queue hands one to the
// worker: valid, sampled, and distinct from every other call's.
func producerContext(n byte) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, n, 0x01},
		SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, n, 0x02},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

// tracedMsg is a parsed row whose message carries sc, as the consumer path
// delivers one published under a span; the zero sc is a row published
// outside any span.
func tracedMsg(sc trace.SpanContext) parsedMsg {
	msg := mq.NewMessage(trace.ContextWithSpanContext(context.Background(), sc),
		mq.Topic{Tenant: tenant.Default, Table: "events"}, []byte(`[1]`), time.Time{},
		func(context.Context) error { return nil }, func() error { return nil }, func() error { return nil })
	return parsedMsg{msg: msg, tenant: tenant.Default, tableName: "events", columns: []string{"id"}, row: []byte(`[1]`)}
}

func TestStartFlushSpan_LinksEachProducerOnce(t *testing.T) {
	rec := recordSpans(t)
	a, b := producerContext(1), producerContext(2)
	group := []parsedMsg{tracedMsg(a), tracedMsg(a), tracedMsg(b), tracedMsg(trace.SpanContext{}), {tenant: tenant.Default}}

	_, span := startFlushSpan(context.Background(), "events", group)
	span.End()

	got := spanNamed(t, rec, "IngestWorker.Flush")
	assert.Equal(t, trace.SpanKindConsumer, got.SpanKind())
	assert.False(t, got.Parent().IsValid(), "a flush is a root: its rows came from many requests")
	require.Len(t, got.Links(), 2, "one link per distinct producer span, none for a row published outside a span")
	assert.Equal(t, a.SpanID(), got.Links()[0].SpanContext.SpanID())
	assert.Equal(t, b.SpanID(), got.Links()[1].SpanContext.SpanID())
	assert.Equal(t, string(tenant.Default), attr(got, "tenant").AsString())
	assert.Equal(t, "events", attr(got, "table").AsString())
	assert.Equal(t, int64(5), attr(got, "rows").AsInt64())
	assert.Equal(t, int64(2), attr(got, "producers").AsInt64())
}

func TestStartFlushSpan_CapsTheLinksAndCountsEveryProducer(t *testing.T) {
	rec := recordSpans(t)
	group := make([]parsedMsg, 0, maxFlushLinks+50)
	for i := range maxFlushLinks + 50 {
		group = append(group, tracedMsg(producerContext(byte(i))))
	}

	_, span := startFlushSpan(context.Background(), "events", group)
	span.End()

	got := spanNamed(t, rec, "IngestWorker.Flush")
	assert.Len(t, got.Links(), maxFlushLinks)
	assert.Equal(t, int64(maxFlushLinks+50), attr(got, "producers").AsInt64())
}

func TestInsertToClickHouse_SpanIsTheFlushSpansChild(t *testing.T) {
	rec := recordSpans(t)
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, wait := newTestWorker(rt)
	m1 := newIngestMsg(t, "events", "", map[string]any{"id": 1})
	m2 := newIngestMsg(t, "events", "", map[string]any{"id": 2})

	w.flushTable(context.Background(), "events", parseAll(t, w, m1, m2))
	wait()

	flush := spanNamed(t, rec, "IngestWorker.Flush")
	assert.Equal(t, codes.Unset, flush.Status().Code)
	insert := spanNamed(t, rec, "clickhouse.insert")
	assert.Equal(t, trace.SpanKindClient, insert.SpanKind())
	assert.Equal(t, flush.SpanContext().SpanID(), insert.Parent().SpanID(), "the INSERT is a child of its flush")
	assert.Equal(t, flush.SpanContext().TraceID(), insert.SpanContext().TraceID())
	assert.Equal(t, "clickhouse", attr(insert, "db.system").AsString())
	assert.Equal(t, "test_db", attr(insert, "db.name").AsString())
	assert.Equal(t, "INSERT", attr(insert, "db.operation").AsString())
	assert.Equal(t, "events", attr(insert, "db.sql.table").AsString())
	assert.Equal(t, "test-clickhouse", attr(insert, "server.address").AsString())
	assert.Equal(t, int64(8123), attr(insert, "server.port").AsInt64())
	assert.Equal(t, int64(2), attr(insert, "rows").AsInt64())
	assert.Equal(t, codes.Unset, insert.Status().Code)
}

func TestInsertToClickHouse_SpanRecordsTheFailure(t *testing.T) {
	rec := recordSpans(t)
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewBufferString("boom"))}, nil
		},
	}
	w, _, _, wait := newTestWorker(rt)
	m1 := newIngestMsg(t, "events", "", map[string]any{"id": 1})

	w.flushTable(context.Background(), "events", parseAll(t, w, m1))
	wait()

	for _, name := range []string{"clickhouse.insert", "IngestWorker.Flush"} {
		s := spanNamed(t, rec, name)
		assert.Equal(t, codes.Error, s.Status().Code, name)
		assert.Contains(t, s.Status().Description, "HTTP 500", name)
		require.Len(t, s.Events(), 1, "%s records the error once", name)
		assert.Equal(t, "exception", s.Events()[0].Name, name)
	}
}

// TestIngestTraceContinuity_EndToEnd drives a row through the real queue: a
// publish under a request's span, the worker's consumer, its flush and its
// INSERT. The flush span links to the request's span, as the queue headers
// carried it, and the INSERT span is the flush span's child — the trace of
// an ingest request no longer ends at the publish.
func TestIngestTraceContinuity_EndToEnd(t *testing.T) {
	// No t.Parallel: the global propagator and tracer provider are swapped
	// for the test (the consumer path extracts with the global propagator).
	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prevProp) })
	rec := recordSpans(t)

	emb := testutil.NewEmbeddedMQ(t, 4*1024*1024)
	var inserts atomic.Int32
	chSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		inserts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(chSrv.Close)
	u, err := url.Parse(chSrv.URL)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cons, err := emb.CreateConsumer(ctx, mq.ConsumerConfig{Durable: BufferConsumerName, MaxAckPending: 1000})
	require.NoError(t, err)
	worker := &IngestWorker{
		dlq:     emb,
		clients: chconn.NewHTTPClients(ingestHTTPClient),
		cache:   &testutil.MockCache{},
		target: func(tenant.ID) chconn.Target {
			return chconn.Target{URL: fmt.Sprintf("http://%s:%s", host, port), Username: "u", Password: "p", Database: "db"}
		},
		maxBatch: defaultMaxBatch,
		maxWait:  200 * time.Millisecond,
	}
	worker.wg.Add(1)
	go worker.dispatchLoop(ctx, cons)
	t.Cleanup(func() {
		cancel()
		worker.wg.Wait()
	})

	// Two rows from one request and one from another, as the handler
	// publishes them: under the request's span.
	reqCtx1, req1 := otel.Tracer("test").Start(context.Background(), "IngestHandler.Handle")
	reqCtx2, req2 := otel.Tracer("test").Start(context.Background(), "IngestHandler.Handle")
	topic := mq.Topic{Tenant: tenant.Default, Table: "events"}
	require.NoError(t, emb.Publish(reqCtx1, topic, makeEnvelope(t, "events", "", map[string]any{"id": 1})))
	require.NoError(t, emb.Publish(reqCtx1, topic, makeEnvelope(t, "events", "", map[string]any{"id": 2})))
	require.NoError(t, emb.Publish(reqCtx2, topic, makeEnvelope(t, "events", "", map[string]any{"id": 3})))
	req1.End()
	req2.End()

	require.Eventually(t, func() bool { return inserts.Load() >= 1 }, 6*time.Second, 25*time.Millisecond, "the worker never inserted")
	require.Eventually(t, func() bool {
		n := 0
		for _, s := range rec.Ended() {
			if s.Name() == "clickhouse.insert" {
				n++
			}
		}
		return n >= 1
	}, 4*time.Second, 25*time.Millisecond, "the insert span never ended")

	// One batch, since all three rows arrived inside the flush window; a
	// loaded host may split them, so collect every flush span.
	var flushes []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "IngestWorker.Flush" {
			flushes = append(flushes, s)
		}
	}
	require.NotEmpty(t, flushes)
	linked := map[trace.SpanID]bool{}
	for _, f := range flushes {
		assert.False(t, f.Parent().IsValid(), "a flush span is a root")
		for _, l := range f.Links() {
			assert.True(t, l.SpanContext.IsRemote(), "the link is to a span another process may have ended")
			linked[l.SpanContext.SpanID()] = true
		}
	}
	assert.True(t, linked[req1.SpanContext().SpanID()], "the flush links to the first request's span")
	assert.True(t, linked[req2.SpanContext().SpanID()], "the flush links to the second request's span")
	assert.Len(t, linked, 2, "each request's span is linked once, however many rows it published")

	flushIDs := map[trace.SpanID]bool{}
	for _, f := range flushes {
		flushIDs[f.SpanContext().SpanID()] = true
	}
	inserted := 0
	for _, s := range rec.Ended() {
		if s.Name() != "clickhouse.insert" {
			continue
		}
		inserted++
		assert.True(t, flushIDs[s.Parent().SpanID()], "the INSERT span is a flush span's child")
	}
	assert.Equal(t, int(inserts.Load()), inserted, "one INSERT span per INSERT")
	// Each request's own trace is untouched by the worker: the link points at
	// it, and nothing the worker started shares its trace id.
	for _, f := range flushes {
		assert.NotEqual(t, req1.SpanContext().TraceID(), f.SpanContext().TraceID())
	}
}
