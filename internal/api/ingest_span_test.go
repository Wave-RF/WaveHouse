package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// The ingest handler's span, the one whose context rides into the queue
// headers, is marked failed when the engine answers 5xx, described by the
// response's message, and left unset for a client's mistake (#899). Its scope
// is wavehouse-api.
// Not parallel: the handler reads the global tracer provider.
func TestIngest_SpanStatus(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		_ = tp.Shutdown(context.Background())
	})

	loaded := fixedRegistry(testRegistry(t))
	notLoaded := func(*settings.Store) *discovery.SchemaRegistry { return nil }
	one := map[string]any{"page": "/home"}
	batch := []map[string]any{{"page": "/a"}, {"page": "/b"}}
	tests := []struct {
		name     string
		registry RegistrySource
		pubErr   error
		table    string
		body     any
		status   int
		wantCode codes.Code
		wantDesc string
	}{
		{
			name: "queue full", registry: loaded, pubErr: fmt.Errorf("%w: maximum bytes exceeded", mq.ErrQueueFull), table: "clicks", body: one,
			status: http.StatusServiceUnavailable, wantCode: codes.Error, wantDesc: "service unavailable",
		},
		{
			name: "queue full, batch", registry: loaded, pubErr: fmt.Errorf("%w: maximum bytes exceeded", mq.ErrQueueFull), table: "clicks", body: batch,
			status: http.StatusServiceUnavailable, wantCode: codes.Error, wantDesc: "service unavailable",
		},
		{
			name: "queue unavailable", registry: loaded, pubErr: fmt.Errorf("%w: nats: timeout", mq.ErrUnavailable), table: "clicks", body: one,
			status: http.StatusServiceUnavailable, wantCode: codes.Error, wantDesc: "service unavailable",
		},
		{
			name: "publish failed", registry: loaded, pubErr: errors.New("some other error"), table: "clicks", body: one,
			status: http.StatusInternalServerError, wantCode: codes.Error, wantDesc: "publish failed",
		},
		{
			name: "schema not loaded", registry: notLoaded, table: "clicks", body: one,
			status: http.StatusServiceUnavailable, wantCode: codes.Error, wantDesc: schemaNotLoadedMessage,
		},
		{name: "accepted", registry: loaded, table: "clicks", body: one, status: http.StatusOK, wantCode: codes.Unset},
		{name: "missing table", registry: loaded, body: one, status: http.StatusBadRequest, wantCode: codes.Unset},
		{name: "unknown table", registry: loaded, table: "nope", body: one, status: http.StatusNotFound, wantCode: codes.Unset},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec.Reset()
			h := NewIngestHandler(tt.registry, &testutil.MockPublisher{Err: tt.pubErr})
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(ingestRequest(t, tt.table, tt.body)))
			require.Equal(t, tt.status, w.Code)

			spans := spansNamed(rec, "IngestHandler.Handle")
			require.Len(t, spans, 1)
			assert.Equal(t, "wavehouse-api", spans[0].InstrumentationScope().Name)
			assert.Equal(t, tt.wantCode, spans[0].Status().Code)
			assert.Equal(t, tt.wantDesc, spans[0].Status().Description)
		})
	}
}

// spansNamed are the ended spans rec holds named name.
func spansNamed(rec *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// A window's publishes are one mq.publish span under the handler's, sized by
// the rows it publishes, so a request of many rows costs a span per window
// rather than per row (#897); one whose publish fails is marked failed.
// Not parallel: the handler reads the global tracer provider.
func TestIngest_PublishSpanPerWindow(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		_ = tp.Shutdown(context.Background())
	})
	rows := []map[string]any{{"page": "/a"}, {"page": "/b"}, {"page": "/c"}}

	h := NewIngestHandler(fixedRegistry(testRegistry(t)), &testutil.MockPublisher{})
	h.window = 2
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", rows)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	handle := spansNamed(rec, "IngestHandler.Handle")
	require.Len(t, handle, 1)
	pubs := spansNamed(rec, "mq.publish")
	require.Len(t, pubs, 2)
	for i, want := range []int{2, 1} {
		assert.Equal(t, handle[0].SpanContext().SpanID(), pubs[i].Parent().SpanID())
		assert.Contains(t, pubs[i].Attributes(), attribute.Int("messaging.batch.message_count", want))
		assert.Equal(t, codes.Unset, pubs[i].Status().Code)
	}

	rec.Reset()
	h = NewIngestHandler(fixedRegistry(testRegistry(t)), &testutil.MockPublisher{Err: errors.New("boom")})
	h.Handle(httptest.NewRecorder(), withTenant(ingestRequest(t, "clicks", rows)))
	pubs = spansNamed(rec, "mq.publish")
	require.Len(t, pubs, 1)
	assert.Equal(t, codes.Error, pubs[0].Status().Code)
}
