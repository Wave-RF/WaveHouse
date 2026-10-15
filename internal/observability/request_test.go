package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// logLines runs log against a TraceHandler over a JSON handler and returns
// each record decoded.
func logLines(t *testing.T, log func(*slog.Logger)) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	log(slog.New(&TraceHandler{Handler: slog.NewJSONHandler(&buf, nil)}))
	var lines []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		lines = append(lines, rec)
	}
	return lines
}

// countKey counts the occurrences of key in a JSON line, which a decoded map
// would collapse.
func countKey(t *testing.T, log func(*slog.Logger), key string) int {
	t.Helper()
	var buf bytes.Buffer
	log(slog.New(&TraceHandler{Handler: slog.NewJSONHandler(&buf, nil)}))
	return strings.Count(buf.String(), `"`+key+`":`)
}

func TestTraceHandler_RequestScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ctx        func() context.Context
		attrs      []any
		wantID     any
		wantTenant any
	}{
		{
			name:       "outside a request",
			ctx:        context.Background,
			wantID:     nil,
			wantTenant: nil,
		},
		{
			name:       "before the tenant is resolved",
			ctx:        func() context.Context { return WithRequest(context.Background(), "req-1") },
			wantID:     "req-1",
			wantTenant: nil,
		},
		{
			name: "after the tenant is resolved",
			ctx: func() context.Context {
				ctx := WithRequest(context.Background(), "req-2")
				SetRequestTenant(ctx, "acme")
				return ctx
			},
			wantID:     "req-2",
			wantTenant: "acme",
		},
		{
			name: "a record naming its own tenant keeps it",
			ctx: func() context.Context {
				ctx := WithRequest(context.Background(), "req-3")
				SetRequestTenant(ctx, "acme")
				return ctx
			},
			attrs:      []any{TenantKey, "globex"},
			wantID:     "req-3",
			wantTenant: "globex",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lines := logLines(t, func(l *slog.Logger) { l.InfoContext(tt.ctx(), "hello", tt.attrs...) })
			require.Len(t, lines, 1)
			assert.Equal(t, tt.wantID, lines[0][RequestIDKey])
			assert.Equal(t, tt.wantTenant, lines[0][TenantKey])
		})
	}
}

// The tenant is resolved after the context an outer middleware logs with was
// taken; the record still carries it.
func TestTraceHandler_TenantSetAfterTheContextWasTaken(t *testing.T) {
	t.Parallel()
	outer := WithRequest(context.Background(), "req-1")
	inner, cancel := context.WithCancel(outer)
	defer cancel()
	SetRequestTenant(inner, "acme")

	lines := logLines(t, func(l *slog.Logger) { l.ErrorContext(outer, "panic recovered") })
	require.Len(t, lines, 1)
	assert.Equal(t, "acme", lines[0][TenantKey])
}

func TestTraceHandler_NoDuplicateTenant(t *testing.T) {
	t.Parallel()
	ctx := WithRequest(context.Background(), "req-1")
	SetRequestTenant(ctx, "acme")
	n := countKey(t, func(l *slog.Logger) { l.InfoContext(ctx, "hello", TenantKey, "acme", "table", "clicks") }, TenantKey)
	assert.Equal(t, 1, n)
}

func TestRequestID(t *testing.T) {
	t.Parallel()
	assert.Empty(t, RequestID(context.Background()))
	assert.Equal(t, "req-1", RequestID(WithRequest(context.Background(), "req-1")))
}

func TestAnnotateSpan(t *testing.T) {
	t.Parallel()
	rec := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

	// No scope: the span is left alone.
	ctx, span := tracer.Start(context.Background(), "bare")
	AnnotateSpan(ctx)
	span.End()

	ctx, span = tracer.Start(WithRequest(context.Background(), "req-1"), "server")
	AnnotateSpan(ctx)
	SetRequestTenant(ctx, "acme")
	span.End()

	ended := rec.Ended()
	require.Len(t, ended, 2)
	assert.Empty(t, ended[0].Attributes())
	assert.ElementsMatch(t, []attribute.KeyValue{
		attribute.String(RequestIDKey, "req-1"),
		attribute.String(TenantKey, "acme"),
	}, ended[1].Attributes())
}

// A tenant resolved before the span starts reaches it through AnnotateSpan.
func TestAnnotateSpan_TenantAlreadyResolved(t *testing.T) {
	t.Parallel()
	rec := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

	ctx := WithRequest(context.Background(), "req-1")
	SetRequestTenant(ctx, "acme")
	ctx, span := tracer.Start(ctx, "server")
	AnnotateSpan(ctx)
	span.End()

	require.Len(t, rec.Ended(), 1)
	assert.Contains(t, rec.Ended()[0].Attributes(), attribute.String(TenantKey, "acme"))
}

func TestTraceHandler_ClientAddress(t *testing.T) {
	t.Parallel()
	bare := logLines(t, func(l *slog.Logger) { l.InfoContext(WithRequest(context.Background(), "req-1"), "hello") })
	require.Len(t, bare, 1)
	assert.NotContains(t, bare[0], ClientAddressKey, "no address recorded, none logged")

	ctx := WithRequest(context.Background(), "req-1")
	SetRequestClient(ctx, "198.51.100.1")
	lines := logLines(t, func(l *slog.Logger) { l.InfoContext(ctx, "hello") })
	require.Len(t, lines, 1)
	assert.Equal(t, "198.51.100.1", lines[0][ClientAddressKey])
}

// AnnotateSpan replaces a client.address the span already carries (otelhttp's,
// read from X-Forwarded-For) with the request scope's.
func TestAnnotateSpan_ReplacesClientAddress(t *testing.T) {
	t.Parallel()
	rec := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

	scoped := WithRequest(context.Background(), "req-1")
	SetRequestClient(scoped, "198.51.100.1")
	ctx, span := tracer.Start(scoped, "server", trace.WithAttributes(attribute.String(ClientAddressKey, "1.2.3.4")))
	AnnotateSpan(ctx)
	span.End()

	require.Len(t, rec.Ended(), 1)
	assert.ElementsMatch(t, []attribute.KeyValue{
		attribute.String(RequestIDKey, "req-1"),
		attribute.String(ClientAddressKey, "198.51.100.1"),
	}, rec.Ended()[0].Attributes())
}
