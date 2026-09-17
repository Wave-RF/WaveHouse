package observability

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// InjectHeaders / ExtractHeaders rely on the global text map propagator. The
// production pipeline installs one via InitProvider; tests don't go through
// that path, so install a standard composite propagator here.
//
// Safety: TestMain (or, here, package init) runs once before any test, so
// this single write doesn't race with parallel test reads. The companion
// risk — TestInitProvider_Shutdown overwriting the global mid-run — is
// neutralised by that test's save/restore in provider_test.go.
func init() {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
}

// localTracerProvider returns a fresh SDK tracer that always samples — used
// by tests that need recording spans regardless of whatever global provider
// InitProvider (tested elsewhere) may have installed.
func localTracerProvider() *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
}

func TestHeaderCarrier_SetGetKeys(t *testing.T) {
	t.Parallel()

	c := headerCarrier{}

	c.Set("foo", "bar")
	c.Set("baz", "qux")

	assert.Equal(t, "bar", c.Get("foo"))
	assert.Equal(t, "qux", c.Get("baz"))
	assert.Empty(t, c.Get("missing"))
	// Exact-key, like nats.Header: no canonicalization on either side.
	assert.Empty(t, c.Get("Foo"))

	keys := c.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"baz", "foo"}, keys)
}

func TestInjectExtractHeaders_Roundtrip(t *testing.T) {
	t.Parallel()

	// Build a context carrying a valid, sampled span so the W3C TraceContext
	// propagator actually writes a traceparent header. We bypass the global
	// tracer provider here — other tests (InitProvider) may have swapped it
	// for one with ratio-based sampling, which would make this flaky.
	tp := localTracerProvider()
	ctx, span := tp.Tracer("observability-test").Start(context.Background(), "publish")
	t.Cleanup(func() { span.End() })

	headers := map[string][]string{}
	InjectHeaders(ctx, headers)

	require.NotEmpty(t, headers, "inject should populate headers")

	extracted := ExtractHeaders(context.Background(), headers)
	sc := trace.SpanContextFromContext(extracted)
	require.True(t, sc.IsValid(), "extracted context must carry a valid span context")
	assert.Equal(t, span.SpanContext().TraceID(), sc.TraceID())
}

func TestInjectHeaders_NoSpanWritesNothing(t *testing.T) {
	t.Parallel()

	// Without a valid span or baggage the propagators have nothing to write,
	// so a message published outside a trace carries no propagation headers.
	headers := map[string][]string{}
	InjectHeaders(context.Background(), headers)
	assert.Empty(t, headers)
}

func TestExtractHeaders_NilHeadersPassthrough(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	got := ExtractHeaders(ctx, nil)
	// No headers → should return the context unchanged, not panic.
	assert.Equal(t, ctx, got)
}
