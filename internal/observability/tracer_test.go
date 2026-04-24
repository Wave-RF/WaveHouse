package observability

import (
	"context"
	"sort"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	// InjectNATS / ExtractNATS rely on the global text map propagator. The
	// production pipeline installs one via InitProvider; tests don't go
	// through that path, so install a standard composite propagator here.
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

func TestNATSCarrier_SetGetKeys(t *testing.T) {
	t.Parallel()

	h := nats.Header{}
	c := natsCarrier(h)

	c.Set("foo", "bar")
	c.Set("baz", "qux")

	assert.Equal(t, "bar", c.Get("foo"))
	assert.Equal(t, "qux", c.Get("baz"))
	assert.Empty(t, c.Get("missing"))

	keys := c.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"baz", "foo"}, keys)
}

// fakeHeaderHolder implements HeaderHolder for testing ExtractNATS.
type fakeHeaderHolder struct{ h nats.Header }

func (f *fakeHeaderHolder) Headers() nats.Header { return f.h }

func TestInjectExtractNATS_Roundtrip(t *testing.T) {
	t.Parallel()

	// Build a context carrying a valid, sampled span so the W3C TraceContext
	// propagator actually writes a traceparent header. We bypass the global
	// tracer provider here — other tests (InitProvider) may have swapped it
	// for one with ratio-based sampling, which would make this flaky.
	tp := localTracerProvider()
	ctx, span := tp.Tracer("observability-test").Start(context.Background(), "publish")
	t.Cleanup(func() { span.End() })

	msg := nats.NewMsg("ingest.test")
	InjectNATS(ctx, msg)

	require.NotNil(t, msg.Header)
	require.NotEmpty(t, msg.Header, "inject should populate headers")

	extracted := ExtractNATS(context.Background(), &fakeHeaderHolder{h: msg.Header})
	sc := trace.SpanContextFromContext(extracted)
	require.True(t, sc.IsValid(), "extracted context must carry a valid span context")
	assert.Equal(t, span.SpanContext().TraceID(), sc.TraceID())
}

func TestInjectNATS_CreatesHeaderWhenNil(t *testing.T) {
	t.Parallel()

	msg := &nats.Msg{Subject: "ingest.nil"}
	require.Nil(t, msg.Header)

	InjectNATS(context.Background(), msg)
	// Even without a valid span, Inject should initialize the header map so
	// subsequent Set calls don't panic.
	assert.NotNil(t, msg.Header)
}

func TestExtractNATS_NilHeadersPassthrough(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	got := ExtractNATS(ctx, &fakeHeaderHolder{h: nil})
	// No headers → should return the context unchanged, not panic.
	assert.Equal(t, ctx, got)
}
