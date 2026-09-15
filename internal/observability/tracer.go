package observability

import (
	"context"

	"go.opentelemetry.io/otel"
)

// headerCarrier adapts a message header map to OpenTelemetry's TextMapCarrier.
// NATS and HTTP headers are both map[string][]string, so either converts to it
// without a copy. Lookups are exact-key (no canonicalization), matching
// nats.Header, so the propagator reads back the keys it wrote.
type headerCarrier map[string][]string

func (c headerCarrier) Get(key string) string {
	if v := c[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (c headerCarrier) Set(key string, value string) {
	c[key] = []string{value}
}

// Keys lists the header names (for OTel debugging).
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectHeaders writes the propagation context (trace + baggage) from ctx into
// the message headers h, which must be non-nil. The publisher calls this before
// handing a message to the broker so the consumer can pick the trace back up.
func InjectHeaders(ctx context.Context, h map[string][]string) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(h))
}

// ExtractHeaders returns ctx carrying the propagation context found in the
// message headers h. A nil h returns ctx unchanged. The subscriber calls this
// when a message is received.
func ExtractHeaders(ctx context.Context, h map[string][]string) context.Context {
	if h == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier(h))
}
