package observability

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
)

// natsCarrier adapts nats.Header to satisfy the TextMapCarrier interface
type natsCarrier nats.Header

// These are methods for retrieving and storing values from and to NATS header.
func (c natsCarrier) Get(key string) string {
	return nats.Header(c).Get(key)
}

func (c natsCarrier) Set(key string, value string) {
	nats.Header(c).Set(key, value)
}

// Gives us back header (for OTEL debugging).
func (c natsCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

type HeaderHolder interface {
	Headers() nats.Header
}

// InjectNATS injects the propagation context into the NATS message header.
// Use this in the API/Producer before publishing.
func InjectNATS(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	// Take the Trace ID from the contets and pack it into the NATS Headers so the worker can find it later.
	otel.GetTextMapPropagator().Inject(ctx, natsCarrier(msg.Header))
}

// ExtractNATS extracts the propagation context from the NATS message header.
// Use this in the Worker/Subscriber when a message is received.
func ExtractNATS(ctx context.Context, msg HeaderHolder) context.Context {
	headers := msg.Headers()
	if headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, natsCarrier(headers))
}
