package observability

import (
	"context"
	"log/slog"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// The keys a request's correlation goes under, the same on a log record and
// on a span, so one query finds both.
const (
	RequestIDKey = "request_id"
	TenantKey    = "tenant"
	// ClientAddressKey is OpenTelemetry's semantic-convention name, which
	// the HTTP server span already uses for the same value.
	ClientAddressKey = "client.address"
)

// requestScope is what a request's log records and spans say about it: its
// id, known when it arrives, its client address, and its tenant, known only
// once a later middleware resolves it. Both are filled in place, so a record
// logged through a context taken before resolution (the panic recoverer's,
// say) still carries them.
type requestScope struct {
	id     string
	client atomic.Pointer[string]
	tenant atomic.Pointer[string]
}

type requestScopeKey struct{}

// WithRequest returns ctx carrying a request scope with the given id. Every
// record TraceHandler handles under it, or under a context derived from it,
// carries request_id, and tenant once SetRequestTenant has run.
func WithRequest(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestScopeKey{}, &requestScope{id: requestID})
}

func scopeFrom(ctx context.Context) *requestScope {
	s, _ := ctx.Value(requestScopeKey{}).(*requestScope)
	return s
}

// RequestID returns the id WithRequest stored, or "" outside a request.
func RequestID(ctx context.Context) string {
	if s := scopeFrom(ctx); s != nil {
		return s.id
	}
	return ""
}

// SetRequestTenant records the request's tenant on its scope and on the span
// in ctx. Outside a request it still annotates the span.
func SetRequestTenant(ctx context.Context, tenant string) {
	if s := scopeFrom(ctx); s != nil {
		s.tenant.Store(&tenant)
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String(TenantKey, tenant))
}

// SetRequestClient records the address the request came from on its scope
// and on the span in ctx. Outside a request it still annotates the span.
func SetRequestClient(ctx context.Context, addr string) {
	if s := scopeFrom(ctx); s != nil {
		s.client.Store(&addr)
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String(ClientAddressKey, addr))
}

// AnnotateSpan copies the request scope in ctx onto the span in ctx. Call it
// once the server span exists: the scope is set up before it. Its
// client.address replaces the one otelhttp derives, which reads
// X-Forwarded-For whoever sent it.
func AnnotateSpan(ctx context.Context) {
	s := scopeFrom(ctx)
	if s == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{attribute.String(RequestIDKey, s.id)}
	if c := s.client.Load(); c != nil {
		attrs = append(attrs, attribute.String(ClientAddressKey, *c))
	}
	if t := s.tenant.Load(); t != nil {
		attrs = append(attrs, attribute.String(TenantKey, *t))
	}
	span.SetAttributes(attrs...)
}

// addRequestAttrs stamps r with the request scope in ctx. A record that
// already names a tenant keeps its own: it may be about another one.
func addRequestAttrs(ctx context.Context, r *slog.Record) {
	s := scopeFrom(ctx)
	if s == nil {
		return
	}
	r.AddAttrs(slog.String(RequestIDKey, s.id))
	if c := s.client.Load(); c != nil {
		r.AddAttrs(slog.String(ClientAddressKey, *c))
	}
	t := s.tenant.Load()
	if t == nil {
		return
	}
	named := false
	r.Attrs(func(a slog.Attr) bool {
		named = a.Key == TenantKey
		return !named
	})
	if !named {
		r.AddAttrs(slog.String(TenantKey, *t))
	}
}
