package api

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

func prefixes(t *testing.T, cidrs ...string) trustedProxies {
	t.Helper()
	out := make(trustedProxies, 0, len(cidrs))
	for _, c := range cidrs {
		out = append(out, netip.MustParsePrefix(c))
	}
	return out
}

func TestTrustedProxies_ClientAddr(t *testing.T) {
	t.Parallel()
	proxies := prefixes(t, "10.0.0.0/8", "172.16.0.0/12", "fd00::/8")
	tests := []struct {
		name    string
		trusted trustedProxies
		remote  string
		xff     []string // every X-Forwarded-For line sent, in order
		want    string   // "" = no address
	}{
		{name: "no proxies trusted: the peer, headers ignored", remote: "203.0.113.9:4000", xff: []string{"198.51.100.1"}, want: "203.0.113.9"},
		{name: "untrusted peer's header ignored", trusted: proxies, remote: "203.0.113.9:4000", xff: []string{"198.51.100.1, 10.0.0.1"}, want: "203.0.113.9"},
		{name: "trusted peer, no header: the peer", trusted: proxies, remote: "10.0.0.1:4000", want: "10.0.0.1"},
		{name: "trusted peer, empty header: the peer", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{""}, want: "10.0.0.1"},
		{name: "one proxy", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"198.51.100.1"}, want: "198.51.100.1"},
		{name: "spoofed leftmost entry is skipped", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"1.2.3.4, 198.51.100.1"}, want: "198.51.100.1"},
		{name: "spoofed trusted-looking entry left of the client", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"10.9.9.9, 198.51.100.1"}, want: "198.51.100.1"},
		{name: "chained proxies", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"1.2.3.4, 198.51.100.1, 172.16.5.5, 10.2.2.2"}, want: "198.51.100.1"},
		{name: "chained proxies across header lines", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"1.2.3.4, 198.51.100.1", "172.16.5.5", "10.2.2.2"}, want: "198.51.100.1"},
		{name: "every hop trusted: the leftmost", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"10.3.3.3, 10.2.2.2"}, want: "10.3.3.3"},
		{name: "empty elements skipped", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"198.51.100.1, , 10.2.2.2,"}, want: "198.51.100.1"},
		{name: "unreadable hop stops at the nearest proxy", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"198.51.100.1, unknown, 10.2.2.2"}, want: "10.2.2.2"},
		{name: "unreadable last hop: the peer", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"198.51.100.1, _hidden"}, want: "10.0.0.1"},
		{name: "hop with a port", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"198.51.100.1:5555"}, want: "198.51.100.1"},
		{name: "IPv6 peer and client", trusted: proxies, remote: "[fd00::1]:4000", xff: []string{"2001:db8::7"}, want: "2001:db8::7"},
		{name: "bracketed IPv6 hop", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"[2001:db8::7]"}, want: "2001:db8::7"},
		{name: "bracketed IPv6 hop with a port", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"[2001:db8::7]:443"}, want: "2001:db8::7"},
		{name: "IPv6 hop inside a trusted prefix is a proxy", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"2001:db8::7, fd00::9"}, want: "2001:db8::7"},
		{name: "IPv4-mapped peer matches an IPv4 prefix", trusted: proxies, remote: "[::ffff:10.0.0.1]:4000", xff: []string{"198.51.100.1"}, want: "198.51.100.1"},
		{name: "IPv4-mapped hop reads as IPv4", trusted: proxies, remote: "10.0.0.1:4000", xff: []string{"::ffff:198.51.100.1"}, want: "198.51.100.1"},
		{name: "zoned peer", trusted: proxies, remote: "[fd00::1%eth0]:4000", xff: []string{"198.51.100.1"}, want: "198.51.100.1"},
		{name: "peer without a port", trusted: proxies, remote: "10.0.0.1", xff: []string{"198.51.100.1"}, want: "198.51.100.1"},
		{name: "peer not an address", trusted: proxies, remote: "@", xff: []string{"198.51.100.1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remote
			for _, v := range tt.xff {
				req.Header.Add(forwardedForHeader, v)
			}
			got := tt.trusted.clientAddr(req)
			if tt.want == "" {
				assert.False(t, got.IsValid(), got.String())
				return
			}
			assert.Equal(t, tt.want, got.String())
		})
	}
}

func TestTrustedProxies_TrustsPeer(t *testing.T) {
	t.Parallel()
	proxies := prefixes(t, "10.0.0.0/8")
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.RemoteAddr = "10.1.2.3:80"
	assert.True(t, proxies.trustsPeer(req))
	assert.False(t, trustedProxies(nil).trustsPeer(req))
	req.RemoteAddr = "203.0.113.9:80"
	assert.False(t, proxies.trustsPeer(req))
	req.RemoteAddr = "garbage"
	assert.False(t, proxies.trustsPeer(req))
}

// The router stamps the resolved client on the server span, replacing the
// leftmost X-Forwarded-For entry otelhttp would record, and joins an incoming
// traceparent only from a trusted proxy; from anyone else it is a link.
// Not parallel: otelhttp reads the global tracer provider and propagator.
func TestNewOpsRouter_ClientAddressAndTraceContinuation(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	savedTP, savedProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(savedTP)
		otel.SetTextMapPropagator(savedProp)
	})

	const parentTrace, parentSpan = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	r := NewOpsRouter(OpsDependencies{
		Health:         &HealthHandler{},
		Version:        NewVersionHandler("test", "test", "test"),
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	tests := []struct {
		name       string
		remote     string
		wantClient string
		wantJoined bool
	}{
		{name: "trusted proxy", remote: "10.0.0.1:4000", wantClient: "198.51.100.1", wantJoined: true},
		{name: "direct client", remote: "203.0.113.9:4000", wantClient: "203.0.113.9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec.Reset()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/version", nil)
			req.RemoteAddr = tt.remote
			req.Header.Set(forwardedForHeader, "1.2.3.4, 198.51.100.1")
			req.Header.Set("traceparent", "00-"+parentTrace+"-"+parentSpan+"-01")
			r.ServeHTTP(httptest.NewRecorder(), req)

			ended := rec.Ended()
			require.Len(t, ended, 1)
			span := ended[0]
			var clients []attribute.Value
			for _, kv := range span.Attributes() {
				if kv.Key == observability.ClientAddressKey {
					clients = append(clients, kv.Value)
				}
			}
			assert.Equal(t, []attribute.Value{attribute.StringValue(tt.wantClient)}, clients, "one client.address, the resolved one")

			parent, _ := trace.TraceIDFromHex(parentTrace)
			if tt.wantJoined {
				assert.Equal(t, parent, span.SpanContext().TraceID())
				assert.Equal(t, parentSpan, span.Parent().SpanID().String())
				assert.Empty(t, span.Links())
				return
			}
			assert.NotEqual(t, parent, span.SpanContext().TraceID(), "a new root")
			assert.False(t, span.Parent().IsValid())
			require.Len(t, span.Links(), 1)
			assert.Equal(t, parent, span.Links()[0].SpanContext.TraceID())
		})
	}
}
