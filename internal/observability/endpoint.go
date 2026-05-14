package observability

import (
	"crypto/tls"
	"fmt"
	"strings"
)

// tlsConfigOrDefault returns the supplied config when non-nil. The OTel SDK's
// credentials.NewTLS expects a non-nil *tls.Config; passing nil panics. An
// empty &tls.Config{} delegates to system defaults (system root CAs, ALPN
// negotiation), which is what production wants for cloud OTLP endpoints.
func tlsConfigOrDefault(c *tls.Config) *tls.Config {
	if c != nil {
		return c
	}
	return &tls.Config{}
}

// ParseEndpoint splits an OTLP endpoint string into the gRPC dial host and a
// useTLS flag. The OpenTelemetry SDK env-var convention is honored: an
// `https://` prefix selects TLS, while `http://` or a bare `host:port` stays
// plaintext (backward-compat with the prior WithInsecure() default).
//
// A URL path component is tolerated and stripped — gRPC routes by service name
// and ignores the path, so `https://otlp-gateway.example.com/otlp` and
// `https://otlp-gateway.example.com` dial the same way.
func ParseEndpoint(addr string) (host string, useTLS bool) {
	switch {
	case strings.HasPrefix(addr, "https://"):
		useTLS = true
		host = strings.TrimPrefix(addr, "https://")
	case strings.HasPrefix(addr, "http://"):
		host = strings.TrimPrefix(addr, "http://")
	default:
		host = addr
	}
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	return host, useTLS
}

// ParseOTelHeaders parses the OpenTelemetry-spec headers env-var format
// (`OTEL_EXPORTER_OTLP_HEADERS`) — comma-separated `key=value` pairs — into a
// map. Whitespace around the key and value is trimmed. Only the first `=` per
// segment splits key from value, so base64 trailing `=` in an Authorization
// header round-trips unchanged. An empty input yields an empty map.
//
// Returns an error (rather than silently dropping the segment) for malformed
// entries so Validate() can fail loud at boot rather than letting a typo
// silently disable auth in production.
//
// MUST stay in sync with config.validateOTelHeaders in
// internal/config/config.go. config can't import observability without
// transitively pulling the OTel SDK into every config consumer, so the
// parsing rules are hand-mirrored. Any rule change here needs the same
// change there, and vice versa.
func ParseOTelHeaders(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	for _, seg := range strings.Split(s, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		i := strings.IndexByte(seg, '=')
		if i < 0 {
			return nil, fmt.Errorf("header segment %q missing '='", seg)
		}
		key := strings.TrimSpace(seg[:i])
		val := strings.TrimSpace(seg[i+1:])
		if key == "" {
			return nil, fmt.Errorf("header segment %q has empty key", seg)
		}
		out[key] = val
	}
	return out, nil
}
