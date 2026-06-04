package observability

import (
	"crypto/tls"
	"fmt"
	"strings"
)

// tlsConfigOrDefault returns c, or a TLS 1.3 floor config when c is nil.
// credentials.NewTLS panics on a nil *tls.Config; the production default
// uses system roots with MinVersion=TLS1.3.
func tlsConfigOrDefault(c *tls.Config) *tls.Config {
	if c != nil {
		return c
	}
	return &tls.Config{MinVersion: tls.VersionTLS13}
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
// (comma-separated `key=value` pairs) into a map. Only the first `=` per
// segment splits key from value, so base64 trailing `=` round-trips. Keys
// are validated against RFC 7230 `token`; empty or whitespace-only values
// are rejected.
//
// This is the single validation point for WH_OTEL_HEADERS — cmd/wavehouse
// calls it at boot and treats a parse error as fatal. Validation deliberately
// lives here rather than in config.Validate() so internal/config stays free of
// the OpenTelemetry SDK import graph (it has zero OTel imports today).
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
		// Error messages quote only the key, never the full segment — `seg`
		// can contain a real API token that would be logged to every sink
		// the structured error field reaches.
		i := strings.IndexByte(seg, '=')
		if i < 0 {
			return nil, fmt.Errorf("header entry missing '=' separator (format is key=value)")
		}
		key := strings.TrimSpace(seg[:i])
		val := strings.TrimSpace(seg[i+1:])
		if key == "" {
			return nil, fmt.Errorf("header entry has empty key (format is key=value)")
		}
		if bad, ok := firstNonTokenChar(key); !ok {
			return nil, fmt.Errorf("header key %q has invalid character %q (RFC 7230 token: letters, digits, and %s)", key, bad, headerNamePunctuation)
		}
		if val == "" {
			return nil, fmt.Errorf("header key %q has empty or whitespace-only value", key)
		}
		// Reject duplicates rather than silently letting the last entry win
		// — in an auth-sensitive context, two `authorization=…` segments
		// would ship the wrong token with no boot-time indication.
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("header key %q appears more than once", key)
		}
		out[key] = val
	}
	return out, nil
}

// headerNamePunctuation lists the punctuation characters legal in an RFC 7230
// `token` (HTTP header field name).
const headerNamePunctuation = "!#$%&'*+-.^_`|~"

// firstNonTokenChar returns the first non-RFC-7230-token rune in s, or
// (0, true) when s is fully valid.
func firstNonTokenChar(s string) (rune, bool) {
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case strings.ContainsRune(headerNamePunctuation, c):
		default:
			return c, false
		}
	}
	return 0, true
}
