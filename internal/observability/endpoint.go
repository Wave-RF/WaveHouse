package observability

import (
	"crypto/tls"
	"fmt"
	"strings"
)

// tlsConfigOrDefault returns the supplied config when non-nil. The OTel SDK's
// credentials.NewTLS expects a non-nil *tls.Config; passing nil panics. The
// production default delegates to system root CAs / ALPN negotiation but
// pins MinVersion to TLS 1.3 — every TLS-terminated cloud OTLP gateway in
// scope (Grafana Cloud, Honeycomb) supports it, and the floor matches OWASP
// modern guidance. Test callers supplying their own *tls.Config keep full
// control over the version range.
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
// (`OTEL_EXPORTER_OTLP_HEADERS`) — comma-separated `key=value` pairs — into a
// map. Whitespace around the key and value is trimmed. Only the first `=` per
// segment splits key from value, so base64 trailing `=` in an Authorization
// header round-trips unchanged. An empty input yields an empty map.
//
// Both empty keys and empty-or-whitespace-only values are rejected (e.g.
// `authorization=   ` would otherwise ship as `authorization: ""` to the
// cloud gateway and 401 silently). Keys are also validated against the
// RFC 7230 `token` production (the HTTP header-name grammar) so a typo like
// `my key=...` fails at boot rather than blowing up inside the gRPC stack
// on first export. gRPC normalizes ASCII case on the wire, so mixed-case
// keys (`Authorization`, `X-Honeycomb-Team`) are accepted.
//
// Returns an error (rather than silently dropping the segment) for malformed
// entries so Validate() can fail loud at boot rather than letting a typo
// silently disable auth in production.
//
// MUST stay in sync with config.validateOTelHeaders in
// internal/config/config.go. config can't import observability without
// transitively pulling the OTel SDK into every config consumer, so the
// parsing rules are hand-mirrored. The cross-parser parity test in
// internal/config/header_parity_test.go pins both parsers to the same
// accept/reject decisions, so a rule change here fails CI until the
// matching change lands there (and vice versa).
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
		if bad, ok := firstNonTokenChar(key); !ok {
			return nil, fmt.Errorf("header segment %q has invalid key character %q (RFC 7230 token: letters, digits, and %s)", seg, bad, headerNamePunctuation)
		}
		if val == "" {
			return nil, fmt.Errorf("header segment %q has empty or whitespace-only value", seg)
		}
		out[key] = val
	}
	return out, nil
}

// headerNamePunctuation lists the punctuation characters legal in an RFC 7230
// `token` (HTTP header field name). Surfaced in error messages so the
// reported "what's allowed" matches what the validator actually accepts.
const headerNamePunctuation = "!#$%&'*+-.^_`|~"

// firstNonTokenChar reports whether s consists entirely of RFC 7230 `token`
// characters (the HTTP header-name grammar): ALPHA / DIGIT / one of the
// punctuation chars in headerNamePunctuation. Returns the first offending
// rune and false on the first non-token char; returns (0, true) when s is
// fully valid.
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
