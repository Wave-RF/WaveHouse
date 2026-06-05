package observability

import (
	"fmt"
	"strings"
)

// schemeURL normalizes an otel.addr into a URL the OTLP exporters'
// WithEndpointURL accepts, so the SDK — not us — owns scheme→TLS selection and
// URL-path stripping. A bare `host:port` (the backward-compatible default) has
// no scheme for url.Parse to key on, so it gets an `http://` prefix → plaintext
// gRPC, matching the prior WithInsecure() default. A value that already carries
// an `https://` or `http://` scheme passes through unchanged; the SDK then
// selects TLS from `https://`, leaves anything else plaintext, and strips any
// URL path (gRPC routes by service name). See provider.go for the call sites.
func schemeURL(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
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
