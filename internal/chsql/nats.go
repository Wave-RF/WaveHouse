package chsql

import (
	"bytes"
	"fmt"
	"net/url"
)

// SafeEncodeNATS converts any ClickHouse table name into a safe, single NATS token.
// It preserves alphanumerics and underscores, but percent-encodes everything else.
// It is the single encoder shared by every side of cache invalidation — the ingest
// worker (write side), the pipe/structured-query handlers (read side), and the
// schema registry's dependency cascade — so all three build identical namespace
// keys for the same table.
func SafeEncodeNATS(raw string) string {
	var buf bytes.Buffer
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		// Pass through safe characters: a-z, A-Z, 0-9, and _ (underscore)
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' {
			buf.WriteByte(b)
		} else {
			// Hex encode everything else (e.g., '.' becomes '%2E', ' ' becomes '%20')
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

// SafeDecodeNATS reverses the NATS-safe encoding back to the raw ClickHouse table name.
func SafeDecodeNATS(safe string) (string, error) {
	// Re-use Go's standard library to unescape the percent-encoding
	// url.PathUnescape perfectly handles the %XX format we generated above
	return url.PathUnescape(safe)
}
