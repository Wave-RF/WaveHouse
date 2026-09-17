package query

import (
	"bytes"
	"fmt"
)

// SafeEncodeToken converts any table or scope name into a single dot-free
// token, for composing the cache's dotted namespace keys. It preserves
// alphanumerics and underscores, but percent-encodes everything else.
func SafeEncodeToken(raw string) string {
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
