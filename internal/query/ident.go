package query

import "github.com/Wave-RF/WaveHouse/internal/keyenc"

// SafeEncodeToken renders a table or scope name as one dot-free token of the
// cache's namespace keys: keyenc's escaping, the same bytes a NATS subject
// carries for the name.
func SafeEncodeToken(raw string) string { return keyenc.Escape(raw) }
