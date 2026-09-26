package dedupe_test

import (
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/stretchr/testify/assert"
)

// The idempotency key is 32 hex characters, stable for one tenant, table and
// id, and different when any of the three differs — ids too long to store
// verbatim included.
func TestIdempotencyKey(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", dedupe.MaxIDBytes+1)
	base := dedupe.IdempotencyKey("acme", dedupe.Key{Table: "clicks", ID: "e1"})
	assert.Regexp(t, `^[0-9a-f]{32}$`, base)
	assert.Equal(t, base, dedupe.IdempotencyKey("acme", dedupe.Key{Table: "clicks", ID: "e1"}))

	others := []string{
		dedupe.IdempotencyKey("globex", dedupe.Key{Table: "clicks", ID: "e1"}),
		dedupe.IdempotencyKey("acme", dedupe.Key{Table: "views", ID: "e1"}),
		dedupe.IdempotencyKey("acme", dedupe.Key{Table: "clicks", ID: "e2"}),
		dedupe.IdempotencyKey("acme", dedupe.Key{Table: "clicks", ID: long}),
		dedupe.IdempotencyKey("acme", dedupe.Key{Table: "clicks", ID: long + "y"}),
	}
	seen := map[string]bool{base: true}
	for _, k := range others {
		assert.False(t, seen[k], "collision: %s", k)
		seen[k] = true
	}
}
