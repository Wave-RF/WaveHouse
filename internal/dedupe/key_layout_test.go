package dedupe_test

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The layout is pinned byte for byte: DynamoDB items and Pebble keys outlive
// the binary that wrote them.
func TestKeyLayout(t *testing.T) {
	t.Parallel()
	got := dedupe.AppendKey(nil, dedupe.KeyPrefix("acme"), dedupe.Key{Table: "a\x00b", ID: "e1"})
	assert.Equal(t, []byte("\x01\x04acme\x03a\x00be1"), got)

	long := strings.Repeat("x", dedupe.MaxIDBytes+1)
	sum := sha256.Sum256([]byte(long))
	got = dedupe.AppendKey(nil, dedupe.KeyPrefix("acme"), dedupe.Key{Table: "t", ID: long})
	assert.Equal(t, append([]byte("\x01\x04acme\x01t\xff"), sum[:]...), got)

	table := strings.Repeat("t", 200)
	got = dedupe.AppendKey(nil, dedupe.KeyPrefix("acme"), dedupe.Key{Table: table, ID: "e1"})
	assert.Equal(t, []byte("\x01\x04acme\xc8\x01"+table+"e1"), got, "a length past 127 takes two bytes")
}

// decodeKey inverts AppendKey. That it exists — every key parses back to the
// one triple that wrote it — is what makes the layout collision-free.
func decodeKey(t *testing.T, b []byte) (tn, table, idPart string) {
	t.Helper()
	require.NotEmpty(t, b)
	require.Equal(t, byte(0x01), b[0])
	b = b[1:]
	field := func() string {
		n, w := binary.Uvarint(b)
		require.Positive(t, w)
		b = b[w:]
		require.LessOrEqual(t, n, uint64(len(b)))
		s := string(b[:n])
		b = b[n:]
		return s
	}
	tn = field()
	table = field()
	return tn, table, string(b)
}

// Triples a separator could confuse — NUL or the version byte in the table,
// in the id, at either end, or moved across the table/id boundary — each get
// a key of their own, and parse back to themselves.
func TestKeyLayout_NoCollisions(t *testing.T) {
	t.Parallel()
	tenants := []tenant.ID{"a", "ab", "a_b", "acme"}
	pieces := []string{"", "\x00", "\x01", "\xff", "a", "b", "a\x00", "\x00b", "a\x00b", "\x01\x04acme", "\x03a"}
	seen := map[string]string{}
	check := func(tn tenant.ID, k dedupe.Key) {
		key := dedupe.AppendKey(nil, dedupe.KeyPrefix(tn), k)
		who := string(tn) + " / " + k.Table + " / " + k.ID
		if prev, ok := seen[string(key)]; ok && prev != who {
			t.Fatalf("%q and %q share the key %q", prev, who, key)
		}
		seen[string(key)] = who
		gotTenant, gotTable, idPart := decodeKey(t, key)
		assert.Equal(t, string(tn), gotTenant)
		assert.Equal(t, k.Table, gotTable)
		if !k.Hashed() {
			assert.Equal(t, k.ID, idPart)
		}
	}
	for _, tn := range tenants {
		for _, table := range pieces {
			for _, id := range pieces {
				check(tn, dedupe.Key{Table: table, ID: id})
			}
		}
	}
	// Every table and id up to three bytes over an alphabet of the bytes a
	// layout could misread.
	var all []string
	var grow func(prefix string)
	grow = func(prefix string) {
		all = append(all, prefix)
		if len(prefix) < 3 {
			for _, c := range []string{"\x00", "\x01", "\x04", "\xff", "a"} {
				grow(prefix + c)
			}
		}
	}
	grow("")
	for _, tn := range tenants {
		for _, table := range all {
			for _, id := range all {
				check(tn, dedupe.Key{Table: table, ID: id})
			}
		}
	}
}
