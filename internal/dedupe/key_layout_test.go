package dedupe_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

func key(tn tenant.ID, k dedupe.Key) string {
	return string(dedupe.AppendKey(nil, dedupe.KeyPrefix(tn), k))
}

// The layout is pinned byte for byte: DynamoDB items and Pebble keys outlive
// the binary that wrote them.
func TestKeyLayout(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		tn   tenant.ID
		k    dedupe.Key
		want string
	}{
		{"acme", dedupe.Key{Table: "clicks", ID: "evt-123"}, "acme/clicks/evt-123"},
		{"acme-co", dedupe.Key{Table: "db.t", ID: "a/b"}, "acme-co/db%2Et/a%2Fb"},
		{"0", dedupe.Key{Table: "a\x00b", ID: "e1"}, "0/a%00b/e1"},
		{"0", dedupe.Key{Table: "", ID: ""}, "0//"},
		{"0", dedupe.Key{Table: "t", ID: "#%café"}, "0/t/%23%25caf%C3%A9"},
	} {
		assert.Equal(t, tt.want, key(tt.tn, tt.k), "%+v", tt.k)
	}

	long := strings.Repeat("x", dedupe.MaxIDBytes+1)
	sum := sha256.Sum256([]byte(long))
	assert.Equal(t, "acme/t/#"+hex.EncodeToString(sum[:]), key("acme", dedupe.Key{Table: "t", ID: long}))
}

// The limit is on the escaped id: 1,024 bytes that escape to more are hashed,
// and an id escaping to exactly 1,024 is not.
func TestKeyLayout_HashesOnTheEscapedLength(t *testing.T) {
	t.Parallel()
	fits := strings.Repeat("x", dedupe.MaxIDBytes)
	assert.False(t, dedupe.Key{ID: fits}.Hashed())
	assert.Equal(t, "a/t/"+fits, key("a", dedupe.Key{Table: "t", ID: fits}))

	escapedFits := strings.Repeat(".", dedupe.MaxIDBytes/3) + "x" // 1,023 + 1 bytes escaped
	assert.False(t, dedupe.Key{ID: escapedFits}.Hashed())
	escapedOver := strings.Repeat(".", dedupe.MaxIDBytes/3+1) // 1,026 bytes escaped
	assert.True(t, dedupe.Key{ID: escapedOver}.Hashed())
	assert.True(t, strings.HasPrefix(key("a", dedupe.Key{Table: "t", ID: escapedOver}), "a/t/#"))
}

// decodeKey inverts AppendKey. That it exists — every key splits back into
// the one triple that wrote it — is what makes the layout collision-free.
func decodeKey(t *testing.T, s string) (tn, table, idPart string) {
	t.Helper()
	require.True(t, utf8.ValidString(s), "a key is a valid DynamoDB String")
	require.NotContains(t, s, "\x00")
	parts := strings.Split(s, "/")
	require.Len(t, parts, 3, s)
	_, err := tenant.Parse(parts[0])
	require.NoError(t, err)
	table, err = keyenc.Unescape(parts[1])
	require.NoError(t, err)
	if strings.HasPrefix(parts[2], "#") {
		return parts[0], table, parts[2]
	}
	id, err := keyenc.Unescape(parts[2])
	require.NoError(t, err)
	return parts[0], table, id
}

// Triples a separator could confuse — the separator, the escape and hash
// marks, NUL, or what an escape looks like, in the table or the id, at either
// end, or moved across the table/id boundary — each get a key of their own
// and parse back to themselves.
func TestKeyLayout_NoCollisions(t *testing.T) {
	t.Parallel()
	tenants := []tenant.ID{"a", "ab", "a_b", "a-b", "acme"}
	pieces := []string{"", "/", "%", "#", "\x00", "\xff", "a", "b", "a/", "/b", "a/b", "%2F", "a%2Fb", "#a", "acme/a", "é"}
	seen := map[string]string{}
	check := func(tn tenant.ID, k dedupe.Key) {
		s := key(tn, k)
		who := string(tn) + " | " + k.Table + " | " + k.ID
		if prev, ok := seen[s]; ok && prev != who {
			t.Fatalf("%q and %q share the key %q", prev, who, s)
		}
		seen[s] = who
		gotTenant, gotTable, idPart := decodeKey(t, s)
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
			for _, c := range []string{"/", "%", "#", "2", "F", "\x00", "a"} {
				grow(prefix + c)
			}
		}
	}
	grow("")
	for _, tn := range tenants[:2] {
		for _, table := range all {
			for _, id := range all {
				check(tn, dedupe.Key{Table: table, ID: id})
			}
		}
	}
	// A hashed id never reads as a verbatim one, even one spelling the hash.
	long := strings.Repeat("x", dedupe.MaxIDBytes+1)
	sum := sha256.Sum256([]byte(long))
	check("a", dedupe.Key{Table: "t", ID: long})
	check("a", dedupe.Key{Table: "t", ID: "#" + hex.EncodeToString(sum[:])})
}
