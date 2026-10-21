package dedupe_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
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

// The tenant-partitioned layout is pinned the same way, bucket included: the
// same key in another bucket is another item, so a refactor that moved it
// would forget every id stored. The buckets were computed apart from this
// code, from FNV-1a 64's published offset and prime.
func TestPartitionedKey(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", dedupe.MaxIDBytes+1)
	sum := sha256.Sum256([]byte(long))
	for _, tt := range []struct {
		tn     tenant.ID
		k      dedupe.Key
		pk, sk string
	}{
		{"acme", dedupe.Key{Table: "clicks", ID: "evt-123"}, "acme/7", "clicks/evt-123"},
		{"acme-co", dedupe.Key{Table: "db.t", ID: "a/b"}, "acme-co/2", "db%2Et/a%2Fb"},
		{"0", dedupe.Key{Table: "", ID: ""}, "0/14", "/"},
		{"0", dedupe.Key{Table: "t", ID: "#%café"}, "0/2", "t/%23%25caf%C3%A9"},
		{"acme", dedupe.Key{Table: "t", ID: long}, "acme/2", "t/#" + hex.EncodeToString(sum[:])},
	} {
		pk, sk := dedupe.PartitionedKey(tt.tn, tt.k)
		assert.Equal(t, tt.pk, pk, "%+v", tt.k)
		assert.Equal(t, tt.sk, sk, "%+v", tt.k)
	}
}

// A sort key holds at most 1,024 bytes, so this layout hashes an id once the
// table, separator and id together would pass that, though AppendKey keeps
// it verbatim up to 1,024 bytes of id alone.
func TestPartitionedKey_HashesPastTheSortKeyLimit(t *testing.T) {
	t.Parallel()
	fits := dedupe.Key{Table: "t", ID: strings.Repeat("x", 1022)}
	pk, sk := dedupe.PartitionedKey("a", fits)
	assert.Equal(t, "a/4", pk)
	assert.Equal(t, "t/"+fits.ID, sk)

	over := dedupe.Key{Table: "t", ID: strings.Repeat("x", 1023)}
	require.False(t, over.Hashed(), "verbatim in AppendKey's form")
	sum := sha256.Sum256([]byte(over.ID))
	pk, sk = dedupe.PartitionedKey("a", over)
	assert.Equal(t, "a/10", pk)
	assert.Equal(t, "t/#"+hex.EncodeToString(sum[:]), sk)
}

// A partition key splits back to its tenant, so no two tenants share one,
// even where one id is a prefix of the other (acme, acme-2); and sequential
// ids reach every bucket about evenly.
func TestPartitionedKey_Buckets(t *testing.T) {
	t.Parallel()
	per := map[string]int{}
	for _, tn := range []tenant.ID{"acme", "acme-2", "acme_x", "acm"} {
		for i := range 1600 {
			pk, _ := dedupe.PartitionedKey(tn, dedupe.Key{Table: "events", ID: fmt.Sprintf("evt-%d", i)})
			got, b, ok := strings.Cut(pk, "/")
			require.True(t, ok, pk)
			require.Equal(t, string(tn), got)
			n, err := strconv.Atoi(b)
			require.NoError(t, err)
			require.True(t, n >= 0 && n < dedupe.TenantBuckets, pk)
			if tn == "acme" {
				per[pk]++
			}
		}
	}
	assert.Len(t, per, dedupe.TenantBuckets)
	for pk, n := range per {
		assert.InDelta(t, 100, n, 25, pk)
	}
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
	parts, err := keyenc.Split(s, '/')
	require.NoError(t, err)
	require.Len(t, parts, 3, s)
	_, err = tenant.Parse(parts[0])
	require.NoError(t, err)
	return parts[0], parts[1], parts[2]
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
		// So is the partitioned key, which is the same text split at the
		// tenant while the sort key is under DynamoDB's limit.
		pk, sk := dedupe.PartitionedKey(tn, k)
		require.Equal(t, s, string(tn)+"/"+sk)
		require.True(t, strings.HasPrefix(pk, string(tn)+"/"), pk)
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
