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
// code, from FNV-1a 64's, splitmix64's and the jump hash's published
// definitions.
func TestPartitionedKey(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", dedupe.MaxIDBytes+1)
	sum := sha256.Sum256([]byte(long))
	empty := sha256.Sum256(nil)
	for _, tt := range []struct {
		tn     tenant.ID
		k      dedupe.Key
		pk, sk string
	}{
		{"acme", dedupe.Key{Table: "clicks", ID: "evt-123"}, "acme/clicks/12", "evt-123"},
		{"acme-co", dedupe.Key{Table: "db.t", ID: "a/b"}, "acme-co/db%2Et/33", "a%2Fb"},
		{"0", dedupe.Key{Table: "t", ID: "#%café"}, "0/t/203", "%23%25caf%C3%A9"},
		{"acme", dedupe.Key{Table: "t", ID: long}, "acme/t/230", "#" + hex.EncodeToString(sum[:])},
		{"a", dedupe.Key{Table: "t", ID: strings.Repeat("x", dedupe.MaxIDBytes)}, "a/t/181", strings.Repeat("x", dedupe.MaxIDBytes)},
		// A sort key cannot be empty, so an empty id is stored hashed.
		{"0", dedupe.Key{Table: "", ID: ""}, "0//163", "#" + hex.EncodeToString(empty[:])},
	} {
		pk, sk := dedupe.PartitionedKey(tt.tn, tt.k)
		assert.Equal(t, tt.pk, pk, "%+v", tt.k)
		assert.Equal(t, tt.sk, sk, "%+v", tt.k)
		assert.LessOrEqual(t, len(sk), 1024, "DynamoDB's limit on a sort key")
	}
}

// A partition key splits back into its tenant, table and bucket, so no two
// tables share one, even where one tenant or table name is a prefix of
// another (acme, acme-2); and sequential ids reach every bucket about evenly.
func TestPartitionedKey_Buckets(t *testing.T) {
	t.Parallel()
	per := map[string]int{}
	for _, tn := range []tenant.ID{"acme", "acme-2", "acme_x", "acm"} {
		for _, table := range []string{"events", "events-2", "a/b"} {
			for i := range 25_600 {
				pk, _ := dedupe.PartitionedKey(tn, dedupe.Key{Table: table, ID: fmt.Sprintf("evt-%d", i)})
				parts, err := keyenc.Split(pk, '/')
				require.NoError(t, err)
				require.Equal(t, []string{string(tn), table}, parts[:2], pk)
				n, err := strconv.Atoi(parts[2])
				require.NoError(t, err)
				require.True(t, n >= 0 && n < dedupe.TableBuckets, pk)
				if tn == "acme" && table == "events" {
					per[pk]++
				}
			}
		}
	}
	assert.Len(t, per, dedupe.TableBuckets)
	for pk, n := range per {
		assert.InDelta(t, 100, n, 30, pk)
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
		// So is the partitioned key: the same text split before the id, the
		// bucket in between, but for the empty id a sort key cannot hold.
		pk, sk := dedupe.PartitionedKey(tn, k)
		if k.ID != "" {
			cut := strings.LastIndexByte(pk, '/')
			require.Equal(t, s, pk[:cut+1]+sk)
		}
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
