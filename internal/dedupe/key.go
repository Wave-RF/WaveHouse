package dedupe

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"strconv"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/pkg/shard"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// The key every backend stores is text:
//
//	<tenant>/<table>/<id>        acme/clicks/evt-123
//	<tenant>/<table>/#<sha256>   an id too long to store verbatim
//
// The table and id are escaped and joined by internal/keyenc, which never
// writes '/' or '#', and a tenant id holds neither (tenant.Parse), so the
// fields split back apart, a table name may hold any byte, and no two
// (tenant, table, id) triples share a key. A key is ASCII, so it is a valid
// DynamoDB String, and holds no NUL, so it never meets a tenant ‖ NUL ‖ id
// key written before #222.
//
// A tenant-partitioned DynamoDB table (PartitionedKey) splits the same key
// before the id, and spreads each table's ids over TableBuckets partition
// keys, so that one table's writes are not held to one partition's:
//
//	pk  <tenant>/<table>/<bucket>   acme/clicks/12
//	sk  <id>                        evt-123
const (
	keySep     = '/'
	hashedMark = '#'
	// MaxIDBytes is the longest escaped id stored verbatim: a DynamoDB
	// partition key holds at most 2,048 bytes, and the tenant and table share
	// them; a tenant-partitioned table's sort key, which the id has to
	// itself, holds 1,024.
	MaxIDBytes = 1024
	// TableBuckets is how many partition keys one table's items spread over
	// in a tenant-partitioned table, each held to DynamoDB's per-partition
	// write ceiling. Changing it moves items, so it is a constant of the
	// layout, never a setting; another count is another layout (dynamodb.go).
	TableBuckets = 256
)

// KeyPrefix is the part of every key that names tenant id, so a backend
// computes it once per tenant store.
func KeyPrefix(id tenant.ID) []byte {
	return append([]byte(id), keySep)
}

// Hashed reports whether k's id is stored as its SHA-256 rather than
// verbatim: whether its escaped form is longer than MaxIDBytes.
func (k Key) Hashed() bool {
	switch {
	case len(k.ID) > MaxIDBytes:
		return true
	case 3*len(k.ID) <= MaxIDBytes: // escaping at most triples a byte
		return false
	}
	return len(keyenc.Escape(k.ID)) > MaxIDBytes
}

// AppendKey appends k's stored form, under the tenant prefix from KeyPrefix,
// to dst.
func AppendKey(dst, prefix []byte, k Key) []byte {
	dst = keyenc.AppendJoin(append(dst, prefix...), keySep, k.Table)
	return appendID(append(dst, keySep), k)
}

// appendID appends k's id as AppendKey stores it.
func appendID(dst []byte, k Key) []byte {
	if k.Hashed() {
		return appendHashedID(dst, k.ID)
	}
	return keyenc.AppendEscape(dst, k.ID)
}

func appendHashedID(dst []byte, id string) []byte {
	sum := sha256.Sum256([]byte(id))
	return hex.AppendEncode(append(dst, hashedMark), sum[:])
}

// PartitionedKey is k's key in a tenant-partitioned table: sk is the id as
// AppendKey stores it, except that an empty id, which a sort key cannot hold,
// is stored hashed too; pk is the tenant, the table and sk's bucket among
// TableBuckets, shard.Jump of shard.Mix of FNV-1a 64 over sk, in decimal.
// Neither a tenant id nor an escaped table holds '/', so no two tables share
// a pk.
func PartitionedKey(id tenant.ID, k Key) (pk, sk string) {
	s := appendID(nil, k)
	if len(s) == 0 {
		s = appendHashedID(s, k.ID)
	}
	h := fnv.New64a()
	_, _ = h.Write(s) // never fails
	p := keyenc.AppendJoin(KeyPrefix(id), keySep, k.Table)
	p = strconv.AppendInt(append(p, keySep), int64(shard.Jump(shard.Mix(h.Sum64()), TableBuckets)), 10)
	return string(p), string(s)
}

// IdempotencyKey is k's message id for the queue under tenant id: the first
// 128 bits of the stored key's SHA-256, in hex, so a republished record is
// recognised without its id riding in a header verbatim.
func IdempotencyKey(id tenant.ID, k Key) string {
	sum := sha256.Sum256(AppendKey(nil, KeyPrefix(id), k))
	return hex.EncodeToString(sum[:16])
}
