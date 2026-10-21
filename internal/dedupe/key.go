package dedupe

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"strconv"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
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
// A tenant-partitioned DynamoDB table (PartitionedKey) splits the same key at
// the tenant, so that one tenant's items can be listed without a scan:
//
//	pk  <tenant>/<bucket>       acme/7
//	sk  <table>/<id>            clicks/evt-123
const (
	keySep     = '/'
	hashedMark = '#'
	// MaxIDBytes is the longest escaped id AppendKey stores verbatim: a
	// DynamoDB partition key holds at most 2,048 bytes, and the tenant and
	// table share them. PartitionedKey may hash a shorter one.
	MaxIDBytes = 1024
	// TenantBuckets is how many partition keys one tenant's items spread
	// over in a tenant-partitioned table, each with DynamoDB's own write
	// ceiling. Changing it would move every item: it is a constant of the
	// layout, never a setting.
	TenantBuckets = 16
	// maxSortKeyBytes is DynamoDB's limit on a sort key.
	maxSortKeyBytes = 1024
)

// KeyPrefix is the part of every key that names tenant id, so a backend
// computes it once per tenant store.
func KeyPrefix(id tenant.ID) []byte {
	return append([]byte(id), keySep)
}

// Hashed reports whether AppendKey stores k's id as its SHA-256 rather than
// verbatim: whether its escaped form is longer than MaxIDBytes.
// PartitionedKey also hashes a shorter id when its sort key would not fit.
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
	dst = append(dst, prefix...)
	if k.Hashed() {
		return appendHashed(dst, k)
	}
	return keyenc.AppendJoin(dst, keySep, k.Table, k.ID)
}

func appendHashed(dst []byte, k Key) []byte {
	sum := sha256.Sum256([]byte(k.ID))
	dst = keyenc.AppendJoin(dst, keySep, k.Table)
	return hex.AppendEncode(append(dst, keySep, hashedMark), sum[:])
}

// PartitionedKey is k's key in a tenant-partitioned table. sk is AppendKey's
// form without the tenant, with the id hashed whenever sk would otherwise
// pass DynamoDB's 1,024 bytes; pk is the tenant and sk's bucket, FNV-1a 64 of
// sk mod TenantBuckets, in decimal. A tenant id holds no '/', so no two
// tenants share a pk.
func PartitionedKey(id tenant.ID, k Key) (pk, sk string) {
	b := AppendKey(nil, nil, k)
	if len(b) > maxSortKeyBytes {
		b = appendHashed(b[:0], k)
	}
	h := fnv.New64a()
	_, _ = h.Write(b) // never fails
	return string(id) + string(keySep) + strconv.FormatUint(h.Sum64()%TenantBuckets, 10), string(b)
}

// IdempotencyKey is k's message id for the queue under tenant id: the first
// 128 bits of the stored key's SHA-256, in hex, so a republished record is
// recognised without its id riding in a header verbatim.
func IdempotencyKey(id tenant.ID, k Key) string {
	sum := sha256.Sum256(AppendKey(nil, KeyPrefix(id), k))
	return hex.EncodeToString(sum[:16])
}
