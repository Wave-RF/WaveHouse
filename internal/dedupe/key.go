package dedupe

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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
const (
	keySep     = '/'
	hashedMark = '#'
	// MaxIDBytes is the longest escaped id stored verbatim: a DynamoDB
	// partition key holds at most 2,048 bytes, and the tenant and table share
	// them.
	MaxIDBytes = 1024
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
	dst = append(dst, prefix...)
	if k.Hashed() {
		sum := sha256.Sum256([]byte(k.ID))
		dst = keyenc.AppendJoin(dst, keySep, k.Table)
		return hex.AppendEncode(append(dst, keySep, hashedMark), sum[:])
	}
	return keyenc.AppendJoin(dst, keySep, k.Table, k.ID)
}

// IdempotencyKey is k's message id for the queue under tenant id: the first
// 128 bits of the stored key's SHA-256, in hex, so a republished record is
// recognised without its id riding in a header verbatim.
func IdempotencyKey(id tenant.ID, k Key) string {
	sum := sha256.Sum256(AppendKey(nil, KeyPrefix(id), k))
	return hex.EncodeToString(sum[:16])
}
