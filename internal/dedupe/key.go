package dedupe

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The key layout every backend stores, byte for byte:
//
//	keyVersion ‖ uvarint(len(tenant)) ‖ tenant ‖ uvarint(len(table)) ‖ table ‖ id
//
// Each field before the id carries its length, so a table name may hold any
// byte — NUL included — and no two (tenant, table, id) triples share a key.
// The id is last, so it needs no length and may hold anything too. A tenant
// id never starts with keyVersion (tenant.Parse admits letters, digits, '_'
// and '-'), so the version-0 keys before #222 (tenant ‖ 0x00 ‖ id) never meet
// these.
const (
	keyVersion byte = 0x01
	// hashedID leads an id stored as its SHA-256 rather than verbatim. Ids
	// that start with it are hashed too, so a verbatim id never reads as a
	// hashed one.
	hashedID byte = 0xFF
	// MaxIDBytes is the longest id stored verbatim: a DynamoDB partition key
	// holds at most 2,048 bytes, and the tenant and table share them.
	MaxIDBytes = 1024
)

// KeyPrefix is the part of every key that names tenant id, so a backend
// computes it once per tenant store.
func KeyPrefix(id tenant.ID) []byte {
	p := make([]byte, 0, len(id)+1+binary.MaxVarintLen64)
	p = append(p, keyVersion)
	return appendField(p, string(id))
}

// Hashed reports whether k's id is stored as its SHA-256 rather than
// verbatim.
func (k Key) Hashed() bool {
	return len(k.ID) > MaxIDBytes || (k.ID != "" && k.ID[0] == hashedID)
}

// AppendKey appends k's stored form, under the tenant prefix from KeyPrefix,
// to dst.
func AppendKey(dst, prefix []byte, k Key) []byte {
	dst = append(dst, prefix...)
	dst = appendField(dst, k.Table)
	if k.Hashed() {
		sum := sha256.Sum256([]byte(k.ID))
		dst = append(dst, hashedID)
		return append(dst, sum[:]...)
	}
	return append(dst, k.ID...)
}

func appendField(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}
