package dedupe

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The key layout every backend stores, byte for byte:
//
//	keyVersion ‖ tenant ‖ keySeparator ‖ table ‖ keySeparator ‖ id
//
// A tenant id is letters, digits, '_' and '-', so it never holds the
// separator and never starts with keyVersion — the version-0 keys before
// #222 (tenant ‖ 0x00 ‖ id) never meet these. The id is last, so it may hold
// anything.
const (
	keyVersion   byte = 0x01
	keySeparator byte = 0x00
	// hashedID leads an id stored as its SHA-256 rather than verbatim. Ids
	// that start with it are hashed too, so a verbatim id never reads as a
	// hashed one.
	hashedID byte = 0xFF
	// MaxIDBytes is the longest id stored verbatim: a DynamoDB partition key
	// holds at most 2,048 bytes, and the tenant and table share them.
	MaxIDBytes = 1024
)

// ErrInvalidKey is returned for a key no backend can store: a table name
// holding the separator byte.
var ErrInvalidKey = errors.New("invalid dedupe key")

// KeyPrefix is the part of every key that names tenant id, so a backend
// computes it once per tenant store.
func KeyPrefix(id tenant.ID) []byte {
	p := make([]byte, 0, len(id)+2)
	p = append(p, keyVersion)
	p = append(p, id...)
	return append(p, keySeparator)
}

// Validate reports whether k can be stored.
func (k Key) Validate() error {
	if strings.IndexByte(k.Table, keySeparator) >= 0 {
		return fmt.Errorf("%w: table name %q holds a NUL byte", ErrInvalidKey, k.Table)
	}
	return nil
}

// Hashed reports whether k's id is stored as its SHA-256 rather than
// verbatim.
func (k Key) Hashed() bool {
	return len(k.ID) > MaxIDBytes || (k.ID != "" && k.ID[0] == hashedID)
}

// AppendKey appends k's stored form, under the tenant prefix from KeyPrefix,
// to dst. k must be valid.
func AppendKey(dst, prefix []byte, k Key) []byte {
	dst = append(dst, prefix...)
	dst = append(dst, k.Table...)
	dst = append(dst, keySeparator)
	if k.Hashed() {
		sum := sha256.Sum256([]byte(k.ID))
		dst = append(dst, hashedID)
		return append(dst, sum[:]...)
	}
	return append(dst, k.ID...)
}

// IdempotencyKey is k's message id for the queue under tenant id: the first
// 128 bits of the stored key's SHA-256, in hex, so a republished record is
// recognised without its id riding in a header verbatim.
func IdempotencyKey(id tenant.ID, k Key) string {
	sum := sha256.Sum256(AppendKey(nil, KeyPrefix(id), k))
	return hex.EncodeToString(sum[:16])
}
