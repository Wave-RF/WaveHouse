package cache

import (
	"cmp"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// tokenLen is the size of a version token: random, so a token key that is
// lost (evicted, expired, flushed) and recreated can never match a value
// stored under its predecessor, as a counter restarting at 0 would.
const tokenLen = 8

// decodedFactor bounds a value's decompressed size at this multiple of the
// stored-size limit, refusing a zip bomb planted in a shared server.
const decodedFactor = 8

// Value layout: format, flags, expires-at (unix ms), token count, tokens,
// payload. Big-endian.
const (
	valueFormat  = 1
	flagZstd     = 1 << 0
	headerLen    = 1 + 1 + 8 + 2
	maxTokenKeys = 1<<16 - 1
)

var errCorruptValue = errors.New("cache: corrupt value")

func newToken() []byte {
	b := make([]byte, tokenLen)
	_, _ = rand.Read(b) // never fails (crypto/rand, Go ≥ 1.24)
	return b
}

// tenantTokenKey is the key of tenant id's token. Every token key carries
// the tenant as a hash tag, so all of a tenant's tokens share one cluster
// slot and a lookup reads them with one MGET. The tenant goes in verbatim:
// its grammar is keyenc's kept bytes, so it is its own escaped form.
func tenantTokenKey(prefix string, id tenant.ID) string {
	return prefix + ":{" + string(id) + "}:T"
}

// tableTokenKey and scopeTokenKey take raw names and escape them after the
// fixed prefix (keyenc), so no ':' in a name reads as the separator.
//
// Their layout is a protocol between builds: every process on the server
// reads and bumps these keys for itself, so two builds that lay them out
// differently — a change here, or to what keyenc keeps — split them, and one
// build's bumps miss the entries the other filed, which are served until
// their TTL for the whole rolling deploy. Such a change needs the new build
// to read and bump both layouts (fold the old tokens into what it files)
// until no old build is left, a later build dropping the old; or an upgrade
// that never runs two builds against the server at once. The tenant token,
// placed verbatim, stays shared; a valueKey change only orphans values,
// which is safe to roll.
func tableTokenKey(prefix string, id tenant.ID, table string) string {
	return string(keyenc.AppendJoin([]byte(prefix+":{"+string(id)+"}:B:"), ':', table))
}

func scopeTokenKey(prefix string, id tenant.ID, table, scope string) string {
	return string(keyenc.AppendJoin([]byte(prefix+":{"+string(id)+"}:S:"), ':', table, scope))
}

// sortedDeps returns deps in canonical order without duplicates.
func sortedDeps(deps []Namespace) []Namespace {
	out := slices.Clone(deps)
	slices.SortFunc(out, func(a, b Namespace) int {
		return cmp.Or(cmp.Compare(a.Table, b.Table), cmp.Compare(a.Scope, b.Scope))
	})
	return slices.Compact(out)
}

// tokenKeys lists the tokens a result for deps of tenant id is filed under,
// in canonical order: the tenant's, then each dep's table and scope tokens.
// A dep with scope s folds B:table (bumped by a whole-table write) and
// S:table:s (bumped by a write to s, and — for s == "" — by any scoped write
// to the table), the same lattice LocalCache's version index encodes.
func tokenKeys(prefix string, id tenant.ID, deps []Namespace) []string {
	keys := []string{tenantTokenKey(prefix, id)}
	for _, d := range sortedDeps(deps) {
		keys = append(keys, tableTokenKey(prefix, id, d.Table), scopeTokenKey(prefix, id, d.Table, d.Scope))
	}
	slices.Sort(keys[1:])
	return append(keys[:1], slices.Compact(keys[1:])...)
}

// bumpKeys lists the tokens an invalidation of ns replaces: a whole-table
// write the table's, a scoped write its scope's and the whole-table view's.
func bumpKeys(prefix string, ns Namespace) []string {
	if ns.Scope == "" {
		return []string{tableTokenKey(prefix, ns.Tenant, ns.Table)}
	}
	return []string{scopeTokenKey(prefix, ns.Tenant, ns.Table, ns.Scope), scopeTokenKey(prefix, ns.Tenant, ns.Table, "")}
}

// valueKey names the entry for sha over deps. It carries no versions, so a
// refill overwrites in place, and no hash tag, so one tenant's values spread
// across a cluster's shards. It hashes the escaped sha and each dep's
// escaped, joined table and scope, each ended by a NUL, which escaping never
// writes, so no two sets of names hash the same input.
func valueKey(prefix string, id tenant.ID, sha string, deps []Namespace) string {
	h := sha256.New()
	b := keyenc.AppendEscape(nil, sha)
	h.Write(append(b, 0))
	for _, d := range sortedDeps(deps) {
		b = keyenc.AppendJoin(b[:0], ':', d.Table, d.Scope)
		h.Write(append(b, 0))
	}
	return prefix + ":q:" + string(id) + ":" + hex.EncodeToString(h.Sum(nil))
}

// codec compresses and frames values. Its zstd encoder and decoder are safe
// for concurrent EncodeAll/DecodeAll.
type codec struct {
	enc         *zstd.Encoder
	dec         *zstd.Decoder
	compressMin int
	maxDecoded  int
}

func newCodec(compressMin, maxDecoded int) (*codec, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(maxDecoded)), zstd.WithDecoderConcurrency(0)) //nolint:gosec // maxDecoded is a positive config-derived int
	if err != nil {
		return nil, err
	}
	return &codec{enc: enc, dec: dec, compressMin: compressMin, maxDecoded: maxDecoded}, nil
}

func (c *codec) close() {
	_ = c.enc.Close()
	c.dec.Close()
}

// encode frames payload with the tokens it was computed under, compressing
// it when that is enabled, the payload is large enough, and it helps.
func (c *codec) encode(tokens []byte, expiresAt time.Time, payload []byte) []byte {
	var flags byte
	body := payload
	if c.compressMin > 0 && len(payload) >= c.compressMin {
		if z := c.enc.EncodeAll(payload, nil); len(z) < len(payload) {
			body, flags = z, flagZstd
		}
	}
	out := make([]byte, headerLen, headerLen+len(tokens)+len(body))
	out[0] = valueFormat
	out[1] = flags
	binary.BigEndian.PutUint64(out[2:10], uint64(expiresAt.UnixMilli()))
	binary.BigEndian.PutUint16(out[10:12], uint16(len(tokens)/tokenLen)) //nolint:gosec // bounded by maxTokenKeys at Lookup
	out = append(out, tokens...)
	return append(out, body...)
}

// decode is encode's inverse. An unknown format — a newer process's value
// during a rolling upgrade — is an error, which the caller reads as a miss.
func (c *codec) decode(b []byte) (tokens []byte, expiresAt time.Time, payload []byte, err error) {
	if len(b) < headerLen {
		return nil, time.Time{}, nil, errCorruptValue
	}
	if b[0] != valueFormat {
		return nil, time.Time{}, nil, fmt.Errorf("%w: format %d", errCorruptValue, b[0])
	}
	flags := b[1]
	expiresAt = time.UnixMilli(int64(binary.BigEndian.Uint64(b[2:10]))) //nolint:gosec // written by encode
	n := int(binary.BigEndian.Uint16(b[10:12])) * tokenLen
	if len(b) < headerLen+n {
		return nil, time.Time{}, nil, errCorruptValue
	}
	tokens = b[headerLen : headerLen+n]
	payload = b[headerLen+n:]
	switch flags {
	case 0:
	case flagZstd:
		if payload, err = c.dec.DecodeAll(payload, nil); err != nil {
			return nil, time.Time{}, nil, fmt.Errorf("%w: %w", errCorruptValue, err)
		}
		if len(payload) > c.maxDecoded {
			return nil, time.Time{}, nil, fmt.Errorf("%w: decodes to %d bytes", errCorruptValue, len(payload))
		}
	default:
		return nil, time.Time{}, nil, fmt.Errorf("%w: flags %#x", errCorruptValue, flags)
	}
	return tokens, expiresAt, payload, nil
}

// jitter spreads d by ±10%, drawing on the token being written so a batch of
// bumps doesn't expire together.
func jitter(d time.Duration, token []byte) time.Duration {
	span := int64(d) / 5
	if span <= 0 {
		return d
	}
	r := int64(binary.BigEndian.Uint64(token) % uint64(span)) //nolint:gosec // r < span, an int64
	return d - time.Duration(span/2) + time.Duration(r)
}
