package api

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// queryCacheKey produces a deterministic L1/L2 cache key for a (sql, params)
// pair. The raw-SQL endpoint (`POST /v1/ops/query`) does not cache, but the
// structured query (`POST /v1/query?table={table}`) and named pipes
// (`GET/POST /v1/pipes/{name}`) handlers do — they share this helper so a
// key change in one place propagates to every cached read path.
//
// params are the ClickHouse query-parameter values, positionally, exactly as
// they go on the wire: already rendered as text and already encoded for
// ClickHouse's parameter reader. That encoding is part of the key's input on
// purpose — two reads whose parameters reach ClickHouse as the same bytes are
// the same query, and two that do not are not. (The values used to be typed
// Go scalars, which is why this framing used to carry a `{type, value}` JSON
// payload; every value is a String parameter now, so the type is constant.)
//
// Every section is framed with a 1-byte type marker (0x01 for sql, 0x00 for
// param) plus an 8-byte big-endian length, then the payload. Without
// length-prefixing the sql itself, a SQL string crafted to end with the
// exact bytes of a param frame (`\x00` + 8 length bytes + payload) would
// hash identically to a shorter SQL plus a real param — distinct
// `(sql, params)` tuples, same digest. Per-param framing keeps its own
// length prefix so an embedded `\x00` inside a value can't be confused for
// a frame boundary.
func queryCacheKey(sql string, params []string) string {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(sql)))
	_, _ = h.Write([]byte{1}) // sql frame marker
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(sql))
	for _, p := range params {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		_, _ = h.Write([]byte{0}) // param frame marker
		_, _ = h.Write(n[:])
		_, _ = h.Write([]byte(p))
	}
	return "query:" + hex.EncodeToString(h.Sum(nil))
}
