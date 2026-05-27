package api

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// queryCacheKey produces a deterministic L1/L2 cache key for a (sql, params)
// pair. The raw-SQL endpoint (`POST /v1/admin/query`) does not cache, but the
// structured query (`POST /v1/query?table={table}`) and named pipes
// (`GET/POST /v1/pipes/{name}`) handlers do — they share this helper so a
// key change in one place propagates to every cached read path.
//
// Every section is framed with a 1-byte type marker (0x01 for sql, 0x00 for
// param) plus an 8-byte big-endian length, then the payload. Without
// length-prefixing the sql itself, a SQL string crafted to end with the
// exact bytes of a param frame (`\x00` + 8 length bytes + payload) would
// hash identically to a shorter SQL plus a real param — distinct
// `(sql, params)` tuples, same digest. Per-param framing also kept its
// own length prefix so embedded `\x00` inside a string param can't be
// confused for a frame boundary, and the JSON `{type, value}` payload
// shape additionally separates `"42"` (string) from `42` (int) so the
// cache can't serve a string-typed row to an int-typed lookup.
func queryCacheKey(sql string, params []any) string {
	h := sha256.New()
	var sqlLen [8]byte
	binary.BigEndian.PutUint64(sqlLen[:], uint64(len(sql)))
	_, _ = h.Write([]byte{1}) // sql frame marker
	_, _ = h.Write(sqlLen[:])
	_, _ = h.Write([]byte(sql))
	for _, p := range params {
		payload, err := json.Marshal(struct {
			Type  string `json:"type"`
			Value any    `json:"value"`
		}{
			Type:  fmt.Sprintf("%T", p),
			Value: p,
		})
		if err != nil {
			// Marshal can only fail on unserialisable types (channels,
			// funcs, cyclic structures) that shouldn't reach this path —
			// pipes/structured-query params are scalars from JSON. Fall
			// back to a type+%v rendering so we still produce a key and
			// don't take down the request path.
			payload = fmt.Appendf(nil, "%T:%v", p, p)
		}
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(payload)))
		_, _ = h.Write([]byte{0}) // param frame marker
		_, _ = h.Write(n[:])
		_, _ = h.Write(payload)
	}
	return "query:" + hex.EncodeToString(h.Sum(nil))
}
