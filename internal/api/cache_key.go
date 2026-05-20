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
// structured query (`POST /v1/tables/{table}/query`) and named pipes
// (`GET/POST /v1/pipes/{name}`) handlers do — they share this helper so a
// key change in one place propagates to every cached read path.
//
// Each param is framed as (8-byte length prefix, JSON payload carrying both
// Go type and value). Length-prefixing prevents two distinct inputs from
// hashing the same — e.g. `["foo\x00bar"]` versus `["foo", "bar"]` would
// have collided under a `"\x00%v"`-separated format because `\x00` is a
// legal byte inside a string param. Type-tagging additionally separates
// `"42"` (string) from `42` (int) so the cache can't serve a string-typed
// row to an int-typed lookup.
func queryCacheKey(sql string, params []any) string {
	h := sha256.New()
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
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(n[:])
		_, _ = h.Write(payload)
	}
	return "query:" + hex.EncodeToString(h.Sum(nil))
}
