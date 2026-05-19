package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// queryCacheKey produces a deterministic L1/L2 cache key for a (sql, params)
// pair. The raw-SQL endpoint (`POST /v1/admin/query`) does not cache, but the
// structured query (`POST /v1/tables/{table}/query`) and named pipes
// (`GET/POST /v1/pipes/{name}`) handlers do — they share this helper so a
// key change in one place propagates to every cached read path.
//
// `\x00` separates the sql from each param byte stream so that
// `("SELECT 1", ["foo"])` and `("SELECT 1foo", [])` cannot collide.
func queryCacheKey(sql string, params []any) string {
	h := sha256.New()
	h.Write([]byte(sql))
	for _, p := range params {
		_, _ = fmt.Fprintf(h, "\x00%v", p)
	}
	return "query:" + hex.EncodeToString(h.Sum(nil))
}
