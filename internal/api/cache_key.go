package api

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// queryCacheKey produces a deterministic L1/L2 cache key for a (sql, params)
// pair of one tenant: "<tenant>:query:<sha256>". The raw-SQL endpoint
// (`POST /v1/ops/query`) does not cache, but the structured query
// (`POST /v1/query?table={table}`) and named pipes (`GET/POST /v1/pipes/{name}`)
// handlers do — they share this helper so a key change in one place propagates
// to every cached read path, and each uses the key it returns as its
// singleflight key too.
//
// The tenant leads the key in the clear rather than going into the hash, so
// a key is tenant-keyed by construction and readable as such (#583 story 8):
// identical SQL and params under two tenants are two entries and two flights,
// and a pipe — whose result carries no dependency namespaces — can never
// answer one tenant with another's rows now that each tenant reads its own
// ClickHouse (story 6).
//
// The hash opens with chRendering, the response contract the cached bytes
// were rendered under, so a build that renders rows differently never serves
// another's entries from a shared cache — even for a pipe or a query without
// parameters, whose SQL is the same under both.
//
// params are the ClickHouse query-parameter values, positionally, exactly as
// they go on the wire: already rendered as text and encoded for ClickHouse's
// parameter reader. Two reads whose parameters reach ClickHouse as the same
// bytes are the same query, and two that do not are not.
//
// Every section is framed with a 1-byte type marker (0x02 for the rendering,
// 0x01 for sql, 0x00 for a param) plus an 8-byte big-endian length, then the
// payload. Without length-prefixing the sql itself, a SQL string crafted to
// end with the exact bytes of a param frame (`\x00` + 8 length bytes +
// payload) would hash identically to a shorter SQL plus a real param —
// distinct `(sql, params)` tuples, same digest. Per-param framing keeps its
// own length prefix so an embedded `\x00` inside a value can't be confused
// for a frame boundary.
func queryCacheKey(id tenant.ID, sql string, params []string) string {
	h := sha256.New()
	writeFrame(h, 2, chRendering)
	writeFrame(h, 1, sql)
	for _, p := range params {
		writeFrame(h, 0, p)
	}
	return id.String() + ":query:" + hex.EncodeToString(h.Sum(nil))
}

func writeFrame(h hash.Hash, marker byte, payload string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(payload)))
	_, _ = h.Write([]byte{marker})
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(payload))
}
