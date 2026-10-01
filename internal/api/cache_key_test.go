package api

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/assert"
)

// queryCacheKey is consumed by structured_query.go and pipes.go, so its
// contract has to stay stable even though /v1/ops/query no longer caches.
// Table-driven so future collision regressions drop in as additional rows
// without growing the assertion flow.
func TestQueryCacheKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sqlA        string
		paramsA     []string
		sqlB        string
		paramsB     []string
		expectEqual bool
	}{
		{
			name:        "same sql, nil params → deterministic",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     nil,
			expectEqual: true,
		},
		{
			name:        "different sql → distinct",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 2",
			paramsB:     nil,
			expectEqual: false,
		},
		{
			name:        "param presence changes key",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     []string{"a"},
			expectEqual: false,
		},
		{
			name:        "embedded NUL byte does not collide with split params",
			sqlA:        "SELECT 1",
			paramsA:     []string{"foo\x00bar"},
			sqlB:        "SELECT 1",
			paramsB:     []string{"foo", "bar"},
			expectEqual: false,
		},
		{
			// Every value reaches ClickHouse as a String parameter, so the
			// same text is the same query whatever JSON type it arrived as.
			name:        "the same text is the same key",
			sqlA:        "SELECT {p0:String}",
			paramsA:     []string{"42"},
			sqlB:        "SELECT {p0:String}",
			paramsB:     []string{"42"},
			expectEqual: true,
		},
		{
			name:        "nil and empty slice params produce the same key",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     []string{},
			expectEqual: true,
		},
		{
			// Constructed as a collision pair under a "raw sql + framed
			// params" format: the param frame for "y" is 0x00 + the 8-byte
			// big-endian length 1 + "y", so sqlA with no params produces the
			// byte stream ("X", ["y"]) would. The 0x01 marker and length
			// prefix on the sql force them apart.
			name:        "sql crafted to mimic a param-frame stream does not collide with shorter sql + real param",
			sqlA:        "X\x00\x00\x00\x00\x00\x00\x00\x00\x01y",
			paramsA:     nil,
			sqlB:        "X",
			paramsB:     []string{"y"},
			expectEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := queryCacheKey(tenant.Default, tt.sqlA, tt.paramsA)
			b := queryCacheKey(tenant.Default, tt.sqlB, tt.paramsB)
			if tt.expectEqual {
				assert.Equal(t, a, b)
			} else {
				assert.NotEqual(t, a, b)
			}
		})
	}
}

// The tenant leads the key in the clear, so a key is tenant-keyed by
// construction rather than by what went into the hash: identical SQL and
// params under two tenants are two keys that differ in the prefix alone, and
// the flat directory's tenant 0 simply gains its "0".
func TestQueryCacheKey_LeadsWithTheTenant(t *testing.T) {
	t.Parallel()
	acme := queryCacheKey("acme", "SELECT 1", []string{"a"})
	globex := queryCacheKey("globex", "SELECT 1", []string{"a"})
	assert.True(t, strings.HasPrefix(acme, "acme:query:"), acme)
	assert.True(t, strings.HasPrefix(globex, "globex:query:"), globex)
	assert.NotEqual(t, acme, globex)
	assert.Equal(t, strings.TrimPrefix(acme, "acme"), strings.TrimPrefix(globex, "globex"),
		"the tenant is a prefix, not an input to the hash")
	assert.True(t, strings.HasPrefix(queryCacheKey(tenant.Default, "SELECT 1", nil), "0:query:"))
}

// The rendering contract opens the hash: a build that renders rows another
// way (chRendering) keys every entry apart from this one's, including a pipe
// or a query with no parameters, whose SQL is the same under both builds.
func TestQueryCacheKey_LeadsWithTheRendering(t *testing.T) {
	t.Parallel()
	h := sha256.New()
	writeFrame(h, 2, chRendering)
	writeFrame(h, 1, "SELECT 1")
	assert.Equal(t, "0:query:"+hex.EncodeToString(h.Sum(nil)), queryCacheKey(tenant.Default, "SELECT 1", nil))

	other := sha256.New()
	writeFrame(other, 2, "JSON/0")
	writeFrame(other, 1, "SELECT 1")
	assert.NotEqual(t, "0:query:"+hex.EncodeToString(other.Sum(nil)), queryCacheKey(tenant.Default, "SELECT 1", nil))
}
