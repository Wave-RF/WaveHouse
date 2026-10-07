//go:build integration

package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// TestStructuredQuery_ResourceCapsEnforcedServerSide is the executable proof
// for #316: a role's policy resource caps reach ClickHouse as per-query
// settings and are enforced SERVER-SIDE, so a structured read can't outrun its
// budget during a scan / aggregation phase. Before the fix the handler sent no
// Settings, so every case below returned 200 with the full result set — the
// caps were latent. The control case ("no cap") shares the exact query shape,
// so a rejection in the capped cases is attributable to the cap, not a broken
// query.
//
// It runs through the production /v1/query, as roles whose policy carries the
// cap under test, with tokens for them. (Server-wide resource backstops are
// ClickHouse's job — its settings profiles / quotas — not WaveHouse's, so
// there's nothing global to assert here; this proves the per-role caps that
// ARE WaveHouse's to enforce.)
func TestStructuredQuery_ResourceCapsEnforcedServerSide(t *testing.T) {
	e := env(t)

	// A handful of rows: enough that a max_rows_to_read=1 cap is exceeded by a
	// full scan, small enough that the uncapped control returns them all.
	const seededRows = 25
	table := createTable(t, "id String, page String, n UInt32", "ORDER BY id")
	seedCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < seededRows; i++ {
		require.NoError(t, e.chConn.Exec(seedCtx,
			fmt.Sprintf("INSERT INTO `%s` (id, page, n) VALUES (?, ?, ?)", table),
			fmt.Sprintf("row-%02d", i), "/p", uint32(i),
		), "seed row")
	}

	tests := []struct {
		role        string
		perms       policy.SelectPermissions // the role's per-table select caps
		wantBodyHas string                   // substring required in the response body
	}{
		// Control: identical query, no resource cap → full result set.
		{role: "uncapped", perms: policy.SelectPermissions{AllowColumns: []string{"*"}}, wantBodyHas: `"row-24"`},
		// max_rows_to_read bounds rows SCANNED — the lever that stops a
		// full-table scan. A 25-row scan blows past a cap of 1. ClickHouse
		// error code 158 == TOO_MANY_ROWS.
		{role: "rows_capped", perms: policy.SelectPermissions{AllowColumns: []string{"*"}, MaxRowsToRead: 1}, wantBodyHas: "code: 158"},
		// max_memory_usage bounds peak query memory — the lever that stops a
		// heavy aggregation from exhausting the box. A 1-byte cap is below the
		// floor any query allocates. Code 241 == MEMORY_LIMIT_EXCEEDED.
		{role: "memory_capped", perms: policy.SelectPermissions{AllowColumns: []string{"*"}, MaxMemoryUsage: 1}, wantBodyHas: "code: 241"},
	}
	grants := policy.TablePolicy{}
	for _, tt := range tests {
		grants[tt.role] = policy.RolePermissions{Select: &tt.perms}
	}
	withPolicy(t, policy.Policy{Tables: map[string]policy.TablePolicy{table: grants}})

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			// A filter per role keeps the query text distinct, so no case is
			// answered from the cache another case filled.
			body := fmt.Sprintf(`{"select_all":true,"filters":[{"column":"page","op":"neq","value":%q}]}`, tt.role)
			got := postJSONAs(t, e.baseURL+"/v1/query?table="+table, body, bearer(t, tt.role, nil))
			if tt.role == "uncapped" {
				require.Equal(t, 200, got.status, got.Error)
				assert.Contains(t, got.raw, tt.wantBodyHas)
				return
			}
			// The role's own cap: the caller's, and not retried. ClickHouse's
			// message spells the code "Code: N." over HTTP.
			assertQueryError(t, got, 400, "clickhouse.limit_exceeded", false)
			assert.Contains(t, strings.ToLower(got.Error), tt.wantBodyHas)
		})
	}
}
