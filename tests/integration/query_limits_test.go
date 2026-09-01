//go:build integration

package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
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
// It drives the real StructuredQueryHandler (not the shared admin-stamped
// server, which bypasses all policy) against the package's real ClickHouse, as
// a non-admin `viewer` whose policy carries the cap under test. (Server-wide
// resource backstops are ClickHouse's job — its settings profiles / quotas —
// not WaveHouse's, so there's nothing global to assert here; this proves the
// per-role caps that ARE WaveHouse's to enforce.)
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
		name        string
		perms       policy.SelectPermissions // viewer's per-table select caps
		wantStatus  int
		wantBodyHas string // substring required in the response body
	}{
		{
			// Control: identical query, no resource cap → full result set.
			name:        "no cap returns all rows",
			perms:       policy.SelectPermissions{AllowColumns: []string{"*"}},
			wantStatus:  http.StatusOK,
			wantBodyHas: `"row-24"`,
		},
		{
			// max_rows_to_read bounds rows SCANNED — the lever that stops a
			// full-table scan. A 25-row scan blows past a cap of 1. ClickHouse
			// error code 158 == TOO_MANY_ROWS (the native driver surfaces the
			// numeric code, not the HTTP interface's symbolic suffix).
			name:        "per-role max_rows_to_read is enforced (code 158 TOO_MANY_ROWS)",
			perms:       policy.SelectPermissions{AllowColumns: []string{"*"}, MaxRowsToRead: 1},
			wantStatus:  http.StatusInternalServerError,
			wantBodyHas: "code: 158",
		},
		{
			// max_memory_usage bounds peak query memory — the lever that stops
			// a heavy aggregation from exhausting the box. A 1-byte cap is below
			// the floor any query allocates. Code 241 == MEMORY_LIMIT_EXCEEDED.
			// (ByteSize literal 1 == 1 byte.)
			name:        "per-role max_memory_usage is enforced (code 241 MEMORY_LIMIT_EXCEEDED)",
			perms:       policy.SelectPermissions{AllowColumns: []string{"*"}, MaxMemoryUsage: 1},
			wantStatus:  http.StatusInternalServerError,
			wantBodyHas: "code: 241",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fresh handler + policy per case. Cache is nil so every case
			// actually executes against ClickHouse (no cross-case cache hit
			// masking enforcement). defaultMaxRows 0 falls back to the builder's
			// constant. singleflight's zero value is ready to use.
			store := policy.Static(&policy.Policy{
				AdminRole: "admin",
				Tables: map[string]policy.TablePolicy{
					table: {"viewer": {Select: &tt.perms}},
				},
			})
			h := api.NewStructuredQueryHandler(
				e.chConn, nil, e.registry, store, func() int { return 60 }, func() time.Duration { return 30 * time.Second }, nil, testutil.NopLogger(),
			)

			req := httptest.NewRequest(http.MethodPost,
				"/v1/query?table="+table, strings.NewReader(`{"select_all":true}`))
			req = req.WithContext(auth.WithRole(req.Context(), "viewer"))
			rec := httptest.NewRecorder()

			h.Handle(rec, req)

			body := rec.Body.String()
			require.Equal(t, tt.wantStatus, rec.Code,
				"unexpected status; body: %s", body)
			assert.Contains(t, body, tt.wantBodyHas)
		})
	}
}
