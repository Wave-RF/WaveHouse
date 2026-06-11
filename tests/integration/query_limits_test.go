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

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
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
// a non-admin `viewer` whose policy carries the cap under test.
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
		perms       policy.RolePermissions // viewer's per-table select caps
		limits      api.QueryLimits        // server-wide query_limits defaults
		role        string                 // request role; defaults to "viewer"
		wantStatus  int
		wantBodyHas string // substring required in the response body
	}{
		{
			// Control: identical query, no resource cap → full result set.
			name:        "no cap returns all rows",
			perms:       policy.RolePermissions{AllowColumns: []string{"*"}},
			wantStatus:  http.StatusOK,
			wantBodyHas: `"row-24"`,
		},
		{
			// max_rows_to_read bounds rows SCANNED — the lever that stops a
			// full-table scan. A 25-row scan blows past a cap of 1. ClickHouse
			// error code 158 == TOO_MANY_ROWS (the native driver surfaces the
			// numeric code, not the HTTP interface's symbolic suffix).
			name:        "per-role max_rows_to_read is enforced (code 158 TOO_MANY_ROWS)",
			perms:       policy.RolePermissions{AllowColumns: []string{"*"}, MaxRowsToRead: 1},
			wantStatus:  http.StatusInternalServerError,
			wantBodyHas: "code: 158",
		},
		{
			// max_memory_usage bounds peak query memory — the lever that stops
			// a heavy aggregation from exhausting the box. A 1-byte cap is below
			// the floor any query allocates. Code 241 == MEMORY_LIMIT_EXCEEDED.
			name:        "per-role max_memory_usage is enforced (code 241 MEMORY_LIMIT_EXCEEDED)",
			perms:       policy.RolePermissions{AllowColumns: []string{"*"}, MaxMemoryUsage: 1},
			wantStatus:  http.StatusInternalServerError,
			wantBodyHas: "code: 241",
		},
		{
			// The server-wide default applies to a non-admin read even with no
			// per-role cap set — the safety net for un-tuned tables.
			name:        "global default_max_memory_usage applies to a non-admin read",
			perms:       policy.RolePermissions{AllowColumns: []string{"*"}},
			limits:      api.QueryLimits{DefaultMaxMemoryBytes: 1},
			wantStatus:  http.StatusInternalServerError,
			wantBodyHas: "code: 241",
		},
		{
			// Admin bypasses the global default — same impossibly-small budget,
			// but the read runs unbounded and returns the full set.
			name:        "admin bypasses the global default",
			limits:      api.QueryLimits{DefaultMaxMemoryBytes: 1},
			role:        "admin",
			wantStatus:  http.StatusOK,
			wantBodyHas: `"row-24"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fresh handler + policy per case. Cache is nil so every case
			// actually executes against ClickHouse (no cross-case cache hit
			// masking enforcement). singleflight's zero value is ready to use.
			store := policy.NewMemoryStore(&policy.Policy{
				AdminRole: "admin",
				Tables: map[string]policy.TablePolicy{
					table: {Select: map[string]policy.RolePermissions{"viewer": tt.perms}},
				},
			})
			h := api.NewStructuredQueryHandler(
				e.chConn, nil, e.registry, store, 60, 30*time.Second, tt.limits, testutil.NopLogger(),
			)

			role := tt.role
			if role == "" {
				role = "viewer"
			}
			req := httptest.NewRequest(http.MethodPost,
				"/v1/query?table="+table, strings.NewReader(`{"select_all":true}`))
			req = req.WithContext(auth.WithRole(req.Context(), role))
			rec := httptest.NewRecorder()

			h.Handle(rec, req)

			body := rec.Body.String()
			require.Equal(t, tt.wantStatus, rec.Code,
				"unexpected status; body: %s", body)
			assert.Contains(t, body, tt.wantBodyHas)
		})
	}
}

// TestPipe_GlobalDefaultsEnforcedServerSide proves the named-pipe read path
// also runs under the server-wide query_limits defaults (#316): a pipe carries
// no per-role caps, so a non-admin pipe read inherits the global default
// rows-scanned / memory budget, while admin bypasses it. Same query shape
// across both cases, so the rejection is attributable to the budget.
func TestPipe_GlobalDefaultsEnforcedServerSide(t *testing.T) {
	e := env(t)

	table := createTable(t, "id String, page String", "ORDER BY id")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 25; i++ {
		require.NoError(t, e.chConn.Exec(ctx,
			fmt.Sprintf("INSERT INTO `%s` (id, page) VALUES (?, ?)", table),
			fmt.Sprintf("row-%02d", i), "/p",
		), "seed row")
	}

	store, err := pipes.NewStore(ctx, e.embeddedMQ.JetStream(), "", testutil.NopLogger())
	require.NoError(t, err)
	const pipeName = "qlimits_scan"
	require.NoError(t, store.Put(ctx, &pipes.NamedQuery{
		Name:         pipeName,
		SQL:          fmt.Sprintf("SELECT * FROM `%s`", table),
		AllowedRoles: []string{"viewer"},
	}))

	policyStore := policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"})

	tests := []struct {
		name       string
		role       string
		wantStatus int
	}{
		// A 1-row scan budget rejects the 25-row pipe scan server-side (code 158).
		{name: "non-admin pipe inherits the global default", role: "viewer", wantStatus: http.StatusInternalServerError},
		// Admin bypasses the default and gets the full result set.
		{name: "admin pipe bypasses the global default", role: "admin", wantStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := api.NewPipesHandler(store, policyStore, e.chConn, nil, 30*time.Second,
				api.QueryLimits{DefaultMaxRowsToRead: 1}, testutil.NopLogger())

			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("name", pipeName)
			req := httptest.NewRequest(http.MethodGet, "/v1/pipes/"+pipeName, nil)
			req = req.WithContext(context.WithValue(
				auth.WithRole(req.Context(), tt.role), chi.RouteCtxKey, rctx))
			rec := httptest.NewRecorder()

			h.Execute(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
			if tt.wantStatus == http.StatusInternalServerError {
				assert.Contains(t, rec.Body.String(), "code: 158")
			}
		})
	}
}
