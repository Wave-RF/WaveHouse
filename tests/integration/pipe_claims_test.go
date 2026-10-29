//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// pipeClaimsTable is the table the tenant_totals pipe in the shared settings
// (writeTestSettings) reads. The pipe's SQL names it, so it is a fixed name
// this test creates, not one generated per test.
const pipeClaimsTable = "it_pipe_claims_orders"

// TestPipe_ClaimBindsOnNumericColumn (#938) pins, against ClickHouse itself,
// what the unit tests can only assert on the SQL text: a claim binds as a
// quoted literal that the column's type reads — a token's tenant_ids array,
// JSON numbers or strings, scopes a UInt64 column — a request parameter
// spelling the claim's name changes nothing, and a token that cannot bind
// the claim (none, so the default_role admin; a member without it; an empty
// list) answers no rows without an error.
func TestPipe_ClaimBindsOnNumericColumn(t *testing.T) {
	t.Parallel()
	e := env(t)
	ctx := context.Background()
	require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (tenant_id UInt64, amount UInt32) ENGINE = MergeTree() ORDER BY tenant_id", pipeClaimsTable)))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.chConn.Exec(dropCtx, "DROP TABLE IF EXISTS "+pipeClaimsTable)
	})
	require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 10), (1, 5), (2, 7), (3, 1)", pipeClaimsTable)))

	// call runs the pipe as the token's bearer (none when claims is nil) with
	// body as the parameters and returns the status and the rows, tenant → total.
	call := func(t *testing.T, claims map[string]any, body string) (int, map[float64]float64) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/v1/pipes/tenant_totals", strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if claims != nil {
			req.Header.Set("Authorization", "Bearer "+testutil.MakeJWT(t, claims))
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal(raw, &rows), "body: %s", raw)
		totals := make(map[float64]float64, len(rows))
		for _, row := range rows {
			totals[row["tenant_id"].(float64)] = row["total"].(float64)
		}
		return resp.StatusCode, totals
	}
	member := func(tenantIDs ...any) map[string]any {
		if tenantIDs == nil {
			tenantIDs = []any{} // an empty list in the token, not null
		}
		return map[string]any{"role": "member", "app_metadata": map[string]any{"tenant_ids": tenantIDs}}
	}

	tests := []struct {
		name   string
		claims map[string]any
		body   string
		want   map[float64]float64
	}{
		{"numbers scope the UInt64 column", member(1, 3), "{}", map[float64]float64{1: 15, 3: 1}},
		{"strings scope it too", member("2"), "{}", map[float64]float64{2: 7}},
		{"a request parameter cannot widen the set", member(1), `{"jwt.app_metadata.tenant_ids": [2, 3], "tenant_ids": [2, 3]}`, map[float64]float64{1: 15}},
		{"no token is the default_role admin with no claims", nil, "{}", map[float64]float64{}},
		{"member without the claim", map[string]any{"role": "member"}, "{}", map[float64]float64{}},
		{"member with an empty list", member(), "{}", map[float64]float64{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, totals := call(t, tt.claims, tt.body)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, tt.want, totals)
		})
	}
}
