//go:build integration

package tests

import (
	"bytes"
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
)

// TestQuery_MutationsReturnEmptyArray pins the response-shape contract for
// non-insert mutations through `/v1/admin/query`: HTTP 200 with body `[]`,
// never HTTP 500 or `null`. The endpoint proxies SQL to ClickHouse's HTTP
// interface, which returns no body for mutations; WaveHouse normalises that
// to the JSON empty-array shape callers can do `result.length` on.
//
// Table-driven because every new sanctioned mutation form (TRUNCATE, DELETE,
// future ALTER/UPDATE/...) ought to be a row, not a duplicated test. Each
// case provides its own row preset, mutation SQL, and post-condition check
// that confirms the mutation actually ran in ClickHouse (not just that
// WaveHouse swallowed the statement and returned `[]` from nowhere).
func TestQuery_MutationsReturnEmptyArray(t *testing.T) {
	tests := []struct {
		name        string
		rowIDs      []string                                                          // IDs to seed via /v1/ingest/{table} before the mutation
		mutationSQL func(table string) string                                         // SQL to POST to /v1/admin/query
		postCheck   func(t *testing.T, ctx context.Context, e *testEnv, table string) // verify the mutation actually ran
	}{
		{
			name:   "TRUNCATE empties the table",
			rowIDs: []string{"row-to-truncate"},
			mutationSQL: func(table string) string {
				return fmt.Sprintf("TRUNCATE TABLE `%s`", table)
			},
			postCheck: func(t *testing.T, ctx context.Context, e *testEnv, table string) {
				require.Eventually(t, func() bool {
					var count uint64
					err := e.chConn.QueryRow(ctx,
						fmt.Sprintf("SELECT count() FROM `%s`", table),
					).Scan(&count)
					return err == nil && count == 0
				}, 10*time.Second, 200*time.Millisecond, "TRUNCATE must remove the row from ClickHouse")
			},
		},
		{
			// Predicate-driven DELETE — the canonical example of why
			// non-insert mutations route through /v1/admin/query rather
			// than the policy-authorized ingest path: we can't prove a
			// WHERE predicate matches only rows the caller is allowed
			// to touch. ClickHouse lightweight DELETE marks matching
			// rows invisible synchronously, so the post-check can poll
			// for the row to disappear.
			//
			// /v1/admin/query proxies to ClickHouse's HTTP interface
			// (named-param syntax, not positional `?`), so the dropID
			// is inlined into the SQL literal — test-controlled, safe.
			name:   "DELETE removes only the targeted row",
			rowIDs: []string{"row-to-keep", "row-to-drop"},
			mutationSQL: func(table string) string {
				return fmt.Sprintf("DELETE FROM `%s` WHERE id = 'row-to-drop'", table)
			},
			postCheck: func(t *testing.T, ctx context.Context, e *testEnv, table string) {
				require.Eventually(t, func() bool {
					var kept, dropped uint64
					if err := e.chConn.QueryRow(ctx,
						fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
						"row-to-keep",
					).Scan(&kept); err != nil {
						return false
					}
					if err := e.chConn.QueryRow(ctx,
						fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
						"row-to-drop",
					).Scan(&dropped); err != nil {
						return false
					}
					return kept == 1 && dropped == 0
				}, 30*time.Second, 500*time.Millisecond,
					"DELETE through /v1/admin/query must mutate only the targeted row")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := env(t)

			table := createTable(t,
				"id String, page String",
				"ORDER BY id",
			)

			// Seed the table via /v1/ingest/{table}. We seed at least one
			// row so the post-check can prove the mutation actually ran —
			// otherwise a no-op "mutation on empty table returns []"
			// assertion would also pass against a regression that swallows
			// the statement.
			for _, id := range tt.rowIDs {
				body := fmt.Sprintf(`{"id":%q,"page":"/about"}`, id)
				resp, err := http.Post(
					e.server.URL+"/v1/ingest/"+table,
					"application/json",
					strings.NewReader(body),
				)
				require.NoError(t, err)
				resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			// Separate timeout contexts per Eventually phase. A single
			// shared 60s ctx would let a slow seed-ingest convergence
			// burn the budget and cause the post-mutation Eventually to
			// fail on `context deadline exceeded` rather than the
			// behavior under test — non-deterministic, hard to diagnose.
			wantSeeded := uint64(len(tt.rowIDs))
			seedCtx, seedCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer seedCancel()
			require.Eventually(t, func() bool {
				var count uint64
				err := e.chConn.QueryRow(seedCtx,
					fmt.Sprintf("SELECT count() FROM `%s`", table),
				).Scan(&count)
				return err == nil && count == wantSeeded
			}, 30*time.Second, 500*time.Millisecond, "all seed inserts should land before the mutation")

			mutationBody, _ := json.Marshal(map[string]string{"sql": tt.mutationSQL(table)})
			qResp, err := http.Post(
				e.server.URL+"/v1/admin/query",
				"application/json",
				bytes.NewReader(mutationBody),
			)
			require.NoError(t, err)
			defer qResp.Body.Close()

			respBytes, err := io.ReadAll(qResp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, qResp.StatusCode,
				"mutations through /v1/admin/query must return 200, not 500; body: %s", respBytes)
			assert.JSONEq(t, "[]", string(respBytes),
				"mutations through /v1/admin/query must marshal to [] (empty result set), not null or {}")

			postCtx, postCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer postCancel()
			tt.postCheck(t, postCtx, e, table)
		})
	}
}
