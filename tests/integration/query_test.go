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

// `/v1/admin/query` is the only sanctioned surface for non-insert mutations,
// and a mutation must return HTTP 200 with an empty JSON array (`[]`)
// — not HTTP 500. We insert a row, observe it land, TRUNCATE through
// `/v1/admin/query`, and then observe the row gone.
//
// Before the fix `executeQuery` routed every statement through
// `driver.Query`, which on a TRUNCATE surfaced as "internal driver error"
// → HTTP 500. The handler now classifies by the leading SQL verb and
// routes mutations through `driver.Exec`, marshalling `[]` on success.
func TestQuery_TruncateReturnsEmptyArray(t *testing.T) {
	e := env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := createTable(t,
		"id String, page String",
		"ORDER BY id",
	)
	const rowID = "row-to-truncate"

	// Land one row so we can prove TRUNCATE actually emptied the table —
	// otherwise a no-op "TRUNCATE on empty table returns []" assertion
	// would also pass and we'd be hiding the bug.
	body := fmt.Sprintf(`{"id":%q,"page":"/about"}`, rowID)
	resp, err := http.Post(
		e.server.URL+"/v1/ingest/"+table,
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
			rowID,
		).Scan(&count)
		return err == nil && count == 1
	}, 30*time.Second, 500*time.Millisecond, "insert should land before TRUNCATE")

	// Now TRUNCATE through /v1/admin/query. Before, this would have
	// returned 500; now it must return 200 with `[]`.
	truncateBody, _ := json.Marshal(map[string]string{
		"sql": fmt.Sprintf("TRUNCATE TABLE `%s`", table),
	})
	qResp, err := http.Post(
		e.server.URL+"/v1/admin/query",
		"application/json",
		bytes.NewReader(truncateBody),
	)
	require.NoError(t, err)
	defer qResp.Body.Close()

	respBytes, err := io.ReadAll(qResp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, qResp.StatusCode,
		"mutations through /v1/admin/query must return 200, not 500; body: %s", respBytes)
	assert.JSONEq(t, "[]", string(respBytes),
		"mutations through /v1/admin/query must marshal to [] (empty result set), not null or {}")

	// And confirm TRUNCATE was actually executed — not just that the
	// handler swallowed the statement and returned [] from nowhere.
	require.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s`", table),
		).Scan(&count)
		return err == nil && count == 0
	}, 10*time.Second, 200*time.Millisecond, "TRUNCATE must remove the row from ClickHouse")
}

// TestQuery_DeleteReturnsEmptyArray covers the predicate-driven DELETE case
// — the canonical example of why we routed non-insert mutations through
// `/v1/admin/query` instead of trying to teach the policy engine to authorize
// WHERE predicates. The response shape contract is the same as TRUNCATE:
// HTTP 200 with `[]`. ClickHouse lightweight DELETE marks the matching
// rows invisible synchronously, so we can poll for the row to disappear.
func TestQuery_DeleteReturnsEmptyArray(t *testing.T) {
	e := env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := createTable(t,
		"id String, page String",
		"ORDER BY id",
	)
	const keepID = "row-to-keep"
	const dropID = "row-to-drop"

	for _, id := range []string{keepID, dropID} {
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

	require.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s`", table),
		).Scan(&count)
		return err == nil && count == 2
	}, 30*time.Second, 500*time.Millisecond, "both inserts should land before DELETE")

	deleteBody, _ := json.Marshal(map[string]any{
		"sql":    fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", table),
		"params": []any{dropID},
	})
	qResp, err := http.Post(
		e.server.URL+"/v1/admin/query",
		"application/json",
		bytes.NewReader(deleteBody),
	)
	require.NoError(t, err)
	defer qResp.Body.Close()

	respBytes, err := io.ReadAll(qResp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, qResp.StatusCode, "body: %s", respBytes)
	assert.JSONEq(t, "[]", string(respBytes))

	// The unparameterized row stays; the parameterized one goes.
	require.Eventually(t, func() bool {
		var kept, dropped uint64
		if err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
			keepID,
		).Scan(&kept); err != nil {
			return false
		}
		if err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
			dropID,
		).Scan(&dropped); err != nil {
			return false
		}
		return kept == 1 && dropped == 0
	}, 30*time.Second, 500*time.Millisecond,
		"DELETE through /v1/admin/query must mutate only the targeted row")
}
