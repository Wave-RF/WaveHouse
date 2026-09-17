//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// TestStructuredQuery_FilterValuesRoundTrip drives every filter value shape
// through the real server, for both the `eq` (scalar `{pN:String}`) and `in`
// (`{pN:Array(String)}`) bindings.
//
// It exists because the two bindings need DIFFERENT encodings and a Go-side
// unit test can only assert what the author believed: a scalar parameter is
// read by ClickHouse's escaped-text reader (an unencoded backslash silently
// becomes an escape sequence, an unencoded tab or newline is a hard code-457
// parse error), while an Array(String) element is read as a quoted literal (a
// raw tab rides through, but a quote or backslash must be escaped). Applying
// either encoding to the other's value is silent data loss, so the server is
// the oracle: seed the value, filter for it, and require exactly the one row.
func TestStructuredQuery_FilterValuesRoundTrip(t *testing.T) {
	e := env(t)
	table := createTable(t, "id String, v String", "ORDER BY id")

	values := map[string]string{
		"plain":            "hello",
		"single-quote":     "it's",
		"backslash":        `a\b`,
		"windows-path":     `C:\Users\x`,
		"tab":              "a\tb",
		"newline":          "a\nb",
		"carriage-return":  "a\rb",
		"literal-bs-n":     `a\nb`,
		"double-backslash": `a\\b`,
		"percent":          "100%",
		"ampersand":        "a&b",
		"unicode":          "héllo→",
		"quote-and-slash":  `it's a\b`,
		"sql-ish":          `') OR 1=1 --`,
		"empty":            "",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for id, v := range values {
		require.NoError(t, e.chConn.Exec(ctx,
			fmt.Sprintf("INSERT INTO `%s` (id, v) VALUES (?, ?)", table), id, v), "seed %q", id)
	}

	store := policy.Static(&policy.Policy{AdminRole: "admin"})
	h := api.NewStructuredQueryHandler(
		func() chconn.Target {
			return chconn.Target{URL: e.chHTTPURL, Username: testCHUser, Password: testCHPassword, Database: testCHDatabase}
		},
		nil, e.registry, store,
		func() int { return 60 },
		func() time.Duration { return 30 * time.Second },
		nil, testutil.NopLogger(),
	)

	query := func(t *testing.T, body string) []map[string]any {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/query?table="+table, strings.NewReader(body))
		req = req.WithContext(auth.WithRole(req.Context(), "admin"))
		rec := httptest.NewRecorder()
		h.Handle(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var rows []map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rows))
		return rows
	}

	for id, v := range values {
		t.Run("eq/"+id, func(t *testing.T) {
			filter, err := json.Marshal(map[string]any{
				"columns": []string{"id"},
				"filters": []any{map[string]any{"column": "v", "op": "eq", "value": v}},
			})
			require.NoError(t, err)
			rows := query(t, string(filter))
			require.Len(t, rows, 1, "eq on %q must match exactly the row that holds it", v)
			require.Equal(t, id, rows[0]["id"])
		})

		t.Run("in/"+id, func(t *testing.T) {
			// A second element that is nowhere in the table keeps the list a
			// real list, so a bad separator would show up as a wrong count.
			filter, err := json.Marshal(map[string]any{
				"columns": []string{"id"},
				"filters": []any{map[string]any{"column": "v", "op": "in", "value": []string{v, "\x00absent\x00"}}},
			})
			require.NoError(t, err)
			rows := query(t, string(filter))
			require.Len(t, rows, 1, "in on %q must match exactly the row that holds it", v)
			require.Equal(t, id, rows[0]["id"])
		})
	}
}
