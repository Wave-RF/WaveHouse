//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStructuredQuery_FilterValuesRoundTrip drives every filter value shape
// through the production /v1/query, for both the `eq` (scalar `{pN:String}`)
// and `in` (an external table of one String column, sent as RowBinary)
// bindings.
//
// It exists because the two bindings carry a value DIFFERENTLY and a Go-side
// unit test can only assert what the author believed: a scalar parameter is
// read by ClickHouse's escaped-text reader (an unencoded backslash silently
// becomes an escape sequence, an unencoded tab or newline is a hard code-457
// parse error), while a RowBinary row carries each element's bytes as they
// are, after their length. Applying the scalar's encoding to an element, or
// leaving it off a scalar, is silent data loss, so the server is the oracle:
// seed the value, filter for it, and require exactly the one row.
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

	query := func(t *testing.T, filter map[string]any) []map[string]any {
		t.Helper()
		filter["columns"] = []string{"id"}
		body, err := json.Marshal(filter)
		require.NoError(t, err)
		got := postJSON(t, e.baseURL+"/v1/query?table="+table, string(body))
		require.Equal(t, http.StatusOK, got.status, "body: %s", got.raw)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal([]byte(got.raw), &rows))
		return rows
	}

	for id, v := range values {
		t.Run("eq/"+id, func(t *testing.T) {
			rows := query(t, map[string]any{"filters": []any{map[string]any{"column": "v", "op": "eq", "value": v}}})
			require.Len(t, rows, 1, "eq on %q must match exactly the row that holds it", v)
			require.Equal(t, id, rows[0]["id"])
		})

		t.Run("in/"+id, func(t *testing.T) {
			// A second element that is nowhere in the table keeps the list a
			// real list, so a bad separator would show up as a wrong count.
			rows := query(t, map[string]any{"filters": []any{map[string]any{"column": "v", "op": "in", "value": []string{v, "\x00absent\x00"}}}})
			require.Len(t, rows, 1, "in on %q must match exactly the row that holds it", v)
			require.Equal(t, id, rows[0]["id"])
		})
	}
}
