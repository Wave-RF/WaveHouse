//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/testutil/mutationtest"
)

// codeSyntaxError is ClickHouse's SYNTAX_ERROR.
const codeSyntaxError = 62

// astMutation is api.IsMutation's answer for each statement kind astRoot
// names.
var astMutation = map[string]bool{
	// A bare EXECUTE AS: it switches the session's user and returns no
	// result set.
	"ExecuteAsQuery":       true,
	"SelectWithUnionQuery": false,
	"ShowTables":           false,
	"DescribeQuery":        false,
	"Explain":              false,
	"ExistsTableQuery":     false,
	"InsertQuery":          true,
	"UpdateQuery":          true,
	"DeleteQuery":          true,
	"TruncateQuery":        true,
	"DropQuery":            true,
	"AlterQuery":           true,
	"CreateQuery":          true,
	"Rename":               true,
	"OptimizeQuery":        true,
	"GrantQuery":           true,
	"SYSTEM":               true,
	"AttachQuery":          true,
	"DetachQuery":          true,
	"KillQueryQuery":       true,
	"Set":                  true,
	"UseQuery":             true,
}

// astRoot is the statement kind ClickHouse's parser gives sql — the root node
// of its tree, or for an EXECUTE AS that leads a statement, that statement's
// — or the error it rejects sql with. Parsing only: nothing runs, and no table
// need exist.
func astRoot(ctx context.Context, conn driver.Conn, sql string) (string, error) {
	rows, err := conn.Query(ctx, "EXPLAIN AST "+sql)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(lines) == 0 {
		return "", errors.New("EXPLAIN AST returned no rows")
	}
	root := strings.Fields(lines[0])
	if len(root) == 0 {
		return "", fmt.Errorf("EXPLAIN AST returned %q", lines[0])
	}
	if root[0] != "ExecuteAsQuery" {
		return root[0], nil
	}
	// Its children, indented one space: the user, then any statement.
	var children []string
	for _, line := range lines[1:] {
		if strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "  ") {
			children = append(children, strings.Fields(line)[0])
		}
	}
	if len(children) < 2 {
		return root[0], nil
	}
	return children[1], nil
}

func isSyntaxError(err error) bool {
	code, ok := chconn.ExceptionCode(err)
	return ok && code == codeSyntaxError
}

// TestIsMutation_AgreesWithClickHouseParser checks every shared IsMutation
// case against the parser of the pinned ClickHouse: a case that parses is
// classified as its statement kind, and one marked unparsed still fails to.
func TestIsMutation_AgreesWithClickHouseParser(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	for _, tc := range mutationtest.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			root, err := astRoot(ctx, e.chConn, tc.SQL)
			if tc.Unparsed {
				require.Error(t, err, "ClickHouse now parses this case as %s: set Mutation to match and drop Unparsed", root)
				assert.True(t, isSyntaxError(err), "want a syntax error, got %v", err)
				return
			}
			require.NoError(t, err)
			want, known := astMutation[root]
			require.True(t, known, "no IsMutation answer for statement kind %s: add it to astMutation", root)
			assert.Equal(t, want, tc.Mutation, "ClickHouse parses this as %s", root)
		})
	}
}

// TestIsMutation_KeywordNamesInWithList spells a WITH list's names — a CTE,
// an alias, a function, a lambda parameter, an operand, a qualified name's
// part, an array element, a bare element — as every ClickHouse keyword, ahead
// of each statement a WITH list can lead, and checks IsMutation against the
// parser on each combination that parses.
func TestIsMutation_KeywordNamesInWithList(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	rows, err := e.chConn.Query(ctx, "SELECT keyword FROM system.keywords")
	require.NoError(t, err)
	seen := map[string]bool{}
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		for _, w := range strings.Fields(k) {
			seen[strings.ToLower(w)] = true
		}
	}
	require.NoError(t, rows.Err())
	_ = rows.Close()
	words := make([]string, 0, len(seen))
	for w := range seen {
		words = append(words, w)
	}
	sort.Strings(words)
	require.NotEmpty(t, words)

	shapes := []string{
		"WITH %s AS (SELECT 1 AS x) %s",
		"WITH 1 AS a, %s AS (SELECT 1) %s",
		"WITH 1 AS %s %s",
		"WITH (SELECT 1) AS a, 2 AS %s %s",
		"WITH %s AS y %s",
		"WITH %s(1) AS y %s",
		"WITH %s -> 1 AS f %s",
		"WITH 1 + %s AS y %s",
		"WITH %s + 1 AS y %s",
		"WITH t.%s AS y %s",
		"WITH [%s] AS a %s",
		"WITH %s %s",
	}
	statements := []string{"SELECT 1", "FROM system.one SELECT 1", "INSERT INTO t SELECT 1"}
	queries := make(chan string)
	go func() {
		defer close(queries)
		for _, w := range words {
			for _, shape := range shapes {
				for _, stmt := range statements {
					queries <- fmt.Sprintf(shape, w, stmt)
				}
			}
		}
	}()

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		parsed   int
		failures []string
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sql := range queries {
				root, err := astRoot(ctx, e.chConn, sql)
				var failure string
				switch want, known := astMutation[root]; {
				case err != nil && !isSyntaxError(err):
					failure = fmt.Sprintf("%q: %v", sql, err)
				case err != nil:
				case !known:
					failure = fmt.Sprintf("%q: no IsMutation answer for statement kind %s", sql, root)
				case api.IsMutation(sql) != want:
					failure = fmt.Sprintf("%q: ClickHouse parses it as %s, IsMutation says %v", sql, root, !want)
				}
				mu.Lock()
				if err == nil {
					parsed++
				}
				if failure != "" {
					failures = append(failures, failure)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	sort.Strings(failures)
	for _, f := range failures {
		t.Error(f)
	}
	// Most combinations parse; far fewer means the check stopped checking.
	assert.Greater(t, parsed, len(words)*len(shapes)*len(statements)/2)
}
