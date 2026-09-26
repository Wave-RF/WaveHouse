package api

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/google/uuid"
)

// connOf is conn's answer for store — the tenant's pool — and nil for an
// unwired source or a tenant on no pool. The nil is untyped: a nil *Manager
// inside a non-nil driver.Conn would pass a nil check and panic on use.
func connOf(conn func(*settings.Store) driver.Conn, store *settings.Store) driver.Conn {
	if conn == nil {
		return nil
	}
	return conn(store)
}

// timeoutOf is timeout's answer for store, zero for an unwired source.
func timeoutOf(timeout func(*settings.Store) time.Duration, store *settings.Store) time.Duration {
	if timeout == nil {
		return 0
	}
	return timeout(store)
}

// executeCHQuery runs sql against the native-protocol driver conn,
// classifying by leading SQL verb to pick the Exec-vs-Query path —
// clickhouse-go's driver.Query() errors on statements that return no
// result set, so the dispatch is correctness, not optimisation. Returns
// a row-array suitable for JSON marshalling; mutations marshal to `[]`,
// preserving the "always-an-array" response shape callers depend on.
//
// Used by the structured-query and pipes handlers — those are the cached
// read paths that need explicit Query/Exec dispatch and per-row scanning.
// The raw-SQL endpoint (/v1/ops/query) proxies straight to ClickHouse
// over HTTP and never calls this; see internal/api/query.go.
func executeCHQuery(ctx context.Context, conn driver.Conn, sql string, params []any) ([]map[string]any, error) {
	if IsMutation(sql) {
		if err := conn.Exec(ctx, sql, params...); err != nil {
			return nil, fmt.Errorf("clickhouse exec: %w", err)
		}
		return []map[string]any{}, nil
	}

	rows, err := conn.Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns := rows.ColumnTypes()
	// Initialize as empty (not nil) so a zero-row result marshals to `[]`,
	// not `null`. SDK consumers do `data!.length` on the response; a `null`
	// crashes the client on every empty fetch.
	results := []map[string]any{}

	for rows.Next() {
		valPtrs := make([]any, len(columns))
		for i, col := range columns {
			valPtrs[i] = reflect.New(col.ScanType()).Interface()
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return nil, fmt.Errorf("scan clickhouse row: %w", err)
		}
		row := make(map[string]any)
		for i, col := range columns {
			row[col.Name()] = reflect.ValueOf(valPtrs[i]).Elem().Interface()
		}
		results = append(results, transformRow(row))
	}
	// rows.Next() returns false both when iteration completes successfully
	// AND when the driver hits an error mid-stream (network drop, decode
	// failure on a row past the first). Without this check, a partial
	// result set silently masquerades as a complete one.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clickhouse rows: %w", err)
	}
	return results, nil
}

// mutationVerbs is a set of SQL leading keywords that don't return a result
// set — anything that mutates schema or data. Routed through Exec rather
// than Query (see executeCHQuery). Sourced from the ClickHouse statement
// reference: DML, DDL, role/privilege management, and runtime control
// (SYSTEM/KILL/SET). Read-only verbs (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN/
// EXISTS) intentionally fall through to the default Query path.
var mutationVerbs = map[string]struct{}{
	"INSERT":   {},
	"UPDATE":   {},
	"DELETE":   {},
	"TRUNCATE": {},
	"DROP":     {},
	"ALTER":    {},
	"CREATE":   {},
	"RENAME":   {},
	"EXCHANGE": {},
	"OPTIMIZE": {},
	"REPLACE":  {},
	"GRANT":    {},
	"REVOKE":   {},
	"ATTACH":   {},
	"DETACH":   {},
	"KILL":     {},
	"SET":      {},
	"USE":      {},
	"SYSTEM":   {},
}

// IsMutation reports whether sql's leading statement is a non-SELECT — i.e.
// one that returns no result set and must go through Exec, not Query.
// Leading whitespace and comments are skipped as ClickHouse's lexer skips
// them, then the first bareword is matched whole, case-insensitively, against
// mutationVerbs. After a WITH list ClickHouse parses only SELECT, a FROM-first
// SELECT or INSERT INTO, so a WITH-led statement is a write exactly when it
// holds INSERT INTO at the top level (hasTopLevelInsertInto). An
// `EXECUTE AS <user>` prefix is looked through to the statement it runs. A
// write classified as a read goes through Query, which runs it and then fails
// the call, so a client that retries the error writes again.
func IsMutation(sql string) bool {
	s := stripLeadingSQLComments(sql)
	if rest, ok := skipExecuteAs(s); ok {
		// Bare, it switches the session's user and returns no result set.
		if rest == "" || rest[0] == ';' {
			return true
		}
		s = rest
	}
	end := skipWord(s, 0)
	if end == 0 {
		return false
	}
	first := strings.ToUpper(s[:end])
	if first != "WITH" {
		_, ok := mutationVerbs[first]
		return ok
	}
	return hasTopLevelInsertInto(s[end:])
}

// skipExecuteAs returns what follows an `EXECUTE AS <user>[@<host>]` prefix
// leading s, past whitespace and comments, and true; or s and false if no such
// prefix leads it.
func skipExecuteAs(s string) (string, bool) {
	i := skipWord(s, 0)
	if !strings.EqualFold(s[:i], "EXECUTE") {
		return s, false
	}
	i = skipSpaceAndComments(s, i)
	j := skipWord(s, i)
	if !strings.EqualFold(s[i:j], "AS") {
		return s, false
	}
	i = skipSpaceAndComments(s, skipName(s, skipSpaceAndComments(s, j)))
	if i < len(s) && s[i] == '@' {
		i = skipSpaceAndComments(s, skipName(s, skipSpaceAndComments(s, i+1)))
	}
	return s[i:], true
}

// skipName returns the index just past the user or host name at s[i]: a
// bareword, a quoted identifier or string literal, or a heredoc.
func skipName(s string, i int) int {
	if i >= len(s) {
		return i
	}
	switch s[i] {
	case '\'', '"', '`':
		return skipQuoted(s, i)
	case 0xE2:
		return skipCurlyQuoted(s, i)
	case '$':
		if j := skipHeredoc(s, i); j > i {
			return j
		}
	}
	return skipWord(s, i)
}

// hasTopLevelInsertInto reports whether s holds INSERT INTO outside
// parentheses, stepping over string literals and quoted identifiers
// (skipQuoted, skipCurlyQuoted), heredocs (skipHeredoc) and comments
// (skipComment). No other word is taken for the statement: a WITH list's
// names and aliases may be spelled like any keyword (`WITH 1 AS select`,
// `WITH desc AS (…)`, `WITH set -> 1 AS f`, `WITH t.from AS y`), but only the
// INSERT statement puts INTO after an insert. The exception, a read's
// `… AS insert INTO OUTFILE 'f'`, is classified as a write and answers `[]`
// uncached: harmless, and contrived. OUTFILE cannot tell the two apart, as
// `INSERT INTO outfile …` names a table.
func hasTopLevelInsertInto(s string) bool {
	depth := 0
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '(':
			depth++
			i++
		case c == ')':
			if depth > 0 {
				depth--
			}
			i++
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(s, i)
		case c == 0xE2:
			// ‘…’ or “…”; any other character led by this byte is stepped
			// over a byte at a time, like the default.
			if j := skipCurlyQuoted(s, i); j > i {
				i = j
			} else {
				i++
			}
		case c == '$':
			// A heredoc, else a bareword led by `$` (never a keyword) or a
			// lone `$`.
			if j := skipHeredoc(s, i); j > i {
				i = j
			} else {
				i = skipWord(s, i+1)
			}
		case c == '.' && i+1 < len(s) && isDigit(s[i+1]):
			// A number led by `.` ends with its digits, so a word glued to it
			// is a word of its own: `.5INSERT` is `.5` then INSERT. After a
			// name ClickHouse reads the `.` as a qualifier (`t.5insert`), which
			// can only make a statement it rejects, or a read's INTO OUTFILE,
			// look like a write.
			i = skipDotNumber(s, i)
		case isWordByte(c):
			// A word led by a digit or `_` is read whole, so its tail is
			// never taken for a keyword (`_insert`, `5insert`).
			start := i
			i = skipWord(s, i)
			if depth == 0 && strings.EqualFold(s[start:i], "INSERT") {
				next := skipSpaceAndComments(s, i)
				if strings.EqualFold(s[next:skipWord(s, next)], "INTO") {
					return true
				}
			}
		case c == '-' && i+1 < len(s) && s[i+1] == '-', c == '#', c == '/' && i+1 < len(s) && (s[i+1] == '*' || s[i+1] == '/'):
			i = skipComment(s, i)
		default:
			i++
		}
	}
	return false
}

// skipWord returns the index just past the bareword at s[i]: ClickHouse's
// barewords run over ASCII letters, digits, `_` and `$`.
func skipWord(s string, i int) int {
	for i < len(s) && (isWordByte(s[i]) || s[i] == '$') {
		i++
	}
	return i
}

func isWordByte(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || isDigit(c) || c == '_'
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// skipDotNumber returns the index just past the number led by the `.` at s[i],
// read as ClickHouse's lexer reads one: digits, then an optional exponent (`e`
// or `E`, an optional sign, any digits), `_` allowed between two digits.
// Unlike a number led by a digit, it ends before any letters that follow it.
func skipDotNumber(s string, i int) int {
	i = skipDigits(s, i+1)
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		i = skipDigits(s, i)
	}
	return i
}

// skipDigits returns the index just past the run of digits at s[i], in which
// each `_` stands between two digits.
func skipDigits(s string, i int) int {
	for i < len(s) && (isDigit(s[i]) || s[i] == '_' && i > 0 && isDigit(s[i-1]) && i+1 < len(s) && isDigit(s[i+1])) {
		i++
	}
	return i
}

// skipHeredoc returns the index just past the heredoc opening at s[i] —
// `$tag$ … $tag$`, the tag a possibly empty run of letters, digits and `_`,
// matched exactly — or i if none does, as an unclosed one is not a heredoc to
// ClickHouse either.
func skipHeredoc(s string, i int) int {
	j := i + 1
	for j < len(s) && isWordByte(s[j]) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return i
	}
	tag := s[i : j+1]
	if k := strings.Index(s[j+1:], tag); k >= 0 {
		return j + 1 + k + len(tag)
	}
	return i
}

// stripLeadingSQLComments trims whitespace and comments from the front of
// sql, the way ClickHouse's lexer skips them before the first token.
func stripLeadingSQLComments(sql string) string {
	return sql[skipSpaceAndComments(sql, 0):]
}

// skipSpaceAndComments returns the index of the first byte at or after i that
// is neither whitespace nor inside a comment.
func skipSpaceAndComments(s string, i int) int {
	for i < len(s) {
		if n := sqlSpaceLen(s, i); n > 0 {
			i += n
			continue
		}
		j := skipComment(s, i)
		if j == i {
			return i
		}
		i = j
	}
	return i
}

// sqlSpaceLen is the byte length of the whitespace character at s[i], or 0.
// The set is ClickHouse's lexer's: ASCII space, \t \n \v \f \r, and the
// Unicode spaces it skips so that SQL pasted from a word processor parses. A
// leading one the classifier did not skip would hide the verb behind it.
func sqlSpaceLen(s string, i int) int {
	switch s[i] {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return 1
	}
	if s[i] < utf8.RuneSelf {
		return 0
	}
	r, n := utf8.DecodeRuneInString(s[i:])
	switch {
	case r == 0x85, r == 0xA0, r == 0x180E, r >= 0x2000 && r <= 0x200D,
		r == 0x2028, r == 0x2029, r == 0x202F, r == 0x205F, r == 0x2060,
		r == 0x3000, r == 0xFEFF:
		return n
	}
	return 0
}

// skipComment returns the index just past the comment starting at s[i], or i
// if none starts there: `--`, `//` and MySQL-compat `#` to end of line,
// `/* … */` nesting as ClickHouse's do. An unclosed block comment runs to the
// end, as it does for ClickHouse, which then rejects the statement.
func skipComment(s string, i int) int {
	switch {
	case strings.HasPrefix(s[i:], "--"), strings.HasPrefix(s[i:], "//"), s[i] == '#':
		if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
			return i + j + 1
		}
		return len(s)
	case strings.HasPrefix(s[i:], "/*"):
		depth := 0
		for j := i; j+1 < len(s); {
			switch {
			case s[j] == '/' && s[j+1] == '*':
				depth++
				j += 2
			case s[j] == '*' && s[j+1] == '/':
				depth--
				j += 2
				if depth == 0 {
					return j
				}
			default:
				j++
			}
		}
		return len(s)
	}
	return i
}

// skipCurlyQuoted returns the index just past a string literal in ‘…’ or a
// quoted identifier in “…”, which ClickHouse reads so that SQL pasted from a
// word processor parses, or i if none opens at s[i]. Nothing escapes inside
// them; an unclosed one runs to the end.
func skipCurlyQuoted(s string, i int) int {
	var closer string
	switch {
	case strings.HasPrefix(s[i:], "\u2018"):
		closer = "\u2019"
	case strings.HasPrefix(s[i:], "\u201c"):
		closer = "\u201d"
	default:
		return i
	}
	start := i + len(closer) // the opener is as long as its closer
	if k := strings.Index(s[start:], closer); k >= 0 {
		return start + k + len(closer)
	}
	return len(s)
}

// skipQuoted returns the index just past the string literal or quoted
// identifier opening at s[i] (`'`, `"` or backtick). As in ClickHouse's lexer,
// a doubled quote or a backslash escapes the next byte; an unclosed one runs
// to the end.
func skipQuoted(s string, i int) int {
	q := s[i]
	for i++; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case q:
			if i+1 < len(s) && s[i+1] == q {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(s)
}

// transformRow converts ClickHouse-specific types to JSON-friendly values.
func transformRow(row map[string]any) map[string]any {
	for k, v := range row {
		switch val := v.(type) {
		case uuid.UUID:
			row[k] = val.String()
		case [16]byte:
			row[k] = uuid.UUID(val).String()
		case time.Time:
			row[k] = val.UTC().Format(time.RFC3339Nano)
		case *time.Time:
			// Nullable(DateTime…) scans as a pointer; NULL stays nil (JSON null).
			if val != nil {
				row[k] = val.UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return row
}
