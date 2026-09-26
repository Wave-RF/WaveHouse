// Package mutationtest holds the statements api.IsMutation is tested on,
// shared by its unit test and by the integration test that checks each one
// against ClickHouse's own parser. Add a case here, not to either test.
package mutationtest

// Case is a statement and how api.IsMutation classifies it.
type Case struct {
	Name string
	SQL  string
	// Mutation is true for a statement that goes through Exec.
	Mutation bool
	// Unparsed marks a statement ClickHouse rejects as a syntax error, so its
	// parser has no answer to check Mutation against; the integration test
	// fails if one starts to parse.
	Unparsed bool
}

// Cases is every statement api.IsMutation is tested on.
var Cases = []Case{
	{"select", "SELECT 1", false, false},
	{"select lower", "select 1", false, false},
	{"with cte", "WITH x AS (SELECT 1) SELECT * FROM x", false, false},
	{"show", "SHOW TABLES", false, false},
	{"describe", "DESCRIBE clicks", false, false},
	{"explain", "EXPLAIN SELECT 1", false, false},
	{"exists", "EXISTS TABLE clicks", false, false},

	{"insert", "INSERT INTO t VALUES (1)", true, false},
	{"update", "UPDATE t SET a=1 WHERE b=2", true, false},
	{"delete", "DELETE FROM t WHERE id=1", true, false},
	{"truncate", "TRUNCATE TABLE t", true, false},
	{"truncate lower", "truncate table t", true, false},
	{"drop", "DROP TABLE t", true, false},
	{"alter", "ALTER TABLE t ADD COLUMN c String", true, false},
	{"create", "CREATE TABLE t (a Int)", true, false},
	{"rename", "RENAME TABLE a TO b", true, false},
	{"exchange", "EXCHANGE TABLES t1 AND t2", true, false},
	{"optimize", "OPTIMIZE TABLE t", true, false},
	{"replace into", "REPLACE INTO t VALUES (1)", true, true},
	{"replace table", "REPLACE TABLE t (a Int) ENGINE = Memory", true, false},
	{"grant", "GRANT SELECT ON t TO u", true, false},
	{"revoke", "REVOKE SELECT ON t FROM u", true, false},
	{"system", "SYSTEM RELOAD CONFIG", true, false},
	{"attach", "ATTACH TABLE t FROM '/path'", true, false},
	{"detach", "DETACH TABLE t", true, false},
	{"kill", "KILL QUERY WHERE query_id = 'abc'", true, false},
	{"set", "SET max_threads = 4", true, false},
	{"use", "USE mydb", true, false},

	{"leading whitespace", "   \n\tTRUNCATE TABLE t", true, false},
	{"line comment then mutation", "-- drop guard\nDROP TABLE t", true, false},
	{"hash line comment then mutation", "# audit\nDROP TABLE t", true, false},
	{"block comment then mutation", "/* admin */ ALTER TABLE t ADD COLUMN c Int", true, false},
	{"mixed comments then select", "-- foo\n# bar\n/* baz */ SELECT 1", false, false},
	{"with insert", "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte", true, false},
	{"with insert lower", "with cte as (select 1) insert into t select * from cte", true, false},
	{"with multi-cte insert", "WITH a AS (SELECT 1), b AS (SELECT 2) INSERT INTO t SELECT * FROM a CROSS JOIN b", true, false},
	{"with nested parens insert", "WITH cte AS (SELECT id FROM t WHERE id IN (1,2,3)) INSERT INTO t2 SELECT * FROM cte", true, false},
	{"with paren-in-string insert", "WITH cte AS (SELECT ')' AS x) INSERT INTO t2 SELECT * FROM cte", true, false},
	{"with materialized insert", "WITH cte AS MATERIALIZED (SELECT 1) INSERT INTO t SELECT * FROM cte", true, false},
	{"with recursive select", "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT * FROM x) SELECT * FROM x", false, false},
	{"with nested select", "WITH x AS (SELECT 1) SELECT * FROM (SELECT * FROM x)", false, false},
	{"with scalar insert", "WITH '/path' AS p INSERT INTO files VALUES (p)", true, false},
	{"with line comment containing DELETE then select", "WITH cte AS (SELECT 1) -- old DELETE approach\nSELECT * FROM cte", false, false},
	{"with hash comment containing TRUNCATE then select", "WITH cte AS (SELECT 1) # was TRUNCATE\nSELECT * FROM cte", false, false},
	{"with block comment containing INSERT then select", "WITH cte AS (SELECT 1) /* INSERT reminder */ SELECT * FROM cte", false, false},
	{"with comment then real mutation", "WITH cte AS (SELECT 1) -- explanatory\nINSERT INTO t SELECT * FROM cte", true, false},
	{"with unclosed block comment", "WITH cte AS (SELECT 1) /* unterminated comment DELETE", false, true},
	{"with select from system tables (collision regression)", "WITH x AS (SELECT 1) SELECT * FROM system.tables", false, false},
	{"with select from system columns lower (collision regression)", "with x as (select 1) select name from system.columns", false, false},
	{"with select aliased as set (false positive regression)", "WITH cte AS (SELECT 1) SELECT * FROM cte AS set", false, false},
	{"with select from system tables then real insert", "WITH x AS (SELECT * FROM system.tables) INSERT INTO snapshot SELECT * FROM x", true, false},
	{"with CTE alias named set (read)", "WITH set AS (SELECT 1) SELECT * FROM set", false, false},
	{"with CTE alias named alter (read)", "WITH alter AS (SELECT 1) SELECT id FROM alter", false, false},
	{"with CTE alias named drop lowercase (read)", "with drop as (select 1) select * from drop", false, false},
	{"with CTE name with column list (read)", "WITH cte (a, b) AS (SELECT 1, 2) SELECT * FROM cte", false, false},
	{"with multi-CTE both with verb-name aliases (read)", "WITH set AS (SELECT 1), kill AS (SELECT 2) SELECT * FROM set CROSS JOIN kill", false, false},
	{"with parenthesized SELECT then system table (CTE-lookahead ordering regression)", "WITH x AS (SELECT 1) SELECT (1) FROM system.tables", false, false},
	{"with tuple-shape SELECT then system table", "WITH x AS (SELECT 1) SELECT (a, b) FROM system.parts", false, false},

	// A backslash escapes the next byte inside all three quote kinds, so an
	// escaped quote does not end the literal or identifier.
	{"with backslash-escaped quote in literal then insert", `WITH m AS (SELECT 'it\'s' AS s) INSERT INTO t SELECT s FROM m`, true, false},
	{"with backslash-escaped quote in literal then select", `WITH m AS (SELECT 'a\'b' AS s) SELECT 'x) INSERT' FROM m`, false, false},
	{"with backslash-escaped double quote then insert", `WITH m AS (SELECT 'x' AS "a\"(b") INSERT INTO t SELECT * FROM m`, true, false},
	{"with backslash-escaped double quote then select", `WITH m AS (SELECT 1 AS "a\"b") SELECT 2 AS "x) INSERT" FROM m`, false, false},
	{"with backslash-escaped backtick then insert", "WITH m AS (SELECT 'x' AS `a\\`(b`) INSERT INTO t SELECT * FROM m", true, false},
	{"with backslash-escaped backtick then select", "WITH m AS (SELECT 1 AS `a\\`b`) SELECT 2 AS `x) INSERT` FROM m", false, false},

	// A heredoc ($$…$$, $tag$…$tag$) is a literal: its parens, quotes
	// and words are not the statement's.
	{"with heredoc holding a paren then insert", "WITH $$ ( $$ AS s INSERT INTO t SELECT s", true, false},
	{"with tagged heredoc holding a quote then insert", "WITH $x$ it's $x$ AS s INSERT INTO t SELECT s", true, false},
	{"with tagged heredoc holding a paren and another tag then insert", "WITH $x$ ( $y$ $x$ AS s INSERT INTO t SELECT s", true, false},
	{"with heredoc holding a verb then select", "WITH $$INSERT$$ AS s SELECT s", false, false},
	{"with tagged heredoc holding a paren and a verb then select", "WITH $x$ ) INSERT $x$ AS s SELECT s", false, false},
	{"with CTE alias set$ (read)", "WITH set$ AS (SELECT 1 AS v) SELECT * FROM set$", false, false},

	// A word led by `_` is one bareword, never a keyword's tail, and a
	// leading bareword is matched whole, never by its first letters.
	{"leading bareword insert_log", "insert_log VALUES (1)", false, true},
	{"leading bareword insert2", "insert2 INTO t VALUES (1)", false, true},
	{"with alias _delete (read)", "WITH 1 AS _delete SELECT _delete", false, false},
	{"with alias _set (read)", "WITH [1,2] AS _set SELECT has(_set, 1)", false, false},

	// `//` starts a line comment.
	{"slash comment then insert", "// note\nINSERT INTO t VALUES (1)", true, false},
	{"slash comment hiding insert then select", "// INSERT\nSELECT 1", false, false},
	{"with slash comment holding a paren then insert", "WITH x AS (SELECT 'a' AS s) // (\nINSERT INTO t SELECT * FROM x", true, false},

	// ‘…’ is a string literal and “…” a quoted identifier; nothing escapes
	// inside them.
	{"with curly-quoted literal holding a paren then insert", "WITH x AS (SELECT \u2018(\u2019 AS s) INSERT INTO t SELECT s FROM x", true, false},
	{"with curly-quoted literal holding a verb then select", "WITH x AS (SELECT \u2018) INSERT\u2019 AS s) SELECT s FROM x", false, false},
	{"with curly-quoted identifier holding a paren then insert", "WITH x AS (SELECT 'q' AS \u201cc(d\u201d) INSERT INTO t SELECT * FROM x", true, false},

	// ClickHouse's lexer skips \v, \f and Unicode spaces as whitespace
	// (TestIsMutation_ClickHouseWhitespace covers the whole set).
	{"leading form feed then insert", "\fINSERT INTO t VALUES (1)", true, false},
	{"leading vertical tab then insert", "\vINSERT INTO t VALUES (1)", true, false},
	{"leading NBSP then insert", "\u00a0INSERT INTO t VALUES (1)", true, false},
	{"leading BOM then insert", "\ufeffINSERT INTO t VALUES (1)", true, false},
	{"leading NBSP then select", "\u00a0SELECT 1", false, false},
	{"with CTE alias named set before form feed AS (read)", "WITH set\fAS (SELECT 1) SELECT * FROM set", false, false},
	{"with CTE alias named set before NBSP AS (read)", "WITH set\u00a0AS (SELECT 1) SELECT * FROM set", false, false},

	// ClickHouse block comments nest.
	{"nested block comment hiding select then insert", "/* a /* b */ SELECT */ INSERT INTO t VALUES (1)", true, false},
	{"nested block comment hiding insert then select", "/* a /* b */ INSERT */ SELECT 1", false, false},
	{"unclosed nested block comment", "/* a /* b */ INSERT INTO t VALUES (1)", false, true},
	{"with nested block comment hiding select then insert", "WITH x AS (SELECT 1) /* a /* b */ SELECT */ INSERT INTO t SELECT * FROM x", true, false},
	{"with nested block comment hiding insert then select", "WITH x AS (SELECT 1) /* a /* b */ INSERT */ SELECT * FROM x", false, false},
	{"with nested block comment before CTE AS (read)", "WITH set /* a /* b */ c */ AS (SELECT 1) SELECT * FROM set", false, false},

	// After a WITH list ClickHouse parses only SELECT, a FROM-first SELECT and
	// INSERT INTO; any other statement is a syntax error, so nothing runs.
	{"with delete", "WITH cte AS (SELECT id FROM x) DELETE FROM t WHERE id IN (SELECT id FROM cte)", false, true},
	{"with alter update", "WITH cte AS (SELECT 1) ALTER TABLE t UPDATE a=1 WHERE id IN (SELECT id FROM cte)", false, true},
	{"with truncate", "WITH cte AS (SELECT 1) TRUNCATE TABLE t", false, true},
	{"with CTE alias named update then alter update", "WITH update AS (SELECT id FROM x) ALTER TABLE other UPDATE c=1 WHERE id IN (SELECT id FROM update)", false, true},
	{"with from-first select", "WITH 1 AS x FROM system.one SELECT x", false, false},
	{"with from-first select from a subquery", "WITH 1 AS x FROM (SELECT 1) SELECT x", false, false},

	// A WITH list's names may be spelled like any keyword: a CTE name, an
	// alias, a function, a lambda parameter, an operand, a qualified name's
	// part, an array element or a bare element.
	{"with alias desc then insert", "WITH 'd' AS desc INSERT INTO t SELECT length(desc)", true, false},
	{"with CTE named desc then insert", "WITH desc AS (SELECT 1 AS x) INSERT INTO t SELECT * FROM desc", true, false},
	{"with CTE named check then insert", "WITH check AS (SELECT 1 AS x) INSERT INTO t SELECT * FROM check", true, false},
	{"with CTE named explain then insert", "WITH explain AS (SELECT 1 AS x) INSERT INTO t SELECT * FROM explain", true, false},
	{"with alias show then insert", "WITH 1 AS show INSERT INTO t SELECT show", true, false},
	{"with alias describe then insert", "WITH 1 AS describe INSERT INTO t SELECT describe", true, false},
	{"with EXISTS expression then insert", "WITH EXISTS(SELECT 1 FROM t WHERE x = 7) AS seen INSERT INTO t2 SELECT seen", true, false},
	{"with alias select then insert", "WITH 1 AS select INSERT INTO t SELECT 1", true, false},
	{"with operand select then insert", "WITH 1 + select AS y INSERT INTO t SELECT y", true, false},
	{"with qualified select then insert", "WITH t.select AS y INSERT INTO t SELECT y", true, false},
	{"with array of select then insert", "WITH [select] AS a INSERT INTO t SELECT a", true, false},
	{"with bare element from then insert", "WITH from INSERT INTO t SELECT 1", true, false},
	{"with comment between INSERT and INTO", "WITH 1 AS x INSERT /* c */ INTO t SELECT x", true, false},
	{"with alias set (read)", "WITH 1 AS set SELECT set", false, false},
	{"with alias use (read)", "WITH 1 AS use SELECT use", false, false},
	{"with alias kill (read)", "WITH 1 AS kill SELECT kill", false, false},
	{"with alias system (read)", "WITH 1 AS system SELECT system", false, false},
	{"with alias insert (read)", "WITH 1 AS insert SELECT insert", false, false},
	{"with alias alter then from-first select", "WITH 1 AS alter FROM system.one SELECT alter", false, false},
	{"with lambda parameter set (read)", "WITH set -> 1 AS f SELECT f(2)", false, false},
	{"with lambda parameter insert (read)", "WITH insert -> 1 AS f SELECT f(2)", false, false},
	{"with operand insert (read)", "WITH insert + 1 AS y SELECT y", false, false},
	{"with bare element insert (read)", "WITH insert SELECT 1", false, false},
	{"with function insert (read)", "WITH insert(1) AS y SELECT y", false, false},

	{"empty", "", false, true},
	{"comment only", "-- just a comment", false, true},
	{"unclosed block comment", "/* never closed", false, true},
}
