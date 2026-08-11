//go:build fuzzdeps

// Differential fuzzer for ResolveTables. Excluded from the normal build by the
// fuzzdeps tag; run on demand against a local ClickHouse:
//
//	go test -tags fuzzdeps -run TestFuzzDeps ./internal/discovery/ -v
//
// It generates hundreds of diverse queries (nested/UNION/parameter-gated, plus
// targeted joinGet/dictGet/cross-db/FINAL/SAMPLE/injection cases) with a
// construction-based ground truth — it builds each query, so it knows which tables
// sit in live vs. dead arms — then runs the real ResolveTables and flags
// under-resolution (stale-cache BUG, fails the test), over-resolution (pruning
// miss / parser hallucination), and EXPLAIN errors. Creates and drops its own
// fuzz_deps / fuzz_other databases; skips if no ClickHouse is reachable on :9000.
package discovery

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	fdb    = "fuzz_deps"
	fother = "fuzz_other"
)

var baseShort = []string{"bt0", "bt1", "bt2", "bt3", "bt4", "bt5", "bt6", "bt7"}

func full(short string) string { return fdb + "." + short }

type occ struct {
	table string
	dead  bool
}

func connectFuzz(t *testing.T) driver.Conn {
	open := func(db string) (driver.Conn, error) {
		return clickhouse.Open(&clickhouse.Options{
			Addr:        []string{"127.0.0.1:9000"},
			Auth:        clickhouse.Auth{Database: db, Username: "default", Password: ""},
			DialTimeout: 5 * time.Second,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	boot, err := open("default")
	if err != nil {
		t.Fatalf("open default: %v", err)
	}
	if err := boot.Ping(ctx); err != nil {
		t.Skipf("clickhouse not reachable: %v", err)
	}
	_ = boot.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+fdb)
	_ = boot.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+fother)
	_ = boot.Close()
	conn, err := open(fdb)
	if err != nil {
		t.Fatalf("open %s: %v", fdb, err)
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping %s: %v", fdb, err)
	}
	return conn
}

func setupSchema(t *testing.T, conn driver.Conn) {
	ctx := context.Background()
	exec := func(s string) {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	exec("CREATE DATABASE IF NOT EXISTS " + fdb)
	exec("CREATE DATABASE IF NOT EXISTS " + fother)
	for _, s := range baseShort {
		exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (x Int32, s String) ENGINE=MergeTree ORDER BY x", fdb, s))
		exec(fmt.Sprintf("INSERT INTO %s.%s SELECT number, toString(number) FROM numbers(5)", fdb, s))
	}
	exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.`weird.name` (x Int32, s String) ENGINE=MergeTree ORDER BY x", fdb))
	exec(fmt.Sprintf("INSERT INTO %s.`weird.name` SELECT number, toString(number) FROM numbers(5)", fdb))
	exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.jt (k String, v String) ENGINE=Join(ANY, LEFT, k)", fdb))
	exec(fmt.Sprintf("INSERT INTO %s.jt VALUES ('a','1')", fdb))
	exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.dsrc (id UInt64, val String) ENGINE=MergeTree ORDER BY id", fdb))
	exec(fmt.Sprintf("INSERT INTO %s.dsrc VALUES (1,'one')", fdb))
	exec(fmt.Sprintf("CREATE DICTIONARY IF NOT EXISTS %s.mydict (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(DB '%s' TABLE 'dsrc')) LAYOUT(HASHED()) LIFETIME(0)", fdb, fdb))
	// Deliberately NOT pre-loading mydict, so the dictGet cases exercise
	// resolveDictSources' load-before path against a NOT_LOADED dictionary.
	exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.rmt (x Int32, s String) ENGINE=ReplacingMergeTree ORDER BY x", fdb))
	exec(fmt.Sprintf("INSERT INTO %s.rmt SELECT number, toString(number) FROM numbers(5)", fdb))
	exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.smp (x Int32, h UInt32) ENGINE=MergeTree ORDER BY h SAMPLE BY h", fdb))
	exec(fmt.Sprintf("INSERT INTO %s.smp SELECT number, toUInt32(number) FROM numbers(5)", fdb))
	exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.ext (x Int32, s String) ENGINE=MergeTree ORDER BY x", fother))
	exec(fmt.Sprintf("INSERT INTO %s.ext SELECT number, toString(number) FROM numbers(5)", fother))
}

func teardown(conn driver.Conn) {
	ctx := context.Background()
	_ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+fdb)
	_ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+fother)
}

// genGate returns a WHERE clause and whether it makes the arm dead. Only foldable
// dead gates ('a'='b') are used, since the parser only prunes constant-folded WHEREs.
func genGate(rng *rand.Rand) (string, bool) {
	switch rng.Intn(6) {
	case 0:
		return "", false // no gate (live)
	case 1:
		return " WHERE 1=1", false // foldable live
	case 2:
		return " WHERE 'a'='b'", true // foldable DEAD (calibrated → CONSTANT UInt64_0)
	case 3:
		return " WHERE 'q'='q'", false // foldable live
	case 4:
		return " WHERE x > 0", false // real predicate (live)
	default:
		return " WHERE x = 0", false // real predicate that must NOT be read as dead
	}
}

func genSelect(rng *rand.Rand, depth int, ancestorDead bool) (string, []occ) {
	if depth <= 0 || rng.Intn(100) < 45 {
		short := baseShort[rng.Intn(len(baseShort))]
		clause, dead := genGate(rng)
		return "SELECT x FROM " + full(short) + clause, []occ{{short, ancestorDead || dead}}
	}
	clause, dead := genGate(rng)
	d := ancestorDead || dead
	var inner string
	var iocc []occ
	if rng.Intn(2) == 0 {
		inner, iocc = genUnion(rng, depth-1, d)
	} else {
		inner, iocc = genSelect(rng, depth-1, d)
	}
	alias := fmt.Sprintf("a%d", rng.Intn(1_000_000))
	return "SELECT x FROM (" + inner + ") " + alias + clause, iocc
}

func genUnion(rng *rand.Rand, depth int, ancestorDead bool) (string, []occ) {
	k := 2 + rng.Intn(3)
	parts := make([]string, 0, k)
	var occs []occ
	for i := 0; i < k; i++ {
		s, o := genSelect(rng, depth-1, ancestorDead)
		parts = append(parts, s)
		occs = append(occs, o...)
	}
	return strings.Join(parts, " UNION ALL "), occs
}

func genTop(rng *rand.Rand) (string, []occ) {
	depth := 2 + rng.Intn(5) // 2..6
	if rng.Intn(2) == 0 {
		return genUnion(rng, depth, false)
	}
	return genSelect(rng, depth, false)
}

func sets(occs []occ) (expected, referenced map[string]bool) {
	expected = map[string]bool{}
	referenced = map[string]bool{}
	for _, o := range occs {
		referenced[o.table] = true
		if !o.dead {
			expected[o.table] = true
		}
	}
	return
}

type special struct {
	cat, sql string
	expected []string
}

func specials() []special {
	return []special{
		{"count", "SELECT count() FROM fuzz_deps.bt0", []string{"bt0"}},
		{"join", "SELECT a.x FROM fuzz_deps.bt0 a JOIN fuzz_deps.bt1 b ON a.x=b.x", []string{"bt0", "bt1"}},
		{"self-join", "SELECT a.x FROM fuzz_deps.bt0 a JOIN fuzz_deps.bt0 b ON a.x=b.x", []string{"bt0"}},
		{"join-dead-where", "SELECT a.x FROM fuzz_deps.bt0 a JOIN fuzz_deps.bt1 b ON a.x=b.x WHERE 'a'='b'", []string{}},
		{"in-subquery", "SELECT x FROM fuzz_deps.bt0 WHERE x IN (SELECT x FROM fuzz_deps.bt1)", []string{"bt0", "bt1"}},
		{"in-subquery-dead", "SELECT x FROM fuzz_deps.bt0 WHERE 'a'='b' UNION ALL SELECT x FROM fuzz_deps.bt2 WHERE x IN (SELECT x FROM fuzz_deps.bt3)", []string{"bt2", "bt3"}},
		{"cte", "WITH c AS (SELECT x FROM fuzz_deps.bt2) SELECT x FROM c", []string{"bt2"}},
		{"cte-join", "WITH c AS (SELECT x FROM fuzz_deps.bt2) SELECT c.x FROM c JOIN fuzz_deps.bt3 t ON c.x=t.x", []string{"bt2", "bt3"}},
		{"joinGet", "SELECT joinGet('fuzz_deps.jt','v', toString(x)) AS g FROM fuzz_deps.bt0", []string{"bt0", "jt"}},
		{"joinGet-bare", "SELECT joinGet('jt','v', toString(x)) AS g FROM fuzz_deps.bt0", []string{"bt0", "jt"}},
		{"dictGet", "SELECT dictGet('fuzz_deps.mydict','val', toUInt64(x)) AS d FROM fuzz_deps.bt0", []string{"bt0", "dsrc"}},
		{"dictGetOrDefault", "SELECT dictGetOrDefault('fuzz_deps.mydict','val', toUInt64(x), '') AS d FROM fuzz_deps.bt0", []string{"bt0", "dsrc"}},
		{"dictHas", "SELECT x FROM fuzz_deps.bt0 WHERE dictHas('fuzz_deps.mydict', toUInt64(x))", []string{"bt0", "dsrc"}},
		{"cross-db", "SELECT x FROM fuzz_deps.bt0 UNION ALL SELECT x FROM fuzz_other.ext", []string{"bt0"}},
		{"cross-db-join", "SELECT a.x FROM fuzz_deps.bt0 a JOIN fuzz_other.ext b ON a.x=b.x", []string{"bt0"}},
		{"table-func", "SELECT number AS x FROM numbers(10)", []string{}},
		{"table-func-in", "SELECT x FROM fuzz_deps.bt0 WHERE x IN (SELECT number FROM numbers(5))", []string{"bt0"}},
		{"weird-name", "SELECT x FROM fuzz_deps.`weird.name`", []string{"weird.name"}},
		{"weird-name-dead", "SELECT x FROM fuzz_deps.`weird.name` WHERE 'a'='b'", []string{}},
		{"weird-name-union", "SELECT x FROM fuzz_deps.`weird.name` WHERE 1=1 UNION ALL SELECT x FROM fuzz_deps.bt0 WHERE 'a'='b'", []string{"weird.name"}},
		{"strinj-tablename", "SELECT 'table_name: fuzz_deps.bt5' AS lbl FROM fuzz_deps.bt0 WHERE x>0", []string{"bt0"}},
		{"strinj-deadconst", "SELECT x FROM fuzz_deps.bt0 WHERE s = 'constant_value: UInt64_0,'", []string{"bt0"}},
		{"strinj-where", "SELECT x FROM fuzz_deps.bt0 WHERE s = 'WHERE' OR s = 'QUERY id: 9'", []string{"bt0"}},
		{"groupby-having", "SELECT x, count() c FROM fuzz_deps.bt0 GROUP BY x HAVING c > 0 ORDER BY x LIMIT 3", []string{"bt0"}},
		{"final-prewhere", "SELECT x FROM fuzz_deps.rmt FINAL PREWHERE x > 0 WHERE s != ''", []string{"rmt"}},
		{"final", "SELECT x FROM fuzz_deps.rmt FINAL", []string{"rmt"}},
		{"final-join", "SELECT r.x FROM fuzz_deps.rmt AS r FINAL JOIN fuzz_deps.bt0 AS b ON r.x = b.x", []string{"rmt", "bt0"}},
		{"sample", "SELECT x FROM fuzz_deps.smp SAMPLE 0.5", []string{"smp"}},
		{"final-dead-arm", "SELECT x FROM fuzz_deps.rmt FINAL WHERE 'a'='b' UNION ALL SELECT x FROM fuzz_deps.bt0", []string{"bt0"}},
		{"window", "SELECT sum(x) OVER (PARTITION BY s) AS w FROM fuzz_deps.bt0", []string{"bt0"}},
		{"deep-live", "SELECT x FROM (SELECT x FROM (SELECT x FROM fuzz_deps.bt3 WHERE x>0) z WHERE 1=1) y", []string{"bt3"}},
		{"live-and-dead-same", "SELECT x FROM fuzz_deps.bt4 WHERE 1=1 UNION ALL SELECT x FROM fuzz_deps.bt4 WHERE 'a'='b'", []string{"bt4"}},
		{"all-dead-union", "SELECT x FROM fuzz_deps.bt5 WHERE 'a'='b' UNION ALL SELECT x FROM fuzz_deps.bt6 WHERE 'x'='y'", []string{}},
		{"dead-arm-with-subquery", "SELECT x FROM fuzz_deps.bt0 WHERE 1=1 UNION ALL SELECT x FROM (SELECT x FROM fuzz_deps.bt1) z WHERE 'a'='b'", []string{"bt0"}},
		{"nested-union-in-dead-arm", "SELECT x FROM fuzz_deps.bt0 WHERE 'a'='b' UNION ALL SELECT x FROM fuzz_deps.bt1 WHERE 1=1", []string{"bt1"}},
	}
}

func toSet(ss []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestFuzzDeps(t *testing.T) {
	conn := connectFuzz(t)
	defer conn.Close()
	setupSchema(t, conn)
	defer teardown(conn)
	ctx := context.Background()

	explainDump := func(sql string) string {
		rows, err := conn.Query(ctx, "EXPLAIN QUERY TREE "+sql)
		if err != nil {
			return "  <explain err: " + err.Error() + ">"
		}
		defer func() { _ = rows.Close() }()
		var b strings.Builder
		for rows.Next() {
			var ln string
			_ = rows.Scan(&ln)
			b.WriteString("    " + ln + "\n")
		}
		return b.String()
	}

	type finding struct {
		cat, sql                   string
		expected, got, under, over []string
	}
	var unders, overs, errs []finding
	total := 0

	check := func(cat, sql string, expected, referenced map[string]bool) {
		total++
		res, err := ResolveTables(ctx, conn, fdb, sql)
		if err != nil {
			errs = append(errs, finding{cat: cat, sql: sql, got: []string{err.Error()}})
			return
		}
		got := res.Tables
		gotSet := toSet(got)
		var under, over []string
		for tbl := range expected {
			if !gotSet[tbl] {
				under = append(under, tbl)
			}
		}
		for _, g := range got {
			if !expected[g] {
				over = append(over, g)
			}
		}
		sort.Strings(under)
		sort.Strings(over)
		if len(under) > 0 {
			unders = append(unders, finding{cat, sql, keys(expected), got, under, over})
		}
		if len(over) > 0 {
			overs = append(overs, finding{cat, sql, keys(expected), got, under, over})
		}
	}

	rng := rand.New(rand.NewSource(0xC0FFEE))
	const N = 500
	for i := 0; i < N; i++ {
		sql, occs := genTop(rng)
		expected, referenced := sets(occs)
		check("random", sql, expected, referenced)
	}
	for _, sp := range specials() {
		check(sp.cat, sp.sql, toSet(sp.expected), nil)
	}
	injPool := []string{
		"table_name: fuzz_deps.bt5", "table_name: fuzz_deps.bt0",
		"function_name: joinGet", "function_name: dictGet",
		"constant_value: UInt64_0,", "WHERE", "QUERY id: 0", "JOIN TREE",
	}
	for i, p := range injPool {
		tbl := baseShort[i%len(baseShort)]
		sql := fmt.Sprintf("SELECT '%s' AS lbl FROM %s WHERE x > %d", p, full(tbl), i)
		check("inject", sql, toSet([]string{tbl}), nil)
	}

	fmt.Printf("\n========== FUZZ DEPS REPORT ==========\n")
	fmt.Printf("queries run: %d   under-resolutions (BUGS): %d   over-resolutions: %d   explain-errors: %d\n\n",
		total, len(unders), len(overs), len(errs))

	if len(unders) > 0 {
		fmt.Printf("---- UNDER-RESOLUTION (stale-cache risk) ----\n")
		for _, f := range unders {
			fmt.Printf("[%s] MISSED %v\n  expected=%v got=%v\n  sql: %s\n  EXPLAIN:\n%s\n",
				f.cat, f.under, f.expected, f.got, f.sql, explainDump(f.sql))
		}
	}

	if len(overs) > 0 {
		fmt.Printf("---- OVER-RESOLUTION (pruning miss / phantom) ----\n")
		shown := 0
		for _, f := range overs {
			fmt.Printf("[%s] EXTRA %v   expected=%v got=%v\n  sql: %s\n", f.cat, f.over, f.expected, f.got, f.sql)
			if shown < 6 {
				fmt.Printf("  EXPLAIN:\n%s\n", explainDump(f.sql))
			}
			shown++
		}
	}

	if len(errs) > 0 {
		fmt.Printf("---- EXPLAIN ERRORS (would over-resolve to all tables) ----\n")
		for _, f := range errs {
			fmt.Printf("[%s] %v\n  sql: %s\n", f.cat, f.got, f.sql)
		}
	}
	fmt.Printf("========== END REPORT ==========\n\n")

	if len(unders) > 0 {
		t.Errorf("%d under-resolution(s) found — see report above", len(unders))
	}
}
