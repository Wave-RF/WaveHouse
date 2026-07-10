package discovery

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// prunedTablesCounter counts table dependencies dropped by dead-branch pruning
// (a parameter-gated UNION arm whose WHERE folded to a constant false). It lets
// operators measure how often the precision optimization fires. A no-op until a
// meter provider is configured, so it is safe to use unconditionally.
var prunedTablesCounter, _ = otel.Meter("wavehouse-pipes").Int64Counter(
	"wavehouse_pipe_dep_tables_pruned_total",
	metric.WithDescription("Table dependencies dropped by dead-branch (constant-false) pruning during pipe resolution"),
)

// Resolution is what ResolveTables reads off ClickHouse's query analysis.
type Resolution struct {
	// Tables is the local (configured-database) table/view names the query reads.
	Tables []string
	// Pruned reports that dead-branch pruning dropped at least one table. The
	// caller TTL-caps a pruned result as a belt-and-suspenders bound: pruning is
	// fuzzed and fails safe toward keeping, but it is the one precision
	// optimization that REMOVES a table from the dependency set, so a hypothetical
	// wrong prune would otherwise cache an unwatched result at full TTL.
	Pruned bool
	// External reports that the query reads something no table version can watch:
	// a table function (s3()/numbers()/url()/…), a table in a DIFFERENT database,
	// or a dictionary whose source isn't a local table (file/http/executable, or
	// cross-database). Such reads contribute nothing to Tables, so writes to them
	// can never evict — the caller TTL-caps the result (UnresolvedDepsTTLCap) so
	// it self-expires instead of serving stale for a full TTL.
	External bool
}

// ResolveTables asks ClickHouse which tables a query reads, by running
// EXPLAIN QUERY TREE and reading the table nodes off its own analysis. This
// replaces static SQL parsing entirely — ClickHouse resolves the query, we just
// read the answer.
//
// EXPLAIN QUERY TREE is deliberately the PRE-optimization form: it keeps a table
// even when the query is answered from metadata (SELECT count() FROM t), and it
// does NOT drop empty tables or trivial counts the way EXPLAIN PLAN / query_log do.
// So it never UNDER-resolves, the invariant we need.
//
// On top of the raw tree we apply two refinements (parseExplainTables):
//
//   - joinGet/dictGet — read-bearing functions whose target is a NAME constant
//     rather than a TABLE node — are tracked (the dict via its backing table). Both
//     are real reads a write must evict.
//   - Dead-branch pruning — a parameter-gated UNION arm whose WHERE folded to a
//     constant false (e.g. {{source}}='web' makes the 'mobile' arm 'web'='mobile')
//     reads nothing, so its tables are dropped. This is data-INDEPENDENT (pure
//     constant folding) and so safe, BUT it makes the resolved set depend on the
//     bound parameter values — the caller must therefore cache the result per BOUND
//     query, not per template, or a different binding would reuse a wrongly-pruned
//     set (see api.PipesHandler.resolveDeps). Pruning fails safe toward KEEPING: any
//     unrecognized shape leaves the table tracked (over-resolve), never dropped.
//
// An EXPLAIN error — a write/DDL pipe, a missing table, or an unreachable
// ClickHouse — is returned so the caller can fall back to the database-version
// namespace rather than risk an under-resolution.
func ResolveTables(ctx context.Context, conn driver.Conn, database, sql string) (Resolution, error) {
	rows, err := conn.Query(ctx, "EXPLAIN QUERY TREE "+sql)
	if err != nil {
		return Resolution{}, fmt.Errorf("explain query tree: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var lines []string
	for rows.Next() {
		var ln string
		if err := rows.Scan(&ln); err != nil {
			return Resolution{}, fmt.Errorf("scan explain row: %w", err)
		}
		lines = append(lines, ln)
	}
	if err := rows.Err(); err != nil {
		return Resolution{}, err
	}

	tables, dicts, pruned, external := parseExplainTables(lines, database)
	if pruned > 0 {
		prunedTablesCounter.Add(ctx, int64(pruned))
	}
	// A dictGet target is the dictionary, not a table; map each to its backing
	// ClickHouse table (a CLICKHOUSE-sourced dict in the configured db). A
	// file/http/executable dict has no local table — an external read, like a
	// table function. Only queried when the pipe actually reads a dictionary.
	if len(dicts) > 0 {
		srcs, dictExternal, err := resolveDictSources(ctx, conn, database, dicts)
		if err != nil {
			return Resolution{}, err
		}
		external = external || dictExternal
		tables = append(tables, srcs...)
	}
	slices.Sort(tables)
	return Resolution{Tables: slices.Compact(tables), Pruned: pruned > 0, External: external}, nil
}

// capture tracks whether the previous line armed extraction of the next string
// constant — joinGet/dictGet render their target as the function's first argument,
// a NAME constant, rather than a TABLE node.
const (
	capNone = iota
	capJoin // next string constant is a Join-engine TABLE name
	capDict // next string constant is a DICTIONARY name
)

// qscope is one QUERY node in the tree, tracked by indentation. dead is set when
// the node's WHERE folded to a constant false (the arm reads nothing). Scopes are
// retained for the whole parse so a TABLE seen BEFORE its arm's WHERE can still have
// its deadness resolved at the end.
type qscope struct {
	indent int
	dead   bool
}

// tableOccurrence records one TABLE node plus the QUERY scope stack enclosing it,
// so its deadness (any enclosing arm dead) is evaluated after the full parse.
type tableOccurrence struct {
	table  string
	scopes []int // indices into the scopes slice
}

// parseExplainTables extracts dependency names from EXPLAIN QUERY TREE output,
// returning (tables, dicts, prunedCount, external):
//
//   - tables: configured-database table/view names from TABLE nodes (minus
//     dead-branch arms) plus the first (NAME) argument of joinGet/joinGetOrNull.
//   - dicts: configured-database dictionary names referenced via a dictGet-family
//     function, for the caller to map to their backing tables.
//   - prunedCount: distinct tables dropped because every occurrence was in a
//     constant-false arm (for the metric).
//   - external: the query reads something no local table version can watch — a
//     TABLE_FUNCTION node (s3()/numbers()/url()/…), a TABLE node that doesn't
//     resolve to a configured-database name (a cross-database table, or an
//     unrecognized rendering — conservative: anything we can't attribute counts
//     as external, so the caller TTL-caps rather than trusts), or a
//     cross-database joinGet/dictGet target.
//
// Structure (verified against ClickHouse 26.6): a UNION arm is a QUERY node; its
// tables sit under JOIN TREE and its filter under a sibling WHERE. ClickHouse folds
// a parameter-gated predicate, so a dead arm's WHERE child is exactly
// "CONSTANT … UInt64_0" — and ONLY the line immediately after WHERE is inspected, so
// a real predicate ("WHERE → FUNCTION greater") or a literal compare
// ("WHERE → FUNCTION equals(x, 0)") is never mistaken for a dead arm. A table is
// dropped only when EVERY occurrence is under a dead arm; appearing in any live arm
// keeps it. Kept pure (no ClickHouse) so the brittle string handling is unit-tested.
func parseExplainTables(lines []string, database string) (tables, dicts []string, prunedCount int, external bool) {
	var scopes []qscope
	var stack []int // indices into scopes for currently-open QUERY nodes
	var occs []tableOccurrence
	dictSet := map[string]struct{}{}
	joinTables := map[string]struct{}{} // joinGet targets: always kept (safe), never pruned
	capture := capNone
	afterWhere := false

	for _, raw := range lines {
		trimmed := strings.TrimLeft(raw, " ")
		if trimmed == "" {
			continue
		}
		indent := len(raw) - len(trimmed)

		// Close any QUERY scopes we have exited (this line is at or shallower than them).
		for len(stack) > 0 && scopes[stack[len(stack)-1]].indent >= indent {
			stack = stack[:len(stack)-1]
		}

		// The single line after a WHERE is its condition root. If that root is the
		// folded constant false, this arm (the enclosing QUERY) is dead. Checking only
		// this one line is what makes pruning safe: a real predicate or an "x = 0"
		// compare renders as a FUNCTION here, not a bare CONSTANT.
		//
		// CH-VERSION COUPLING: the literal below is ClickHouse 26.6's rendering of a
		// false-folded predicate. A future version could fold to a different type
		// (Bool_0/UInt8_0) or drop the trailing comma; either way this test stops
		// matching and pruning silently STOPS — which fails SAFE (the arm's tables
		// stay tracked, over-resolve, never a wrong prune). The regression signal is
		// tests/integration's pruning test, which asserts a parameter-gated UNION
		// actually prunes against the ClickHouse version we ship, plus the
		// wavehouse_pipe_dep_tables_pruned_total metric going flat in production.
		if afterWhere {
			afterWhere = false
			if len(stack) > 0 && strings.Contains(raw, "constant_value: UInt64_0,") {
				scopes[stack[len(stack)-1]].dead = true
			}
			// Fall through: the WHERE condition root may itself be a joinGet/dictGet
			// (e.g. WHERE joinGet(...)), which still needs tracking.
		}

		switch {
		case strings.HasPrefix(trimmed, "QUERY id:"):
			scopes = append(scopes, qscope{indent: indent})
			stack = append(stack, len(scopes)-1)
			capture = capNone
			continue
		case trimmed == "WHERE" || strings.HasPrefix(trimmed, "WHERE "):
			afterWhere = true
			capture = capNone
			continue
		}

		// A table function (s3()/numbers()/url()/…) is its own node type: a read no
		// local table version can watch, so it marks the resolution external.
		if strings.HasPrefix(trimmed, "TABLE_FUNCTION ") {
			external = true
			capture = capNone
			continue
		}

		// TABLE node — record with its enclosing scope stack (deadness resolved later).
		// Gate on the node type ("TABLE ...") so a string-literal value that merely
		// contains "table_name:" (a projected label, or a bound parameter value) can't
		// be mistaken for a table reference.
		if strings.HasPrefix(trimmed, "TABLE ") {
			tracked := false
			if _, after, found := strings.Cut(raw, "table_name: "); found {
				// table_name is NOT always the last field on the line: FINAL appends
				// ", final: 1" and SAMPLE ", final: 0, sample_size: …". Keep only the
				// qualified name up to the first comma, else the modifier text leaks in.
				name, _, _ := strings.Cut(after, ",")
				if db, table, ok := strings.Cut(strings.TrimSpace(name), "."); ok && db == database {
					occs = append(occs, tableOccurrence{table: table, scopes: append([]int(nil), stack...)})
					tracked = true
				}
			}
			// A TABLE node that doesn't resolve to a configured-database name — a
			// cross-database table, or a rendering we don't recognize — is a read we
			// can't version-watch: external, conservatively.
			if !tracked {
				external = true
			}
			capture = capNone
			continue
		}

		// joinGet/dictGet: arm capture of the next string constant (the target name).
		// The matches are PREFIX matches ("function_name: dictGet" also hits
		// dictGetOrDefault, dictGetHierarchy, dictGetChildren, …) and that is
		// deliberate: every dictGet*-family function reads the dictionary, so there
		// is no dictGet* we want to exclude. dictHas and dictIsIn are the only
		// read-bearing dict functions outside that prefix.
		switch {
		case strings.Contains(raw, "function_name: joinGet"):
			capture = capJoin
			continue
		case strings.Contains(raw, "function_name: dictGet"),
			strings.Contains(raw, "function_name: dictHas"),
			strings.Contains(raw, "function_name: dictIsIn"):
			capture = capDict
			continue
		}
		if capture != capNone {
			if val, isStr, found := constantString(raw); found {
				if isStr {
					if db, name, ok := strings.Cut(val, "."); ok {
						if db == database {
							addCaptured(capture, name, joinTables, dictSet)
						} else {
							// A cross-database joinGet/dictGet target: same class of
							// unwatchable read as a cross-database TABLE node.
							external = true
						}
					} else {
						addCaptured(capture, val, joinTables, dictSet) // bare name -> configured db
					}
				}
				capture = capNone
			}
		}
	}

	// Resolve table deadness: a table is kept if it has at least one occurrence with
	// no dead enclosing arm. A table dropped here (all occurrences dead) is counted.
	tableSet := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, o := range occs {
		seen[o.table] = struct{}{}
		dead := false
		for _, si := range o.scopes {
			if scopes[si].dead {
				dead = true
				break
			}
		}
		if !dead {
			tableSet[o.table] = struct{}{}
		}
	}
	for t := range seen {
		if _, kept := tableSet[t]; !kept {
			prunedCount++
		}
	}
	for t := range joinTables {
		tableSet[t] = struct{}{}
	}
	return setSlice(tableSet), setSlice(dictSet), prunedCount, external
}

// addCaptured routes a captured NAME to the table or dict set per the armed kind.
func addCaptured(kind int, name string, tables, dicts map[string]struct{}) {
	switch kind {
	case capJoin:
		tables[name] = struct{}{}
	case capDict:
		dicts[name] = struct{}{}
	}
}

// constantString parses a "constant_value: <v>" line. found reports the line is a
// constant node; isStr reports the value is a quoted String (the only kind that can
// be a table/dict name) and val is its unquoted content. A non-string constant
// (e.g. "UInt64_1") returns isStr=false so the caller stops capturing.
func constantString(ln string) (val string, isStr, found bool) {
	_, after, ok := strings.Cut(ln, "constant_value: ")
	if !ok {
		return "", false, false
	}
	after = strings.TrimSpace(after)
	if !strings.HasPrefix(after, "'") {
		return "", false, true // e.g. UInt64_0 — a non-name constant
	}
	if i := strings.IndexByte(after[1:], '\''); i >= 0 {
		return after[1 : 1+i], true, true
	}
	return "", false, true // unterminated quote — malformed, ignore
}

// resolveDictSources maps the named dictionaries to their backing ClickHouse
// tables. system.dictionaries.source renders a CLICKHOUSE source as
// "ClickHouse: <db>.<table>"; only a source table in the configured database is
// trackable (a write to it evicts). Other source kinds (file/http/executable) and
// cross-database sources yield no local table, like a table function.
//
// A NOT_LOADED dictionary reports an EMPTY source — under ClickHouse's default lazy
// loading a dict stays unloaded until first used, so resolving a dictGet pipe before
// the dict's first load would silently drop its backing table (→ stale reads on
// writes to it). So any referenced-but-unloaded dict is force-loaded once, then
// re-read. Best-effort: a dict that can't load is left untracked rather than failing
// the whole resolution.
//
// external reports that at least one referenced dictionary yielded no trackable
// local table — a file/http/executable or cross-database source, a dict that
// couldn't load, or one missing from system.dictionaries entirely. Writes to such
// a source can never evict, so the caller TTL-caps the result.
func resolveDictSources(ctx context.Context, conn driver.Conn, database string, dicts []string) (out []string, external bool, err error) {
	want := make(map[string]struct{}, len(dicts))
	for _, d := range dicts {
		want[d] = struct{}{}
	}

	readSources := func() (map[string]string, error) {
		rows, err := conn.Query(ctx, "SELECT name, source FROM system.dictionaries WHERE database = ?", database)
		if err != nil {
			return nil, fmt.Errorf("query system.dictionaries: %w", err)
		}
		defer func() { _ = rows.Close() }()
		srcs := make(map[string]string, len(want))
		for rows.Next() {
			var name, source string
			if err := rows.Scan(&name, &source); err != nil {
				return nil, fmt.Errorf("scan dictionary row: %w", err)
			}
			if _, ok := want[name]; ok {
				srcs[name] = source
			}
		}
		return srcs, rows.Err()
	}

	sources, err := readSources()
	if err != nil {
		return nil, false, err
	}

	loaded := false
	for name, source := range sources {
		if source != "" {
			continue // already loaded
		}
		stmt := "SYSTEM RELOAD DICTIONARY " + chsql.QuoteIdent(database) + "." + chsql.QuoteIdent(name)
		if err := conn.Exec(ctx, stmt); err == nil {
			loaded = true
		}
	}
	if loaded {
		if sources, err = readSources(); err != nil {
			return nil, false, err
		}
	}

	for name := range want {
		source, found := sources[name]
		if !found {
			external = true // not in system.dictionaries at all — nothing to watch
			continue
		}
		if table, ok := parseDictSourceTable(source, database); ok {
			out = append(out, table)
		} else {
			external = true // file/http/executable or cross-database source, or still unloaded
		}
	}
	return out, external, nil
}

// parseDictSourceTable extracts the backing table from a system.dictionaries
// source string, returning ok=false for a non-CLICKHOUSE source or a source in a
// different database (neither is trackable here).
func parseDictSourceTable(source, database string) (string, bool) {
	const prefix = "ClickHouse: "
	if !strings.HasPrefix(source, prefix) {
		return "", false
	}
	qualified := strings.TrimSpace(strings.TrimPrefix(source, prefix))
	db, table, ok := strings.Cut(qualified, ".")
	if !ok {
		return qualified, true // bare table name -> configured db
	}
	if db != database {
		return "", false
	}
	return table, true
}

// setSlice returns the set's keys as a sorted slice (nil when empty).
func setSlice(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}
