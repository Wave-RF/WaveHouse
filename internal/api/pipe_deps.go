package api

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/query"
)

// pipeResolveTimeout bounds the EXPLAIN + view-expansion round-trips a pipe Put()
// makes to resolve its table dependencies, so a slow or stuck ClickHouse can't
// hang pipe creation. Generous because EXPLAIN QUERY TREE only analyzes (it scans
// no data) and Put() is a rare admin write, not a per-request cost.
const pipeResolveTimeout = 10 * time.Second

// resolvePipeDeps resolves the ingested base tables a pipe reads, for cache
// invalidation. Best-effort: if no schema registry / ClickHouse connection is
// wired, or resolution fails, it returns nil and the pipe falls back to TTL-only
// caching. Because the table set depends only on the SQL (not on per-request
// parameter values), resolving here at Put() — a rare admin write — keeps it off
// the per-request read path and means it is computed once per definition.
//
// Resolution runs once and is not repeated, so a table created (or a view redefined)
// after the pipe is saved is not picked up until the pipe is next saved; until then
// the pipe stays TTL-only for that table.
func (h *PipesHandler) resolvePipeDeps(ctx context.Context, q *pipes.NamedQuery) []string {
	if h.Registry == nil || h.CHConn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, pipeResolveTimeout)
	defer cancel()
	tables := resolvePipeTables(ctx, h.CHConn, h.Registry, h.Database, q)
	// Surface the outcome: an operator can otherwise not tell which pipes are
	// write-invalidated and which silently fell back to TTL-only — because resolution
	// failed, or because the pipe reads only things WaveHouse has no change signal for
	// (views, table functions, other databases, system tables).
	if len(tables) == 0 {
		h.logger.InfoContext(ctx, "pipe has no invalidatable table dependencies; results cache TTL-only", "pipe", q.Name)
	} else {
		h.logger.DebugContext(ctx, "pipe table dependencies resolved", "pipe", q.Name, "tables", tables)
	}
	return tables
}

// resolvePipeTables binds the pipe with dummy parameters, asks ClickHouse to
// resolve the query to the tables it actually reads — resolving aliases,
// subqueries, and JOINs via EXPLAIN QUERY TREE and expanding any view to its base
// tables via collectReadTables — then keeps only the names the schema registry
// knows in the configured database (i.e. tables the ingest worker writes and can
// therefore version-invalidate). Any failure returns nil/empty so the caller
// degrades to TTL-only with no error surfaced.
func resolvePipeTables(ctx context.Context, conn driver.Conn, registry *discovery.SchemaRegistry, database string, q *pipes.NamedQuery) []string {
	if conn == nil || registry == nil || q == nil {
		return nil
	}
	boundSQL, err := pipes.DummyBind(q)
	if err != nil {
		return nil
	}
	raw := collectReadTables(ctx, conn, database, boundSQL, map[string]struct{}{})
	return filterKnownTables(raw, database, registry)
}

// maxViewExpansions backstops view→view expansion against a pathological chain;
// the visited set already prevents cycles, this just bounds total work.
const maxViewExpansions = 32

// collectReadTables runs EXPLAIN QUERY TREE over sql and returns the raw
// `database.table` identifiers it references, recursively expanding any normal
// VIEW to the tables in its own definition. EXPLAIN QUERY TREE does NOT inline a
// normal view — it reports the view itself as the table read (verified on
// ClickHouse 26) — so a pipe that reads a view would otherwise resolve to nothing
// once the view is filtered out. We expand views ourselves via
// system.tables.as_select. visited guards against view cycles and repeated work;
// identifiers in another database and non-view unknowns are returned as-is for
// filterKnownTables to drop.
func collectReadTables(ctx context.Context, conn driver.Conn, database, sql string, visited map[string]struct{}) []string {
	if len(visited) > maxViewExpansions {
		return nil
	}
	refs, err := explainQueryTreeTables(ctx, conn, sql)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r)
		db, table := splitQualified(r)
		if table == "" || (db != "" && db != database) {
			continue // another database — outside the ingest universe
		}
		if _, seen := visited[table]; seen {
			continue
		}
		visited[table] = struct{}{}
		// Expand any view to its definition and recurse — and deliberately do NOT
		// shortcut on schema-registry membership ("known, so a base table, stop").
		// The registry is built from system.columns, which lists views and
		// materialized views too, so once auto-refresh has discovered a view it would
		// be treated as terminal and never expanded — keying the pipe on the view/MV
		// name instead of the ingest-written source table, so an ingest would never
		// invalidate the cached result. viewAsSelect is the authority here: "" for a
		// base table, the defining SELECT for a view.
		if as := viewAsSelect(ctx, conn, database, table); as != "" {
			out = append(out, collectReadTables(ctx, conn, database, as, visited)...)
		}
	}
	return out
}

// viewAsSelect returns the defining SELECT of a view in the configured database,
// or "" if name is not a view (a base table has an empty as_select). It is how we
// expand a view to its underlying tables, since EXPLAIN QUERY TREE does not inline
// views. Best-effort: any error (e.g. an older server without the column) returns
// "", so the view simply stays unresolved (TTL-only).
func viewAsSelect(ctx context.Context, conn driver.Conn, database, name string) string {
	rows, err := conn.Query(ctx,
		"SELECT as_select FROM system.tables WHERE database = ? AND name = ? AND as_select != ''",
		database, name)
	if err != nil {
		return ""
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return ""
	}
	var as string
	if err := rows.Scan(&as); err != nil {
		return ""
	}
	return as
}

// explainQueryTreeTables runs EXPLAIN QUERY TREE over sql and returns the raw
// `database.table` identifiers it references. Running the analyzer's passes
// (run_passes = 1) resolves aliases and finds tables inside subqueries, CTEs, and
// JOINs — which static FROM-clause parsing cannot do, the whole reason this goes
// through ClickHouse. It does NOT inline a normal view (the view is reported as
// the table read); collectReadTables handles view expansion. Read-only, so it is
// safe to issue for any pipe (pipes are read queries).
func explainQueryTreeTables(ctx context.Context, conn driver.Conn, sql string) ([]string, error) {
	rows, err := conn.Query(ctx, "EXPLAIN QUERY TREE run_passes = 1 "+sql)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parseQueryTreeTables(lines), nil
}

// parseQueryTreeTables pulls table identifiers out of an EXPLAIN QUERY TREE dump.
// After the analyzer passes run, every table the query reads — including those
// reached through aliases, subqueries, CTEs, and JOINs — appears as a TABLE node
// carrying a `table_name: <database>.<table>` field (a referenced view appears as
// its own TABLE node; collectReadTables expands it). We collect those raw
// identifiers; the database qualifier and backtick quoting are handled by
// filterKnownTables. A table function (merge, remote, cluster, s3, url, numbers) or a
// dictionary (dictGet) is not a TABLE node, so its read targets are not resolved and
// a pipe that reads only through one falls back to TTL-only.
func parseQueryTreeTables(lines []string) []string {
	const marker = "table_name:"
	var out []string
	for _, line := range lines {
		_, after, found := strings.Cut(line, marker)
		if !found {
			continue
		}
		if id := identifierField(after); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// identifierField returns the leading identifier of a query-tree field value:
// everything up to the first comma that is not inside backticks, trimmed. The
// dump separates a node's fields with ", " and quotes identifiers containing
// special characters with backticks, so a naive comma split would truncate a
// quoted name that itself contains a comma. A backslash-escaped backtick (\`)
// inside a quoted name does not end the quoting.
func identifierField(s string) string {
	s = strings.TrimSpace(s)
	inTick := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if inTick {
				i++ // skip the escaped char (\` or \\) so it can't flip inTick
			}
		case '`':
			inTick = !inTick
		case ',':
			if !inTick {
				return strings.TrimSpace(s[:i])
			}
		}
	}
	return strings.TrimSpace(s)
}

// filterKnownTables normalizes raw `database.table` identifiers to bare table
// names and keeps only those the schema registry knows in the configured
// database — i.e. tables the ingest worker writes and can version-invalidate. A
// qualifier naming a different database is dropped (ingest can't signal changes
// to it), as is anything the registry hasn't discovered (views, system tables,
// unknown names): a pipe may still read those, but WaveHouse has no change signal
// for them, so they stay covered by the TTL floor. Results are deduped and
// sorted for a stable, order-independent dependency set.
func filterKnownTables(raw []string, database string, registry *discovery.SchemaRegistry) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		db, table := splitQualified(r)
		if db != "" && db != database {
			continue // a table in another database — outside the ingest universe
		}
		if table == "" || registry.Get(table) == nil {
			continue
		}
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		out = append(out, table)
	}
	sort.Strings(out)
	return out
}

// splitQualified splits a ClickHouse `database.table` (or bare `table`)
// identifier into its parts, tolerating the backtick quoting ClickHouse uses for
// names with special characters. Only the first dot outside backticks separates
// the database qualifier, so a quoted table name containing dots stays intact. A
// backslash-escaped backtick (\`) inside a quoted part does not end the quoting.
// A bare name returns db "".
func splitQualified(s string) (db, table string) {
	s = strings.TrimSpace(s)
	inTick := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if inTick {
				i++ // skip the escaped char (\` or \\) so it can't flip inTick
			}
		case '`':
			inTick = !inTick
		case '.':
			if !inTick {
				return unquoteIdent(s[:i]), unquoteIdent(s[i+1:])
			}
		}
	}
	return "", unquoteIdent(s)
}

// unquoteIdent strips ClickHouse backtick quoting from a single identifier and
// unescapes the `\“ and `\\` sequences inside it, so the result matches the
// bare name the schema registry is keyed by.
func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		s = s[1 : len(s)-1]
		s = strings.ReplaceAll(s, "\\`", "`")
		s = strings.ReplaceAll(s, "\\\\", "\\")
	}
	return s
}

// pipeDeps maps a pipe's resolved base tables to cache dependency namespaces.
// The scope is empty (whole-table): a pipe can't know which scope of a table it
// reads, and a scoped write bumps the whole-table view too, so a whole-table
// dependency is still correctly invalidated by any write to the table. Each name
// is NATS-encoded exactly as the ingest worker encodes it (worker.go
// handleSuccess), so the read and invalidation sides build identical keys. nil
// in, nil out — keeping pipes keyed by sha alone (TTL-only).
func pipeDeps(tables []string) []cache.Namespace {
	if len(tables) == 0 {
		return nil
	}
	deps := make([]cache.Namespace, 0, len(tables))
	for _, t := range tables {
		deps = append(deps, cache.Namespace{Table: query.SafeEncodeNATS(t)})
	}
	return deps
}
