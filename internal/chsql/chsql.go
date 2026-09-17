// Package chsql holds ClickHouse SQL helpers shared across packages that build
// SQL: safe identifier quoting, and the encoding a value needs to survive a
// `{p:String}` query parameter. It is dependency-free so internal/query,
// internal/policy and internal/typelayer can all use it without an import
// cycle.
package chsql

import "strings"

// identEscaper escapes a ClickHouse identifier's special characters exactly as
// ClickHouse's own backQuote() does (confirmed against SHOW CREATE TABLE on a
// live server): a backslash becomes `\\` and a backtick becomes “ \` “. The
// two replacements run in a single left-to-right pass, so neither re-processes
// the other's output.
var identEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

// QuoteIdent renders any value as a backtick-quoted ClickHouse identifier
// (column, table, or alias). It is the single place an identifier becomes SQL
// text, which is what lets callers accept any name a customer's existing schema
// actually uses — dots, spaces, unicode, keywords, even embedded
// backticks/backslashes.
//
// We ALWAYS quote, never emit a bare identifier for names that "look safe":
// ClickHouse treats `x` and x identically, so quoting is free semantically, and
// the "looks safe" test is more than the identifier regex — reserved words like
// `all` and `distinct` match the regex yet are syntax errors when unquoted
// (verified on a live server). Always quoting is unconditionally correct and
// needs no keyword table.
//
// A value is never rendered into SQL text here; it binds as a query parameter
// and is encoded by EscapeStringParam.
func QuoteIdent(name string) string {
	return "`" + identEscaper.Replace(name) + "`"
}

// BindUnsafe reports whether an identifier contains a character that the
// positional-placeholder scan miscounts. Today that is just '?': the scan that
// rewrites a built query's `?` placeholders into named `{pN:…}` parameters
// walks the SQL text left to right and cannot tell a placeholder from a '?'
// inside a backtick-quoted identifier, so a name containing '?' would shift
// every value that follows it. Callers refuse such names (fail closed) rather
// than risk mis-binding a value (including a row-level-security filter value).
// Pathological; no real schema names a column '?'. Tracked in
// Wave-RF/WaveHouse#279.
func BindUnsafe(name string) bool {
	return strings.ContainsRune(name, '?')
}

// EscapeStringParam encodes one value for a ClickHouse `{p:String}` query
// parameter — the SINGLE encoding both read surfaces use, so the SQL path
// (internal/query, over the HTTP interface) and the row-filter path
// (internal/typelayer, over the chtypes artifact) cannot disagree about what a
// claim value is.
//
// ClickHouse reads a scalar parameter with its ESCAPED-TEXT reader, so a raw
// backslash starts an escape sequence and a raw tab or newline ends the field.
// Measured on 26.6.3.62 over the HTTP interface: `param_p0=a\b` came back
// holding a backspace with no error, and a raw tab or newline was a hard code
// 457 parse error (a 500 for the caller). Measured on the 26.6 chtypes
// artifact through typelayer's compiled filters: the same stored values were
// answered false (the backslash case) or refused at compile time (tab,
// newline, a trailing backslash), and the same encoding made all of them
// compare equal. Encoding `\` → `\\`, tab → `\t`, newline → `\n`, CR → `\r`
// round-trips every value byte for byte on both surfaces, including an
// embedded NUL.
//
// An Array(String) parameter takes a DIFFERENT rule and must NOT be run
// through this one: its elements are read as QUOTED values, where a raw tab or
// newline rides through untouched and only `'` and `\` need escaping.
// Applying both encodings is corruption — `a\b` becomes `a\\b`. See
// quoteCHElement in internal/query.
var EscapeStringParam = strings.NewReplacer(
	`\`, `\\`,
	"\t", `\t`,
	"\n", `\n`,
	"\r", `\r`,
).Replace
