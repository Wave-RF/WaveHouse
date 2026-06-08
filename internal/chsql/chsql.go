// Package chsql holds ClickHouse SQL helpers shared across packages that build
// SQL — primarily safe identifier quoting. It is dependency-free so both
// internal/query and internal/policy can use it without an import cycle.
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
// Values are never quoted here; they remain positional `?` parameters bound by
// the driver.
func QuoteIdent(name string) string {
	return "`" + identEscaper.Replace(name) + "`"
}

// BindUnsafe reports whether an identifier contains a character that
// clickhouse-go's positional binder miscounts. Today that is just '?': the
// driver counts every '?' in the query text — even inside a backtick-quoted
// identifier — so a name containing '?' would shift the value parameters that
// follow it. Callers refuse such names (fail closed) rather than risk mis-binding
// a value (including a row-level-security filter value). Pathological; no real
// schema names a column '?'. Tracked in Wave-RF/WaveHouse#279.
func BindUnsafe(name string) bool {
	return strings.ContainsRune(name, '?')
}
