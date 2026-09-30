// Package chsql holds ClickHouse SQL helpers shared across packages that build
// SQL: safe identifier quoting, the encoding a value needs to survive a
// `{p:String}` query parameter, and the strict cast an integer column's claims
// are compared through. It is dependency-free so internal/query,
// internal/policy and internal/typelayer can all use it without an import
// cycle.
package chsql

import "strings"

// identEscaper is ClickHouse's backQuote() escaping, byte for byte as the
// chtypes library's QuoteIdentifier returns it (measured over all 256 bytes;
// typelayer's parity test pins it): `\\`, `\“, and `\0 \b \t \n \f \r`
// for NUL and those control characters. One left-to-right pass, so no
// replacement re-processes another's output.
var identEscaper = strings.NewReplacer(
	`\`, `\\`, "`", "\\`",
	"\x00", `\0`, "\b", `\b`, "\t", `\t`, "\n", `\n`, "\f", `\f`, "\r", `\r`,
)

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

// IntType is one of ClickHouse's integer type names, as IntegerType returns
// it. It is a distinct type so StrictInt can only be handed a name from that
// closed set, never text read off a schema.
type IntType string

// intTypes is the closed set IntegerType answers from. Bool is UInt8
// underneath but compares as a boolean, and Enum/Decimal are not integers, so
// none of them is here.
var intTypes = map[string]IntType{
	"UInt8": "UInt8", "UInt16": "UInt16", "UInt32": "UInt32", "UInt64": "UInt64",
	"UInt128": "UInt128", "UInt256": "UInt256",
	"Int8": "Int8", "Int16": "Int16", "Int32": "Int32", "Int64": "Int64",
	"Int128": "Int128", "Int256": "Int256",
}

// IntegerType reports whether colType — a ClickHouse type as system.columns
// and the chtypes library spell it — is an integer type, possibly wrapped in
// Nullable(...) and/or LowCardinality(...), and returns the bare integer type.
//
// It only picks which expression form a policy claim is compared through
// (StrictInt for an integer column, a plain {p:String} for everything else);
// it models no ClickHouse semantics. The wrappers are stripped because
// accurateCastOrNull to a LowCardinality type is refused by the server (code
// 455) and the bare type answers identically on a Nullable column (measured
// on 26.6.3.62 and the 26.6 chtypes artifact). A type it does not recognise
// keeps the {p:String} form.
func IntegerType(colType string) (IntType, bool) {
	t := colType
	for {
		inner, ok := unwrap(t, "Nullable(")
		if !ok {
			inner, ok = unwrap(t, "LowCardinality(")
		}
		if !ok {
			break
		}
		t = inner
	}
	it, ok := intTypes[t]
	return it, ok
}

func unwrap(t, prefix string) (string, bool) {
	if strings.HasPrefix(t, prefix) && strings.HasSuffix(t, ")") {
		return t[len(prefix) : len(t)-1], true
	}
	return "", false
}

// StrictInt renders the round-trip strict cast an integer column compares a
// claim bound as {param:String} against:
//
//	if(toString(accurateCastOrNull({p:String}, 'T')) = {p:String}, accurateCastOrNull({p:String}, 'T'), NULL)
//
// A bare {p:String} wraps a value at or past 2^64 on every integer column (and
// a 128/256-bit column at its own width), and accurateCastOrNull alone still
// wraps on [U]Int128/[U]Int256. The round trip turns every value that is not
// the canonical spelling of an in-range integer into NULL, which no operator
// admits, while an in-range canonical value compares exactly as before and
// keeps the primary key in use. Measured identical on ClickHouse 26.6.3.62 and
// the 26.6/25.8 chtypes artifacts.
func StrictInt(param string, t IntType) string {
	cast := "accurateCastOrNull({" + param + ":String}, '" + string(t) + "')"
	return "if(toString(" + cast + ") = {" + param + ":String}, " + cast + ", NULL)"
}

// IntParam is a positional value the query builder binds through StrictInt
// instead of as a bare {pN:String}: one parameter, referenced from the
// expression the placeholder expands to.
type IntParam struct {
	Value string
	Type  IntType
}
