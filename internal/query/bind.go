package query

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
)

// ─── ClickHouse binding ──────────────────────────────────────────────────────

// Bound is a built query in the form ClickHouse's HTTP interface takes: the
// SQL with named placeholders, the query parameters they read, and the
// external tables its `in` lists read.
type Bound struct {
	SQL string
	// Params supply param_<Name>, already encoded for ClickHouse's parameter
	// reader, in placeholder order.
	Params []Param
	// Tables are the `in` lists, in placeholder order.
	Tables []Table
}

// Param is one query parameter: the value of param_<Name>.
type Param struct {
	Name, Value string
}

// Table is one `in` list sent as a ClickHouse external table called Name:
// TableStructure, one row per element, in TableFormat — each element's bytes
// as written, length-prefixed, so nothing in a value needs escaping.
type Table struct {
	Name string
	Data []byte
}

// The shape of every Table, which the SQL reads as `SELECT … v FROM <Name>`.
const (
	TableStructure = "v String"
	TableFormat    = "RowBinary"
)

// listParam is an `in` list: one external table, its elements converted to
// the column's type in the subquery that reads it.
type listParam struct {
	Values []any
	Conv   conversion
}

// convertedParam is a scalar bound as {pN:String} whose placeholder expands
// to Conv's expression around it.
type convertedParam struct {
	Value any
	Conv  conversion
}

// conversionKind is how ClickHouse turns a bound String into a column's type.
type conversionKind int

const (
	// asString leaves it to ClickHouse's comparison, which converts a String
	// operand to the column's type. An `in` list on such a column is a set of
	// Strings, which only a String column compares as typed.
	asString conversionKind = iota
	// parseTime parses with parseDateTime64BestEffort: an offset or Z gives
	// the exact instant, and a zone-less value reads in the column's declared
	// zone, else the server's — as it does compared to the column directly.
	parseTime
	// parseDate and parseDate32 take the calendar date of parseTime's
	// instant, in the server's zone.
	parseDate
	parseDate32
	// castTo converts each element of an `in` list to the column's type with
	// accurateCastOrNull. A subquery's set keeps its own type and ClickHouse
	// casts the column to it, so a set of Strings would compare text:
	// Decimal 12.50 against '12.50' missed (measured on 26.8.15.10). CAST
	// instead wrapped '256' to 0 on a UInt8 column; accurateCastOrNull turns
	// it into NULL, which matches nothing, as the literal list did.
	castTo
)

// conversion is how ClickHouse turns a filter's bound String into the
// filtered column's type, read off the column's type in the schema.
type conversion struct {
	kind  conversionKind
	scale int    // parseTime: the parse's scale
	zone  string // parseTime: the column's declared zone, "" for the server's
	typ   string // castTo: the column's type without Nullable/LowCardinality
}

// The scales a parseTime conversion parses at. Fine enough to compare any
// value exactly at the column's precision; scale 9 is used only for a
// DateTime64(9) column, because it tops out at 2262-04-11 and a
// DateTime64(3) value past that failed the whole comparison with
// DECIMAL_OVERFLOW (measured on 24.8.14.39 and 26.8.15.10), where scale 8
// holds DateTime64's whole range. Scale 0 is no use: it parses into a
// DateTime, so `>= …04:00:00.5Z` admitted 04:00:00, and it floors at 1970.
const (
	timeScale     = 8
	timeScaleNano = 9
)

// conversionFor picks the conversion for a column of type colType, as
// system.columns spells it. A type it cannot read keeps asString, which
// leaves ClickHouse to refuse what it cannot compare rather than guess a
// zone.
func conversionFor(colType string) conversion {
	t := colType
	for {
		inner, ok := unwrapType(t, "Nullable(")
		if !ok {
			inner, ok = unwrapType(t, "LowCardinality(")
		}
		if !ok {
			break
		}
		t = inner
	}
	switch {
	case t == "Date":
		return conversion{kind: parseDate}
	case t == "Date32":
		return conversion{kind: parseDate32}
	case t == "DateTime":
		return conversion{kind: parseTime, scale: timeScale}
	case strings.HasPrefix(t, "DateTime(") || strings.HasPrefix(t, "DateTime64("):
		if c, ok := dateTimeConversion(t); ok {
			return c
		}
		return conversion{kind: asString}
	case t == "String" || t == "":
		return conversion{kind: asString}
	default:
		return conversion{kind: castTo, typ: t}
	}
}

// dateTimeConversion reads `DateTime('zone')`, `DateTime64(p)` and
// `DateTime64(p, 'zone')`.
func dateTimeConversion(t string) (conversion, bool) {
	open := strings.IndexByte(t, '(')
	if !strings.HasSuffix(t, ")") {
		return conversion{}, false
	}
	args := strings.Split(t[open+1:len(t)-1], ",")
	c := conversion{kind: parseTime, scale: timeScale}
	zoneArg := ""
	if strings.HasPrefix(t, "DateTime64(") {
		if len(args) > 2 {
			return conversion{}, false
		}
		p, err := strconv.Atoi(strings.TrimSpace(args[0]))
		if err != nil {
			return conversion{}, false
		}
		if p == 9 {
			c.scale = timeScaleNano
		}
		if len(args) == 2 {
			zoneArg = args[1]
		}
	} else {
		if len(args) != 1 {
			return conversion{}, false
		}
		zoneArg = args[0]
	}
	if zoneArg != "" {
		z := strings.TrimSpace(zoneArg)
		if len(z) < 2 || z[0] != '\'' || z[len(z)-1] != '\'' || strings.ContainsAny(z[1:len(z)-1], `'\`) {
			return conversion{}, false
		}
		c.zone = z[1 : len(z)-1]
	}
	return c, true
}

func unwrapType(t, prefix string) (string, bool) {
	if strings.HasPrefix(t, prefix) && strings.HasSuffix(t, ")") {
		return t[len(prefix) : len(t)-1], true
	}
	return "", false
}

// scalar is v as Build binds it for this column: wrapped when ClickHouse must
// parse it, as written otherwise.
func (c conversion) scalar(v any) any {
	switch c.kind {
	case parseTime, parseDate, parseDate32:
		return convertedParam{Value: v, Conv: c}
	case asString, castTo:
	}
	return v
}

// expr is the SQL converting arg, a String, to the column's type. For
// asString and castTo it is a scalar's own placeholder — ClickHouse's
// comparison converts it — and only an `in` element is cast (element).
func (c conversion) expr(arg string) string {
	switch c.kind {
	case parseTime:
		if c.zone == "" {
			return fmt.Sprintf("parseDateTime64BestEffort(%s, %d)", arg, c.scale)
		}
		return fmt.Sprintf("parseDateTime64BestEffort(%s, %d, %s)", arg, c.scale, sqlString(c.zone))
	case parseDate:
		return fmt.Sprintf("toDate(parseDateTime64BestEffort(%s, %d))", arg, timeScale)
	case parseDate32:
		return fmt.Sprintf("toDate32(parseDateTime64BestEffort(%s, %d))", arg, timeScale)
	case asString, castTo:
	}
	return arg
}

// element is the SQL converting one `in` element, the String arg, to the
// column's type.
func (c conversion) element(arg string) string {
	if c.kind == castTo {
		return "accurateCastOrNull(" + arg + ", " + sqlString(c.typ) + ")"
	}
	return c.expr(arg)
}

// sqlString renders s as a ClickHouse string literal. Only a schema's own
// type and zone names reach it, never a caller's value.
func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// Bind rewrites the positional `?` placeholders in the built SQL into the
// form ClickHouse's HTTP interface takes, and renders each bound value as the
// text ClickHouse reads it back from. Placeholder i is named pN with N = i:
//
//   - a scalar binds as {pN:String}. String is not a weaker binding than the
//     column's own type: ClickHouse converts the parameter to the column's
//     type for the comparison, so `UInt8 = {p:String}` with "256" is false
//     and with "1.5" is a type error, matching what the server answers for
//     the same literal. One rule, shared with the row filters the type layer
//     compiles.
//   - a value on a Date or DateTime column binds the same way and expands to
//     the parse its conversion names, so ClickHouse reads the caller's own
//     spelling — an RFC 3339 instant, or a zone-less time in the column's
//     zone — instead of Go rewriting it. Compared directly, RFC 3339 is
//     read only under cast_string_to_date_time_mode=best_effort: the
//     default on 26.8.15.10, but 24.8.14.39 refused it on DateTime and
//     DateTime64 (TYPE_MISMATCH), as a server set to basic still does.
//   - a policy claim on an integer column (chsql.IntParam) expands to
//     chsql.StrictInt over its one {pN:String}, because the plain form wraps
//     a value at or past 2^64.
//   - an `in` list becomes the external table _pN, read by
//     `(SELECT <element> FROM _pN)`. A table has no size cap, where a query
//     parameter is capped at 128 KiB, and a subquery's set uses the primary
//     key, where `c IN arrayMap(…)` read every granule on 26.8.15.10 and was
//     refused on 24.8.14.39.
//
// The rewrite is a left-to-right scan for `?`, which is exact for this SQL and
// only for this SQL: Build never renders a value or a string literal, and
// chsql.BindUnsafe rejects a `?` in any identifier it quotes. The literals a
// conversion renders are written after the scan has passed them.
func (r *BuildResult) Bind() (*Bound, error) {
	out := &Bound{}
	if len(r.Params) == 0 {
		if strings.Contains(r.SQL, "?") {
			return nil, fmt.Errorf("query has placeholders but no bound values")
		}
		out.SQL = r.SQL
		return out, nil
	}

	var b strings.Builder
	b.Grow(len(r.SQL) + len(r.Params)*16)
	rest := r.SQL
	for i, v := range r.Params {
		q := strings.IndexByte(rest, '?')
		if q < 0 {
			return nil, fmt.Errorf("query has %d bound values but only %d placeholders", len(r.Params), i)
		}
		placeholder, err := out.bind("p"+strconv.Itoa(i), v)
		if err != nil {
			return nil, err
		}
		b.WriteString(rest[:q])
		b.WriteString(placeholder)
		rest = rest[q+1:]
	}
	if strings.Contains(rest, "?") {
		return nil, fmt.Errorf("query has more placeholders than the %d bound values", len(r.Params))
	}
	b.WriteString(rest)
	out.SQL = b.String()
	return out, nil
}

// bind adds v, bound under name, to out, and returns the SQL its placeholder
// becomes.
func (out *Bound) bind(name string, v any) (string, error) {
	switch val := v.(type) {
	case listParam:
		data, err := rowBinaryStrings(val.Values)
		if err != nil {
			return "", err
		}
		table := "_" + name
		out.Tables = append(out.Tables, Table{Name: table, Data: data})
		return "(SELECT " + val.Conv.element("v") + " FROM " + table + ")", nil
	case chsql.IntParam:
		out.Params = append(out.Params, Param{Name: name, Value: chsql.EscapeStringParam(val.Value)})
		return chsql.StrictInt(name, val.Type), nil
	case convertedParam:
		raw, err := chScalarText(val.Value)
		if err != nil {
			return "", err
		}
		out.Params = append(out.Params, Param{Name: name, Value: chsql.EscapeStringParam(raw)})
		return val.Conv.expr("{" + name + ":String}"), nil
	}
	raw, err := chScalarText(v)
	if err != nil {
		return "", err
	}
	out.Params = append(out.Params, Param{Name: name, Value: chsql.EscapeStringParam(raw)})
	return "{" + name + ":String}", nil
}

// chScalarText is one scalar's value as plain text, before any encoding —
// what the caller means, not what the wire needs.
func chScalarText(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case json.Number:
		// The caller's own digits, not a float64 round-trip: 12.50 stays
		// "12.50" and an integer past 2^53 keeps every digit.
		return val.String(), nil
	case bool:
		return strconv.FormatBool(val), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case int:
		return strconv.Itoa(val), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case nil:
		// `col = NULL` is never true in SQL, so a null used to answer "no
		// rows"; an empty String parameter would instead compare against the
		// empty string, which is a different question. Refuse it (→ 400)
		// rather than answer a question the caller did not ask.
		return "", fmt.Errorf("filter value must not be null")
	default:
		return "", fmt.Errorf("unsupported filter value type %T", v)
	}
}

// rowBinaryStrings renders a list as RowBinary rows of one String column: each
// element's length as a varint, then its bytes.
func rowBinaryStrings(vals []any) ([]byte, error) {
	var b []byte
	for _, v := range vals {
		if _, isList := v.([]any); isList {
			return nil, fmt.Errorf("nested list in an 'in' value")
		}
		text, err := chScalarText(v)
		if err != nil {
			return nil, err
		}
		b = binary.AppendUvarint(b, uint64(len(text)))
		b = append(b, text...)
	}
	return b, nil
}
