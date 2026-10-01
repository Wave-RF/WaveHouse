package typelayer

import (
	"bytes"
	"strconv"
	"strings"
)

// Counting the records a body holds, independently of chtypes.
//
// chtypes answers one verdict per record its reader took, and its reader can
// take more than one record at a time. Measured on 26.8.15.10: a UUID value
// shorter than 36 characters makes the UUID reader consume a fixed 36-byte
// window past it, and the error recovery that follows resumes at the end of the
// line the window ended on — so the records the window reached are gone from
// the answer, with no verdict of their own. `{"id":"x"}` followed by two short
// records answers ONE verdict; the same in CSV loses the line after. Nothing
// in the per-record detail says so: the batch outcome is still Accepted.
//
// So the type layer counts the records itself and refuses to report a short
// batch: fewer verdicts than recordFloor means records went unread, and the
// whole body is declined (see IngestWith). The count is a FLOOR, never an
// estimate, so a body chtypes read whole can never trip it:
//
//   - JSONEachRow: top-level objects, scanned string- and escape-aware — a
//     raw newline inside a string is part of the string to ClickHouse too
//     (measured), and blank lines, pretty-printing, commas between objects and
//     an outer array are not records. Exact for a well-formed body; a malformed
//     one can only under-count (a truncated object hides the ones after it), so
//     a scalar or garbage line costs the check its precision, never a false
//     decline.
//   - CSV and TSV: records, which to ClickHouse are LINES — a blank line is a
//     record (an empty value on a one-column table, a refused record on a
//     wider one; measured), as is a non-empty tail with no newline. A CSV
//     newline inside a double-quoted field, a quote opening after spaces or
//     tabs included, is not a terminator; a quote anywhere else is a literal.
//     A TSV newline escaped by a backslash is not one either. A CSV line ends
//     at LF, CRLF or LF CR; a TSV one at LF alone (an LF CR leaves the CR to
//     open the next record). The header line of a WithNames body is not a
//     record; under header auto-detection see detectedHeaderRows. For a
//     leading UTF-8 byte order mark see bomFloor.
func recordFloor(format Format, opts IngestOptions, body []byte, wire []string, types map[string]string) int {
	switch format {
	case FormatJSONEachRow:
		return jsonObjects(body)
	case FormatCSV, FormatCSVWithNames, FormatTSV, FormatTSVWithNames:
	default:
		return 0
	}
	if bytes.HasPrefix(body, utf8BOM) {
		return bomFloor(format, opts, body, wire, types)
	}
	return lineFloor(format, opts, body, wire, types)
}

// utf8BOM is the UTF-8 byte order mark.
var utf8BOM = []byte("\xEF\xBB\xBF")

// bomFloor is the floor of a CSV or TSV body led by a UTF-8 byte order mark.
// Measured on 26.8.15.10, ClickHouse always skips the mark in a WithNames
// body; in a positional one (bare or header=absent) the first wire column's
// type decides. String and FixedString, bare or inside Nullable or
// LowCardinality, keep it as the start of the value, as do Array(String) and
// Map(String, …); UUID, DateTime, numbers, Enum, IPv4 and Array(UInt8) skip
// it. So
// `BOM id,page,n` is a detected header on a UUID- or DateTime-first table and
// a record on a String-first one, and a quote right after the mark opens a
// field only where it was skipped.
//
// A WithNames body is counted without the mark and one whose first column is
// a stringType with it. Every other first column takes the lower of the two readings, so the
// floor holds whichever one ClickHouse makes for a type not measured here.
func bomFloor(format Format, opts IngestOptions, body []byte, wire []string, types map[string]string) int {
	skipped := lineFloor(format, opts, body[len(utf8BOM):], wire, types)
	if format == FormatCSVWithNames || format == FormatTSVWithNames {
		return skipped
	}
	kept := lineFloor(format, opts, body, wire, types)
	if len(wire) > 0 && stringType(types[wire[0]]) {
		return kept
	}
	return min(skipped, kept)
}

// stringType reports whether t is String or FixedString, bare or inside
// Nullable or LowCardinality.
func stringType(t string) bool {
	for {
		if inner, ok := strings.CutPrefix(t, "Nullable("); ok {
			t = strings.TrimSuffix(inner, ")")
		} else if inner, ok := strings.CutPrefix(t, "LowCardinality("); ok {
			t = strings.TrimSuffix(inner, ")")
		} else {
			return t == "String" || strings.HasPrefix(t, "FixedString(")
		}
	}
}

// lineFloor is recordFloor for a CSV or TSV body: its records less the header
// lines ClickHouse reads as one.
func lineFloor(format Format, opts IngestOptions, body []byte, wire []string, types map[string]string) int {
	csv := format == FormatCSV || format == FormatCSVWithNames
	var n int
	var first, second []byte
	if csv {
		n, first, second = csvRecords(body)
	} else {
		n, first, second = tsvRecords(body)
	}
	switch {
	case n == 0:
		return 0
	case format == FormatCSVWithNames || format == FormatTSVWithNames:
		// Exactly one header line; a second line spelling the types is a
		// record (measured).
		return n - 1
	case opts.StrictPositional:
		return n
	default:
		return n - detectedHeaderRows(csv, first, second, n, wire, types)
	}
}

// jsonObjects counts the top-level objects of a JSONEachRow body: those that
// open at depth 0, or at depth 1 inside an outer array.
func jsonObjects(b []byte) int {
	n, depth := 0, 0
	inArray, inStr, esc := false, false, false
	for _, c := range b {
		switch {
		case esc:
			esc = false
		case inStr && c == '\\':
			esc = true
		case c == '"':
			inStr = !inStr
		case inStr:
		case c == '{' || c == '[':
			if depth == 0 && c == '[' {
				inArray = true
			} else if c == '{' && (depth == 0 || depth == 1 && inArray) {
				n++
			}
			depth++
		case c == '}' || c == ']':
			if depth > 0 {
				depth--
			}
			if depth == 0 {
				inArray = false
			}
		}
	}
	return n
}

// csvRecords counts ClickHouse's CSV records in b and returns the first two,
// without their terminators. A double quote opens a quoted field only where a
// field starts (leading spaces and tabs aside); inside one, `""` is a quote and
// a newline is data. A CR right after a terminating LF belongs to it: LF CR is
// one line end to ClickHouse, like CRLF. ClickHouse reads the same body the
// same way (measured, including a quote after leading whitespace, a quote
// mid-field, a CRLF inside quotes and LF CR line ends).
func csvRecords(b []byte) (n int, first, second []byte) {
	start := 0
	inQuote, fieldStart := false, true
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inQuote {
			if c == '"' {
				if i+1 < len(b) && b[i+1] == '"' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		switch c {
		case '"':
			inQuote = fieldStart
			fieldStart = false
		case ',':
			fieldStart = true
		case ' ', '\t':
		case '\n':
			n, first, second = takeRecord(n, first, second, b[start:i])
			if i+1 < len(b) && b[i+1] == '\r' {
				i++
			}
			start, fieldStart = i+1, true
		default:
			fieldStart = false
		}
	}
	if start < len(b) {
		n, first, second = takeRecord(n, first, second, b[start:])
	}
	return n, first, second
}

// tsvRecords is csvRecords for TSV: no quoting, a newline after an odd run of
// backslashes is an escaped newline inside a field (measured: `a\` + LF is one
// value, `a\\` + LF ends the record), and a CR after an LF is not part of the
// line end — it is the first byte of the next record (measured).
func tsvRecords(b []byte) (n int, first, second []byte) {
	start, run := 0, 0
	for i, c := range b {
		if c == '\n' && run%2 == 0 {
			n, first, second = takeRecord(n, first, second, b[start:i])
			start = i + 1
		}
		if c == '\\' {
			run++
		} else {
			run = 0
		}
	}
	if start < len(b) {
		n, first, second = takeRecord(n, first, second, b[start:])
	}
	return n, first, second
}

func takeRecord(n int, first, second, rec []byte) (int, []byte, []byte) {
	switch n {
	case 0:
		first = rec
	case 1:
		second = rec
	}
	return n + 1, first, second
}

// detectedHeaderRows is how many leading records ClickHouse's header
// auto-detection (input_format_{csv,tsv}_detect_header, on for bare CSV and
// TSV) consumes, as measured on 26.8.15.10:
//
//   - the first record is a names row when its values, as a set, are
//     contained in the table's columns or contain them — case-sensitive, CSV
//     values unquoted and trimmed of spaces and tabs, TSV values unescaped and
//     not trimmed;
//   - after a names row, the second record is consumed too when it spells
//     column types. Accepted only when each value is exactly the type of the
//     column the names row put there; any other set of valid type names
//     rejects the whole batch, which is declined without reaching this count.
//
// Where a value cannot be read the way ClickHouse reads it (an escape this does
// not decode, an unterminated quote), it is taken to match, so the answer
// errs towards consuming a row: that lowers the floor, which can only miss a
// lost record, never decline a body chtypes read whole.
func detectedHeaderRows(csv bool, first, second []byte, n int, wire []string, types map[string]string) int {
	names, known := splitRecord(csv, first)
	if !namesRow(names, known, wire) {
		return 0
	}
	if n < 2 {
		return 1
	}
	values, vknown := splitRecord(csv, second)
	if typesRow(names, values, vknown, types) {
		return 2
	}
	return 1
}

// namesRow reports whether values could be a names row for columns wire. A
// value whose spelling is not known (known[i] false) may be any column.
func namesRow(values []string, known []bool, wire []string) bool {
	cols := make(map[string]struct{}, len(wire))
	for _, c := range wire {
		cols[c] = struct{}{}
	}
	seen := make(map[string]struct{}, len(values))
	inCols, wild := true, 0
	for i, v := range values {
		if !known[i] {
			wild++
			continue
		}
		seen[v] = struct{}{}
		if _, ok := cols[v]; !ok {
			inCols = false
		}
	}
	if inCols {
		return true
	}
	missing := 0
	for c := range cols {
		if _, ok := seen[c]; !ok {
			missing++
		}
	}
	return missing <= wild
}

// typesRow reports whether values spells, position by position, the types of
// the columns names put there.
func typesRow(names, values []string, known []bool, types map[string]string) bool {
	if len(values) != len(names) {
		return false
	}
	for i, v := range values {
		if !known[i] {
			continue
		}
		if t, ok := types[names[i]]; !ok || t != v {
			return false
		}
	}
	return true
}

// splitRecord splits one record into the values header detection compares,
// with known[i] false where value i's spelling could not be decoded exactly.
func splitRecord(csv bool, rec []byte) (values []string, known []bool) {
	if csv {
		return csvValues(rec)
	}
	return tsvValues(rec)
}

func csvValues(rec []byte) (values []string, known []bool) {
	rec = bytes.TrimSuffix(rec, []byte("\r"))
	for {
		rec = bytes.TrimLeft(rec, " \t")
		var v []byte
		ok := true
		if len(rec) > 0 && rec[0] == '"' {
			j := 1
			for ; j < len(rec); j++ {
				if rec[j] != '"' {
					v = append(v, rec[j])
					continue
				}
				if j+1 < len(rec) && rec[j+1] == '"' {
					v = append(v, '"')
					j++
					continue
				}
				break
			}
			if j >= len(rec) {
				return append(values, string(v)), append(known, false) // unterminated
			}
			rec = bytes.TrimLeft(rec[j+1:], " \t")
			if len(rec) > 0 && rec[0] != ',' {
				// Text after the closing quote: ClickHouse refuses the field.
				ok = false
				k := bytes.IndexByte(rec, ',')
				if k < 0 {
					k = len(rec)
				}
				rec = rec[k:]
			}
		} else {
			k := bytes.IndexByte(rec, ',')
			if k < 0 {
				k = len(rec)
			}
			v = bytes.TrimRight(rec[:k], " \t")
			rec = rec[k:]
		}
		values, known = append(values, string(v)), append(known, ok)
		if len(rec) == 0 {
			return values, known
		}
		rec = rec[1:] // the comma
	}
}

func tsvValues(rec []byte) (values []string, known []bool) {
	for f := range bytes.SplitSeq(rec, []byte("\t")) {
		v, ok := tsvUnescape(f)
		values, known = append(values, v), append(known, ok)
	}
	return values, known
}

// tsvUnescape decodes TabSeparated's escapes. ok is false for one it does not
// decode, or a trailing CR (whose reading depends on a CRLF setting).
func tsvUnescape(f []byte) (string, bool) {
	if bytes.HasSuffix(f, []byte("\r")) {
		return "", false
	}
	if bytes.IndexByte(f, '\\') < 0 {
		return string(f), true
	}
	out := make([]byte, 0, len(f))
	for i := 0; i < len(f); i++ {
		if f[i] != '\\' {
			out = append(out, f[i])
			continue
		}
		if i+1 >= len(f) {
			return "", false
		}
		i++
		switch f[i] {
		case '\\', '\'', '"':
			out = append(out, f[i])
		case 't':
			out = append(out, '\t')
		case 'n', '\n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case '0':
			out = append(out, 0)
		case 'x':
			if i+2 >= len(f) {
				return "", false
			}
			h, err := strconv.ParseUint(string(f[i+1:i+3]), 16, 8)
			if err != nil {
				return "", false
			}
			out = append(out, byte(h))
			i += 2
		default:
			return "", false
		}
	}
	return string(out), true
}
