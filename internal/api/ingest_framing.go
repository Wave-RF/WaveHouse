package api

import (
	"encoding/json"
	"errors"
)

// Framing: everything ingest reads out of the bytes it handles, the request
// body and the rows ClickHouse exported from it. It is deliberately small —
// single-pass scanners, no decoder — because the whole point of the type layer
// is that ClickHouse's parser reads the records and Go does not.

// reframeArray turns a top-level JSON array into the newline-framed body
// chtypes reads per record, IN PLACE, and reports how many elements it holds.
//
// It exists because of a measured cliff: a SINGLE-LINE array with one bad
// record loses the whole batch — chtypes answers Outcome=rejected with no
// exported bytes, so the records that parsed perfectly are lost too (pinned by
// TestIngest_JSONArray_CompactWithOneBadRecord). The same
// records newline-separated skip the bad one and export the rest. Rewriting the
// depth-1 commas to newlines restores per-record salvage (#195's promise) for
// 0.7 ms per 617 KB, with no decode and no copy.
//
// The scan is string- and escape-aware, so a comma or a bracket inside a value
// is untouched, and it runs ONLY when the declared format is the JSON family and
// the first non-whitespace byte is '['. It must not run on anything else: a bare
// object's own commas are at depth 1 and rewriting them destroys the record
// (measured).
//
// Three substitutions, all in place and all the same length:
//
//   - a depth-1 comma becomes a newline — the framing itself;
//   - the OUTER '[' and ']' become spaces. Commas alone are not enough:
//     measured, a bad LAST record still loses the whole batch, because the
//     closing bracket shares that record's line and the reader cannot resync
//     past it. JSONEachRow needs no brackets, so removing them costs nothing and
//     makes every position salvageable, first and last included;
//   - every other newline outside a string becomes a space. Not cosmetic: when
//     chtypes declines a whole batch without per-record detail, the type layer
//     counts the body's lines to answer each record, so a pretty-printed array
//     would come back with one phantom declined record per line of layout.
//     This leaves exactly elements-1 newlines, so that count stays right.
//
// A raw newline inside a string is illegal JSON, so leaving those alone costs
// nothing and keeps the caller's bytes the caller's.
//
// An error is a whole-request 400, and nothing is published from a body we
// cannot frame: errUnterminatedArray when the brackets do not balance — a
// truncated upload, or a structural syntax error — and errAfterArray when
// anything but whitespace follows the array's closing ']'. That tail is not a
// record of the array, and framing it as more records would publish what the
// caller never put in the batch.
func reframeArray(b []byte) (elements int, err error) {
	depth, commas := 0, 0
	sawValue, closed := false, false
	inStr, esc := false, false
	for i := range b {
		c := b[i]
		if closed {
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				return 0, errAfterArray
			}
			continue
		}
		switch {
		case esc:
			esc = false
		case inStr && c == '\\':
			esc = true
		case c == '"':
			inStr = !inStr
			sawValue = sawValue || depth >= 1
		case inStr:
		case c == '[' || c == '{':
			sawValue = sawValue || depth >= 1
			depth++
			if c == '[' && depth == 1 {
				b[i] = ' '
			}
		case c == ']' || c == '}':
			depth--
			if depth == 0 {
				if c != ']' {
					return 0, errUnterminatedArray // the array's '[' closed by a '}'
				}
				b[i] = ' '
				closed = true
			}
		case c == ',' && depth == 1:
			b[i] = '\n'
			commas++
		case c == '\n' || c == '\r':
			b[i] = ' '
		case c == ' ' || c == '\t':
		default:
			sawValue = sawValue || depth >= 1
		}
	}
	if depth != 0 || inStr {
		return 0, errUnterminatedArray
	}
	if !sawValue {
		return 0, nil // `[]`, possibly with whitespace inside
	}
	return commas + 1, nil
}

// The two ways reframeArray refuses a body, each the tail of the caller's
// "invalid json: …" 400.
var (
	errUnterminatedArray = errors.New("unterminated json array")
	errAfterArray        = errors.New("content after the closing ']' of the json array")
)

// cellAt returns the k-th top-level cell of one JSONCompactEachRow line — a
// `[v0, v1, …]` array as ClickHouse's own writer produced it — without decoding
// the row. Leading and trailing whitespace around the cell is trimmed; the cell
// itself is returned verbatim, still JSON-encoded.
//
// This is how the dedupe id is read (eventIDAt): the id column's position in
// the table's wire columns is known, so the value is a byte span rather than a
// map lookup. The scanner is the same string- and escape-aware shape as
// reframeArray, so a comma or a bracket inside a value cannot end a cell.
func cellAt(line []byte, k int) ([]byte, bool) {
	if k < 0 {
		return nil, false
	}
	depth, idx, start := 0, 0, -1
	inStr, esc := false, false
	for i := range line {
		c := line[i]
		switch {
		case esc:
			esc = false
			continue
		case inStr && c == '\\':
			esc = true
			continue
		case c == '"':
			inStr = !inStr
		case inStr:
		case c == '[' || c == '{':
			depth++
			if depth == 1 {
				start = i + 1
				continue
			}
		case c == ']' || c == '}':
			depth--
			if depth == 0 {
				if idx == k && start >= 0 {
					return trimSpaceBytes(line[start:i]), true
				}
				return nil, false
			}
		case c == ',' && depth == 1:
			if idx == k {
				return trimSpaceBytes(line[start:i]), true
			}
			idx++
			start = i + 1
			continue
		}
	}
	return nil, false
}

// trimSpaceBytes drops ASCII layout around a cell. bytes.TrimSpace would also
// do it, but this stays byte-exact about which bytes count as layout in a
// JSONCompactEachRow line (the writer emits ", " between cells) and allocates
// nothing.
func trimSpaceBytes(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\t' || b[j-1] == '\n' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}

// eventIDAt reads the dedupe id out of an exported row by POSITION — the id
// column's index in the wire columns the row was exported with — so no record
// is decoded for it. idx is -1 when the column is not on the wire at all.
//
// Consequences worth knowing, all documented:
//   - the key is the STORED value, not the caller's spelling: `256` into a
//     UInt8 keys on `0`, and a DateTime keys on ClickHouse's rendering. For the
//     documented case — a string id — the two are identical.
//   - "missing" means "the row carries no value". A `null` cell is missing, as
//     an explicit null always has been (#370): keying on its spelling would make
//     every null one id. So is an empty string, which is what an omitted
//     `event_id String` stores. A numeric id column cannot distinguish an
//     omitted 0 from a supplied one.
func eventIDAt(line []byte, idx int) (string, bool) {
	if idx < 0 {
		return "", false
	}
	cell, ok := cellAt(line, idx)
	if !ok || len(cell) == 0 || string(cell) == "null" {
		return "", false
	}
	if cell[0] == '"' {
		// One scalar string, not the record: the cell is JSON-encoded by
		// ClickHouse's own writer (it escapes "/" as "\/"), so Go's own
		// string-literal unquoting would refuse it.
		var s string
		if err := json.Unmarshal(cell, &s); err != nil || s == "" {
			return "", false
		}
		return s, true
	}
	return string(cell), true
}
