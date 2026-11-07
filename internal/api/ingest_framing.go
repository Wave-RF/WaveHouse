package api

import (
	"encoding/json"
)

// Framing: what ingest reads out of the rows ClickHouse exported. It is
// deliberately small — single-pass scanners, no decoder — because the whole
// point of the type layer is that ClickHouse's parser reads the records and Go
// does not.

// cellAt returns the k-th top-level cell of one JSONCompactEachRow line — a
// `[v0, v1, …]` array as ClickHouse's own writer produced it — without decoding
// the row. Leading and trailing whitespace around the cell is trimmed; the cell
// itself is returned verbatim, still JSON-encoded.
//
// This is how the dedupe id is read (eventIDAt): the id column's position in
// the table's wire columns is known, so the value is a byte span rather than a
// map lookup. The scanner is string- and escape-aware, so a comma or a
// bracket inside a value cannot end a cell.
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
// The key is the STORED value, not the caller's spelling: `256` into a
// `UInt8` keys on `0`, and a DateTime keys on ClickHouse's rendering. For the
// documented case, a string id, the two are identical.
//
// "Missing" means "the row carries no value": a `null` cell or an empty
// string. Keying on a null's spelling would make every null one id (#370).
// Nothing else is missing: a column that stores a value for an id the record
// did not send (a number's 0, a nil UUID, a DEFAULT) is keyed on it. The
// schema guidance (a Nullable or String id column with no DEFAULT) keeps that
// from happening; nothing here checks it.
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
		// ClickHouse's own writer, so it is JSON-decoded. Go's own
		// string-literal unquoting reads a different escape grammar (it
		// refuses JSON's "\/", for one).
		var s string
		if err := json.Unmarshal(cell, &s); err != nil || s == "" {
			return "", false
		}
		return s, true
	}
	return string(cell), true
}
