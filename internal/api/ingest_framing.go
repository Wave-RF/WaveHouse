package api

// Framing: everything ingest reads out of the request body's own bytes. It is
// deliberately small — three single-pass scanners, no decoder, no allocation —
// because the whole point of the type layer is that ClickHouse's parser reads
// the records and Go does not.

// reframeArray turns a top-level JSON array into the newline-framed body
// chtypes reads per record, IN PLACE, and reports how many elements it holds.
//
// It exists because of a measured cliff (AUDIT §0.2): a SINGLE-LINE array with
// one bad record loses the whole batch — chtypes answers Outcome=rejected with
// no exported bytes, so the records that parsed perfectly are lost too. The same
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
//   - every other newline outside a string becomes a space. Not cosmetic:
//     typelayer.Ingest pads its verdict list out to the body's newline count, so
//     a pretty-printed array would come back with one phantom "no verdict"
//     record per line of layout. This leaves exactly elements-1 newlines.
//
// A raw newline inside a string is illegal JSON, so leaving those alone costs
// nothing and keeps the caller's bytes the caller's.
//
// ok is false when the brackets do not balance — a truncated upload, or a
// structural syntax error — which is a whole-request 400. Nothing is published
// from a body we cannot frame.
func reframeArray(b []byte) (elements int, ok bool) {
	depth, commas := 0, 0
	sawValue := false
	inStr, esc := false, false
	for i := range b {
		c := b[i]
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
			if c == ']' && depth == 0 {
				b[i] = ' '
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
		return 0, false
	}
	if !sawValue {
		return 0, true // `[]`, possibly with whitespace inside
	}
	return commas + 1, true
}

// cellAt returns the k-th top-level cell of one JSONCompactEachRow line — a
// `[v0, v1, …]` array as ClickHouse's own writer produced it — without decoding
// the row. Leading and trailing whitespace around the cell is trimmed; the cell
// itself is returned verbatim, still JSON-encoded.
//
// This is how the dedupe id is read (AUDIT §A.4): the id column's position in
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
