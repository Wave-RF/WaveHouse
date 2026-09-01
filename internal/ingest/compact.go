package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// EncodeCompactRow renders one record as a single JSONCompactEachRow line: a
// JSON array carrying exactly one value per schema column, in declaration order
// (discovery orders TableSchema.Columns by system.columns.position). A column
// the record does not carry encodes as null; the insert path turns that back
// into the column's default via input_format_null_as_default. The result has no
// trailing newline — the caller joins lines.
//
// This is serialization ONLY. It performs no validation and makes no decision
// about a value: schema validation upstream has already rejected unknown keys
// and unacceptable types, so there is nothing here to reject and nothing to
// coerce. Record values arrive json.Number-preserving (every decoder on the
// ingest path sets UseNumber), and json.Marshal writes a json.Number as its
// exact digits, so a 64-bit id past 2^53 keeps every one of them.
//
// Transitional: replaced by chtypes RowsExport; serialization only, never add
// rules here.
func EncodeCompactRow(orderedCols []discovery.Column, record map[string]any) (json.RawMessage, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, col := range orderedCols {
		if i > 0 {
			buf.WriteByte(',')
		}
		v, ok := record[col.Name]
		if !ok {
			buf.WriteString("null")
			continue
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("encode column %q: %w", col.Name, err)
		}
		buf.Write(b)
	}
	buf.WriteByte(']')
	return json.RawMessage(buf.Bytes()), nil
}
