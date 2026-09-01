package ingest

import "encoding/json"

// BufferConsumerName is the durable consumer name used by the ingest
// worker. The Active Sweeper references this to read the AckFloor.
const BufferConsumerName = "buffer-consumer"

// FormatJSONCompactEachRow is the only row format the envelope carries today.
// It is stated on the wire rather than assumed so a reader can tell an envelope
// it understands from one it doesn't, and so a second format can be added
// without a third guess at what the bytes mean.
const FormatJSONCompactEachRow = "JSONCompactEachRow"

// EventMessage is the wire format published to the MQ.
//
// Row data travels POSITIONALLY: Row is one JSONCompactEachRow line (a JSON
// array, no trailing newline) and Columns names its positions in the table's
// declaration order. The two are only meaningful together — a reader that
// cannot pair them (a length mismatch, an undecodable row) has no way to map a
// value to a column and must fail closed rather than guess.
type EventMessage struct {
	TableName         string          `json:"table_name"`
	Scope             string          `json:"scope"`
	ReceivedTimestamp string          `json:"received_timestamp"`
	Format            string          `json:"format"`
	Columns           []string        `json:"columns"`
	Row               json.RawMessage `json:"row"`
}
