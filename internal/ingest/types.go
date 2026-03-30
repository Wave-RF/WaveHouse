package ingest

// BufferConsumerName is the durable consumer name used by the Bento ingest
// worker. The Active Sweeper references this to read the AckFloor.
const BufferConsumerName = "buffer-consumer"

// EventMessage is the wire format published to the MQ.
type EventMessage struct {
	TableName         string         `json:"table_name"`
	ReceivedTimestamp string         `json:"received_timestamp"`
	Data              map[string]any `json:"data"`
}
