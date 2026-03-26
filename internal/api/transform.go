package api

import (
	"encoding/json"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
)

// transformForClient converts a raw EventMessage JSON (from MQ) into a
// client-friendly format.
func transformForClient(raw []byte) ([]byte, error) {
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil {
		return raw, nil // Not an EventMessage — pass through unchanged.
	}

	out := map[string]any{
		"table_name":         evt.TableName,
		"received_timestamp": evt.ReceivedTimestamp,
		"data":               evt.Data,
	}

	return json.Marshal(out)
}
