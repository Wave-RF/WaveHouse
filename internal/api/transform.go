package api

import (
	"encoding/json"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
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

// filterEventColumns removes columns from event data that the role is not allowed to see.
func filterEventColumns(data map[string]any, perms *policy.ResolvedPermissions) {
	if perms == nil || data == nil {
		return
	}
	for col := range data {
		if !perms.IsColumnAllowed(col) {
			delete(data, col)
		}
	}
}
