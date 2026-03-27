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
	if err := json.Unmarshal(raw, &evt); err != nil || evt.TableName == "" {
		return raw, nil // Not an EventMessage — pass through unchanged.
	}

	out := map[string]any{
		"table_name":         evt.TableName,
		"received_timestamp": evt.ReceivedTimestamp,
		"data":               evt.Data,
	}

	return json.Marshal(out)
}

// filterEventColumns returns a copy of data containing only columns the role is allowed to see.
// Returns the original map unchanged if perms is nil.
func filterEventColumns(data map[string]any, perms *policy.ResolvedPermissions) map[string]any {
	if perms == nil || data == nil {
		return data
	}
	filtered := make(map[string]any, len(data))
	for col, val := range data {
		if perms.IsColumnAllowed(col) {
			filtered[col] = val
		}
	}
	return filtered
}
