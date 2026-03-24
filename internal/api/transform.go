package api

import (
	"encoding/json"

	"github.com/Wave-RF/BeachHouse/internal/ingest"
	"github.com/Wave-RF/BeachHouse/internal/schema"
)

// transformForClient converts a raw EventMessage JSON (from MQ) into a
// client-friendly format: unflattens typed map columns into nested "data",
// and strips internal fields like tenant_id.
func transformForClient(raw []byte) ([]byte, error) {
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil {
		return raw, nil // Not an EventMessage — pass through unchanged.
	}

	out := map[string]any{
		"event_id":           evt.EventID,
		"timestamp":          evt.Timestamp,
		"received_timestamp": evt.ReceivedTimestamp,
		"table_name":         evt.TableName,
		"data":               schema.Unflatten(evt.StrData, evt.NumData, evt.BoolData),
	}

	return json.Marshal(out)
}
