package ingest

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventMessage_RoundTrip(t *testing.T) {
	t.Parallel()
	evt := EventMessage{
		TableName:         "clicks",
		Scope:             "org:1",
		ReceivedTimestamp: "2024-01-01T00:00:00Z",
		Format:            FormatJSONCompactEachRow,
		Columns:           []string{"page", "count"},
		Row:               json.RawMessage(`["/home",9007199254740993]`),
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var got EventMessage
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, evt.TableName, got.TableName)
	assert.Equal(t, evt.Scope, got.Scope)
	assert.Equal(t, evt.ReceivedTimestamp, got.ReceivedTimestamp)
	assert.Equal(t, FormatJSONCompactEachRow, got.Format)
	assert.Equal(t, evt.Columns, got.Columns)
	assert.JSONEq(t, string(evt.Row), string(got.Row))
	assert.Contains(t, string(got.Row), "9007199254740993",
		"the row survives the round trip as raw JSON, so a 64-bit id keeps its digits")
}
