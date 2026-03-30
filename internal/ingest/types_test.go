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
		ReceivedTimestamp: "2024-01-01T00:00:00Z",
		Data:              map[string]any{"page": "/home", "count": float64(42)},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var got EventMessage
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, evt.TableName, got.TableName)
	assert.Equal(t, evt.ReceivedTimestamp, got.ReceivedTimestamp)
	assert.Equal(t, evt.Data["page"], got.Data["page"])
}
