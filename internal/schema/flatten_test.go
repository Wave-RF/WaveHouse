package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatten_SimpleObject(t *testing.T) {
	raw := json.RawMessage(`{"name":"alice","age":30}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"name": "alice"}, strData)
	assert.Equal(t, map[string]float64{"age": 30}, numData)
	assert.Empty(t, boolData)
}

func TestFlatten_NestedObject(t *testing.T) {
	raw := json.RawMessage(`{"user":{"name":"bob","address":{"city":"NYC"}}}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"user.name":         "bob",
		"user.address.city": "NYC",
	}, strData)
	assert.Empty(t, numData)
	assert.Empty(t, boolData)
}

func TestFlatten_Array(t *testing.T) {
	raw := json.RawMessage(`{"tags":["a","b","c"]}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"tags.0": "a",
		"tags.1": "b",
		"tags.2": "c",
	}, strData)
	assert.Empty(t, numData)
	assert.Empty(t, boolData)
}

func TestFlatten_BoolAndNull(t *testing.T) {
	raw := json.RawMessage(`{"active":true,"deleted":false,"meta":null}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Empty(t, strData)
	assert.Empty(t, numData)
	assert.Equal(t, map[string]bool{"active": true, "deleted": false}, boolData)
	// Null values are skipped entirely.
}

func TestFlatten_Numbers(t *testing.T) {
	raw := json.RawMessage(`{"count":42,"price":9.99,"zero":0}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Empty(t, strData)
	assert.Equal(t, map[string]float64{"count": 42, "price": 9.99, "zero": 0}, numData)
	assert.Empty(t, boolData)
}

func TestFlatten_MixedArray(t *testing.T) {
	raw := json.RawMessage(`{"items":[1,"two",true,null]}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"items.1": "two"}, strData)
	assert.Equal(t, map[string]float64{"items.0": 1}, numData)
	assert.Equal(t, map[string]bool{"items.2": true}, boolData)
	// items.3 (null) is skipped.
}

func TestFlatten_MixedNestedTypes(t *testing.T) {
	raw := json.RawMessage(`{"user":{"name":"alice","age":30,"active":true}}`)
	strData, numData, boolData, err := Flatten(raw)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"user.name": "alice"}, strData)
	assert.Equal(t, map[string]float64{"user.age": 30}, numData)
	assert.Equal(t, map[string]bool{"user.active": true}, boolData)
}

func TestFlatten_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`not json`)
	_, _, _, err := Flatten(raw)
	assert.Error(t, err)
}
