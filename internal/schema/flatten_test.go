package schema

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatten_SimpleObject(t *testing.T) {
	raw := json.RawMessage(`{"name":"alice","age":30}`)
	keys, values, err := Flatten(raw)
	require.NoError(t, err)

	pairs := zip(keys, values)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })

	assert.Equal(t, [][2]string{{"age", "30"}, {"name", "alice"}}, pairs)
}

func TestFlatten_NestedObject(t *testing.T) {
	raw := json.RawMessage(`{"user":{"name":"bob","address":{"city":"NYC"}}}`)
	keys, values, err := Flatten(raw)
	require.NoError(t, err)

	pairs := zip(keys, values)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })

	assert.Equal(t, [][2]string{
		{"user.address.city", "NYC"},
		{"user.name", "bob"},
	}, pairs)
}

func TestFlatten_Array(t *testing.T) {
	raw := json.RawMessage(`{"tags":["a","b","c"]}`)
	keys, values, err := Flatten(raw)
	require.NoError(t, err)

	pairs := zip(keys, values)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })

	assert.Equal(t, [][2]string{
		{"tags.0", "a"},
		{"tags.1", "b"},
		{"tags.2", "c"},
	}, pairs)
}

func TestFlatten_BoolAndNull(t *testing.T) {
	raw := json.RawMessage(`{"active":true,"deleted":false,"meta":null}`)
	keys, values, err := Flatten(raw)
	require.NoError(t, err)

	pairs := zip(keys, values)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })

	assert.Equal(t, [][2]string{
		{"active", "true"},
		{"deleted", "false"},
		{"meta", ""},
	}, pairs)
}

func TestFlatten_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`not json`)
	_, _, err := Flatten(raw)
	assert.Error(t, err)
}

func zip(keys, values []string) [][2]string {
	pairs := make([][2]string, len(keys))
	for i := range keys {
		pairs[i] = [2]string{keys[i], values[i]}
	}
	return pairs
}
