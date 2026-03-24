package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnflatten_SimpleStrings(t *testing.T) {
	result := Unflatten(
		map[string]string{"name": "alice", "city": "NYC"},
		nil, nil,
	)
	assert.Equal(t, map[string]any{"name": "alice", "city": "NYC"}, result)
}

func TestUnflatten_NestedObject(t *testing.T) {
	result := Unflatten(
		map[string]string{"user.name": "bob", "user.address.city": "NYC"},
		nil, nil,
	)
	assert.Equal(t, map[string]any{
		"user": map[string]any{
			"name": "bob",
			"address": map[string]any{
				"city": "NYC",
			},
		},
	}, result)
}

func TestUnflatten_Array(t *testing.T) {
	result := Unflatten(
		map[string]string{"tags.0": "a", "tags.1": "b", "tags.2": "c"},
		nil, nil,
	)
	assert.Equal(t, map[string]any{
		"tags": []any{"a", "b", "c"},
	}, result)
}

func TestUnflatten_MixedTypes(t *testing.T) {
	result := Unflatten(
		map[string]string{"user.name": "alice"},
		map[string]float64{"user.age": 30},
		map[string]bool{"user.active": true},
	)
	assert.Equal(t, map[string]any{
		"user": map[string]any{
			"name":   "alice",
			"age":    float64(30),
			"active": true,
		},
	}, result)
}

func TestUnflatten_MixedArray(t *testing.T) {
	result := Unflatten(
		map[string]string{"items.1": "two"},
		map[string]float64{"items.0": 1},
		map[string]bool{"items.2": true},
	)
	assert.Equal(t, map[string]any{
		"items": []any{float64(1), "two", true},
	}, result)
}

func TestUnflatten_EmptyMaps(t *testing.T) {
	result := Unflatten(nil, nil, nil)
	assert.Equal(t, map[string]any{}, result)
}

func TestUnflatten_DeeplyNested(t *testing.T) {
	result := Unflatten(
		map[string]string{"a.b.c.d.e": "deep"},
		nil, nil,
	)
	assert.Equal(t, map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": map[string]any{
					"d": map[string]any{
						"e": "deep",
					},
				},
			},
		},
	}, result)
}

func TestRoundtrip_FlattenUnflatten(t *testing.T) {
	input := json.RawMessage(`{
		"user": {
			"name": "alice",
			"age": 30,
			"active": true
		},
		"tags": ["web", "mobile"],
		"score": 99.5,
		"verified": false,
		"page": "/home"
	}`)

	strData, numData, boolData, err := Flatten(input)
	require.NoError(t, err)

	result := Unflatten(strData, numData, boolData)

	expected := map[string]any{
		"user": map[string]any{
			"name":   "alice",
			"age":    float64(30),
			"active": true,
		},
		"tags":     []any{"web", "mobile"},
		"score":    float64(99.5),
		"verified": false,
		"page":     "/home",
	}
	assert.Equal(t, expected, result)
}

func TestRoundtrip_WithNulls(t *testing.T) {
	// Nulls are skipped by Flatten, so they won't appear in the roundtrip.
	input := json.RawMessage(`{"name":"alice","meta":null,"count":1}`)

	strData, numData, boolData, err := Flatten(input)
	require.NoError(t, err)

	result := Unflatten(strData, numData, boolData)

	// "meta" is absent because nulls are skipped.
	expected := map[string]any{
		"name":  "alice",
		"count": float64(1),
	}
	assert.Equal(t, expected, result)
}

func TestUnflatten_NonConsecutiveIndicesStayAsMap(t *testing.T) {
	// If indices are not consecutive from 0, keep as a map (not an array).
	result := Unflatten(
		map[string]string{"items.0": "a", "items.2": "c"},
		nil, nil,
	)
	expected := map[string]any{
		"items": map[string]any{
			"0": "a",
			"2": "c",
		},
	}
	assert.Equal(t, expected, result)
}
