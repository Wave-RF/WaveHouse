package schema

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Flatten takes raw JSON and produces three typed maps using dot-notation keys:
//   - strData: string values
//   - numData: numeric values (JSON numbers → float64)
//   - boolData: boolean values
//
// Null values are skipped (not stored in any map).
// Nested objects use dot-notation (e.g., "user.name").
// Arrays use index-based keys (e.g., "tags.0", "tags.1").
func Flatten(raw json.RawMessage) (strData map[string]string, numData map[string]float64, boolData map[string]bool, err error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, nil, nil, fmt.Errorf("unmarshal: %w", err)
	}
	strData = make(map[string]string)
	numData = make(map[string]float64)
	boolData = make(map[string]bool)
	flattenMap("", obj, strData, numData, boolData)
	return strData, numData, boolData, nil
}

func flattenMap(prefix string, m map[string]any, strData map[string]string, numData map[string]float64, boolData map[string]bool) {
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		flattenValue(fullKey, v, strData, numData, boolData)
	}
}

func flattenValue(key string, v any, strData map[string]string, numData map[string]float64, boolData map[string]bool) {
	switch val := v.(type) {
	case map[string]any:
		flattenMap(key, val, strData, numData, boolData)
	case []any:
		for i, elem := range val {
			flattenValue(key+"."+strconv.Itoa(i), elem, strData, numData, boolData)
		}
	case nil:
		// Skip nulls — not stored in any map.
	case bool:
		boolData[key] = val
	case float64:
		numData[key] = val
	case string:
		strData[key] = val
	default:
		strData[key] = fmt.Sprintf("%v", val)
	}
}
