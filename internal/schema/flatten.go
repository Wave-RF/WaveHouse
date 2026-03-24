package schema

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Flatten takes raw JSON and produces dot-notation keys and string values
// suitable for ClickHouse Map(String, String) columns.
func Flatten(raw json.RawMessage) (keys []string, values []string, err error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, nil, fmt.Errorf("unmarshal: %w", err)
	}
	keys = make([]string, 0)
	values = make([]string, 0)
	flattenMap("", obj, &keys, &values)
	return keys, values, nil
}

func flattenMap(prefix string, m map[string]any, keys *[]string, values *[]string) {
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		flattenValue(fullKey, v, keys, values)
	}
}

func flattenValue(key string, v any, keys *[]string, values *[]string) {
	switch val := v.(type) {
	case map[string]any:
		flattenMap(key, val, keys, values)
	case []any:
		for i, elem := range val {
			flattenValue(key+"."+strconv.Itoa(i), elem, keys, values)
		}
	case nil:
		*keys = append(*keys, key)
		*values = append(*values, "")
	case bool:
		*keys = append(*keys, key)
		*values = append(*values, strconv.FormatBool(val))
	case float64:
		*keys = append(*keys, key)
		if val == float64(int64(val)) {
			*values = append(*values, strconv.FormatInt(int64(val), 10))
		} else {
			*values = append(*values, strconv.FormatFloat(val, 'f', -1, 64))
		}
	default:
		*keys = append(*keys, key)
		*values = append(*values, fmt.Sprintf("%v", val))
	}
}
