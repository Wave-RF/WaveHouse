package discovery

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Validate checks that the given data matches the table schema.
// It rejects unknown fields and checks type compatibility.
func Validate(schema *TableSchema, data map[string]any) error {
	colMap := make(map[string]Column, len(schema.Columns))
	for _, col := range schema.Columns {
		colMap[col.Name] = col
	}

	// TODO: the unknown field rejection can technically be controlled with `input_format_skip_unknown_fields = 1` I think, which would mean this is a false negative in some cases...

	// Reject unknown fields.
	for key := range data {
		if _, ok := colMap[key]; !ok {
			return fmt.Errorf("unknown column %q for table %q", key, schema.Name)
		}
	}

	// TODO: I think clickhouse actually implicitly has defaults for strings, numbers etc like "" and 0 – so (if true) then omitting that column, even if the schema doesn't set that column to nullable or have a default, clickhouse will still set the implicit value if one – so technically we then shouldn't reject missing columns and this is a false negative? But that get's quite a bit messier...

	// Check type compatibility and required columns.
	for _, col := range schema.Columns {
		val, provided := data[col.Name]
		if !provided {
			if !col.IsNullable && !col.HasDefault {
				return fmt.Errorf("missing required column %q for table %q", col.Name, schema.Name)
			}
			continue
		}
		if val == nil {
			if !col.IsNullable && !col.HasDefault {
				return fmt.Errorf("null value for non-nullable column %q", col.Name)
			}
			continue
		}
		if !isTypeCompatible(col.Type, val) {
			return fmt.Errorf("type mismatch for column %q: cannot store %T in %s", col.Name, val, col.Type)
		}
	}

	return nil
}

// isTypeCompatible checks whether a Go/JSON value can be stored in the given ClickHouse type.
func isTypeCompatible(chType string, val any) bool {
	// Robustly unwrap nested modifiers (e.g., LowCardinality(Nullable(String)))
	for {
		if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
			chType = chType[9 : len(chType)-1]
			continue
		}
		if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
			chType = chType[15 : len(chType)-1]
			continue
		}
		break
	}

	switch {
	// String-compatible types accept Strings, Numbers (coerced), and Bools
	case chType == "String",
		strings.HasPrefix(chType, "FixedString("),
		chType == "UUID":
		switch val.(type) {
		case string, float64, json.Number, bool:
			return true
		default:
			return false
		}

	// Dates/Times accept Strings and Numbers (Unix timestamps)
	case strings.HasPrefix(chType, "DateTime"),
		strings.HasPrefix(chType, "Date"):
		switch val.(type) {
		case string, float64, json.Number:
			return true
		default:
			return false
		}

	// Enums accept Strings (names) and Numbers (integer mappings)
	case strings.HasPrefix(chType, "Enum8("),
		strings.HasPrefix(chType, "Enum16("):
		switch val.(type) {
		case string, float64, json.Number:
			return true
		default:
			return false
		}

	// IPs accept Strings and Numbers (UInt32 representations)
	case chType == "IPv4", chType == "IPv6":
		switch val.(type) {
		case string, float64, json.Number:
			return true
		default:
			return false
		}

	// Bools accept actual bools, numbers (0/1), and strings ("true"/"false")
	case chType == "Bool":
		switch val.(type) {
		case bool, float64, json.Number, string:
			return true
		default:
			return false
		}

	// Numerics accept Numbers and Strings (to prevent JS precision loss)
	case isNumericType(chType):
		switch val.(type) {
		case float64, json.Number, string:
			return true
		default:
			return false
		}

	// Array types accept JSON arrays.
	case strings.HasPrefix(chType, "Array("):
		_, ok := val.([]any)
		return ok

	// Map types accept JSON objects.
	case strings.HasPrefix(chType, "Map("):
		_, ok := val.(map[string]any)
		return ok

	// Tuple types accept JSON arrays or objects.
	case strings.HasPrefix(chType, "Tuple("):
		_, okArr := val.([]any)
		_, okMap := val.(map[string]any)
		return okArr || okMap

	default:
		// Unknown type — accept any value and let ClickHouse validate.
		return true
	}
}

// isNumericType returns true for ClickHouse integer, float, and decimal types.
func isNumericType(chType string) bool {
	switch {
	case chType == "UInt8", chType == "UInt16", chType == "UInt32", chType == "UInt64", chType == "UInt128", chType == "UInt256":
		return true
	case chType == "Int8", chType == "Int16", chType == "Int32", chType == "Int64", chType == "Int128", chType == "Int256":
		return true
	case chType == "Float32", chType == "Float64":
		return true
	case strings.HasPrefix(chType, "Decimal"):
		return true
	default:
		return false
	}
}
