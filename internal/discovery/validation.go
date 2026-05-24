package discovery

import (
	"fmt"
	"strings"
)

// ClassifyValidationError maps a Validate error to a coarse reason label for
// the wavehouse_schema_validation_rejected_total counter. Substring-based
// because Validate currently emits errors via fmt.Errorf — promoting them to
// typed sentinels is a bigger refactor than this PR's scope. Keep the
// substrings in lock-step with the messages emitted by Validate below.
func ClassifyValidationError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unknown column"):
		return "unknown_field"
	case strings.Contains(msg, "missing required column"):
		return "missing_required"
	case strings.Contains(msg, "null value for non-nullable"):
		return "null_violation"
	case strings.Contains(msg, "type mismatch"):
		return "type_mismatch"
	default:
		return "other"
	}
}

// Validate checks that the given data matches the table schema.
// It rejects unknown fields and checks type compatibility.
func Validate(schema *TableSchema, data map[string]any) error {
	colMap := make(map[string]Column, len(schema.Columns))
	for _, col := range schema.Columns {
		colMap[col.Name] = col
	}

	// Reject unknown fields.
	for key := range data {
		if _, ok := colMap[key]; !ok {
			return fmt.Errorf("unknown column %q for table %q", key, schema.Name)
		}
	}

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
	// Unwrap Nullable.
	if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
		chType = chType[9 : len(chType)-1]
	}

	// Unwrap LowCardinality.
	if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
		chType = chType[15 : len(chType)-1]
	}

	switch {
	// String-compatible types accept JSON strings.
	case chType == "String",
		strings.HasPrefix(chType, "FixedString("),
		chType == "UUID",
		strings.HasPrefix(chType, "DateTime"),
		strings.HasPrefix(chType, "Date"),
		strings.HasPrefix(chType, "Enum8("),
		strings.HasPrefix(chType, "Enum16("),
		chType == "IPv4",
		chType == "IPv6":
		_, ok := val.(string)
		return ok

	// Numeric types accept JSON numbers (float64 from json.Unmarshal).
	case chType == "Bool":
		_, okBool := val.(bool)
		_, okNum := val.(float64)
		return okBool || okNum

	case isNumericType(chType):
		_, ok := val.(float64)
		return ok

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
