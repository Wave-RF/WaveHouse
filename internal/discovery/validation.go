package discovery

import (
	"encoding/json"
	"fmt"
	"strconv"
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
		col, ok := colMap[key]
		if !ok {
			return fmt.Errorf("unknown column %q for table %q", key, schema.Name)
		}
		// A computed column has no writable storage: ClickHouse refuses to
		// insert a MATERIALIZED column and does not resolve an ALIAS one at
		// all. The published row carries only insertable columns, so a value
		// supplied for one of these would otherwise be dropped on the way out
		// and the record would insert as though it had never been sent.
		// Refuse it here instead, where the caller still hears about it.
		if !col.IsInsertable() {
			return fmt.Errorf("column %q of table %q is %s and cannot be inserted",
				key, schema.Name, strings.ToLower(col.DefaultKind))
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

// unwrapType strips Nullable(...) and LowCardinality(...) modifiers (nested in any
// order, e.g. LowCardinality(Nullable(String))) down to the base ClickHouse type.
func unwrapType(chType string) string {
	for {
		if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
			chType = chType[9 : len(chType)-1]
			continue
		}
		if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
			chType = chType[15 : len(chType)-1]
			continue
		}
		return chType
	}
}

// isTypeCompatible checks whether a Go/JSON value can be stored in the given ClickHouse type.
func isTypeCompatible(chType string, val any) bool {
	chType = unwrapType(chType)

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

// IsNumericType reports whether chType is a ClickHouse numeric type (integer, float,
// or decimal), unwrapping Nullable/LowCardinality modifiers first. The stream
// row-filter evaluator classifies such columns numeric-comparable, so ordering
// predicates (>, <) on numbers match ClickHouse (9 < 100).
func IsNumericType(chType string) bool {
	return isNumericType(unwrapType(chType))
}

// IsStringType reports whether chType is a ClickHouse String (unwrapping
// Nullable/LowCardinality). For String columns, byte comparison IS ClickHouse
// comparison — equality and lexicographic order alike — so the stream row-filter
// evaluator can compare them exactly. FixedString is deliberately excluded: its
// stored values are zero-padded to the declared width, so a byte comparison of an
// ingested value against a filter constant would not match ClickHouse.
func IsStringType(chType string) bool {
	return unwrapType(chType) == "String"
}

// NumericStorage describes how a ClickHouse numeric column stores a value —
// the narrowing AND range the stream row-filter must apply to BOTH comparison
// operands so its verdicts match the query path, where ClickHouse narrows the
// stored value at insert and the bound constant at compare, and errors the
// query outright on a constant outside the column's range. Each family carries
// its parameters: Integer + IntBits/Unsigned for Int*/UInt* (exact within the
// width's range), FloatBits (32/64) for Float*, Precision+Scale for Decimal*.
type NumericStorage struct {
	Integer   bool
	IntBits   int
	Unsigned  bool
	FloatBits int
	Precision int
	Scale     int
}

// NumericStorageOf classifies chType's numeric storage model, unwrapping
// Nullable/LowCardinality. ok=false for non-numeric types AND for a Decimal
// whose precision/scale cannot be parsed — the caller must then refuse numeric
// comparison rather than compare under a guessed model (fail closed).
// system.columns always reports the two-argument canonical Decimal(P, S) form
// (Decimal32(4) is stored as Decimal(9, 4)); the shorthand widths are handled
// anyway for robustness, and a bare single-argument Decimal(P) is refused
// rather than misread.
func NumericStorageOf(chType string) (NumericStorage, bool) {
	chType = unwrapType(chType)
	switch {
	case !isNumericType(chType):
		return NumericStorage{}, false
	case chType == "Float32":
		return NumericStorage{FloatBits: 32}, true
	case chType == "Float64":
		return NumericStorage{FloatBits: 64}, true
	case strings.HasPrefix(chType, "Decimal"):
		p, s, ok := decimalParams(chType)
		if !ok {
			return NumericStorage{}, false
		}
		return NumericStorage{Precision: p, Scale: s}, true
	default: // isNumericType admits only Int*/UInt* beyond the cases above
		bits, unsigned, ok := integerWidth(chType)
		if !ok {
			return NumericStorage{}, false
		}
		return NumericStorage{Integer: true, IntBits: bits, Unsigned: unsigned}, true
	}
}

// integerWidth reads the bit width and signedness from an Int*/UInt* type name.
func integerWidth(chType string) (bits int, unsigned bool, ok bool) {
	rest, found := strings.CutPrefix(chType, "UInt")
	if found {
		unsigned = true
	} else {
		rest, found = strings.CutPrefix(chType, "Int")
		if !found {
			return 0, false, false
		}
	}
	bits, err := strconv.Atoi(rest)
	if err != nil {
		return 0, false, false
	}
	switch bits {
	case 8, 16, 32, 64, 128, 256:
		return bits, unsigned, true
	default:
		return 0, false, false
	}
}

// decimalParams extracts (P, S) from Decimal(P, S) and the DecimalN(S)
// shorthands (Decimal32/64/128/256, whose precisions are fixed at 9/18/38/76).
// ClickHouse bounds them to 1 ≤ P ≤ 76 and 0 ≤ S ≤ P; anything outside that,
// malformed, or a single-argument Decimal(P) — whose lone number is a
// precision, not a scale — reports ok=false.
func decimalParams(chType string) (precision, scale int, ok bool) {
	open := strings.IndexByte(chType, '(')
	if open < 0 || !strings.HasSuffix(chType, ")") {
		return 0, 0, false
	}
	args := strings.Split(chType[open+1:len(chType)-1], ",")
	last, err := strconv.Atoi(strings.TrimSpace(args[len(args)-1]))
	if err != nil {
		return 0, 0, false
	}
	switch prefix := chType[:open]; prefix {
	case "Decimal32":
		precision = 9
	case "Decimal64":
		precision = 18
	case "Decimal128":
		precision = 38
	case "Decimal256":
		precision = 76
	case "Decimal":
		if len(args) != 2 {
			return 0, 0, false
		}
		if precision, err = strconv.Atoi(strings.TrimSpace(args[0])); err != nil {
			return 0, 0, false
		}
	default:
		return 0, 0, false
	}
	scale = last
	if precision < 1 || precision > 76 || scale < 0 || scale > precision {
		return 0, 0, false
	}
	return precision, scale, true
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
