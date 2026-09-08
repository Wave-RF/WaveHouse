package discovery

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	// A shared schema used across multiple tests
	baseSchema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String", IsNullable: false},
			{Name: "amount", Type: "Float64", IsNullable: false},
			{Name: "notes", Type: "Nullable(String)", IsNullable: true},
			{Name: "created_at", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		},
	}

	tests := []struct {
		name    string
		schema  *TableSchema
		data    map[string]any
		wantErr string // Substring to match in error; empty means success expected
	}{
		{
			name:   "valid data all fields",
			schema: baseSchema,
			data: map[string]any{
				"user_id":    "alice",
				"amount":     json.Number("42.5"),
				"notes":      "hello",
				"created_at": "2025-01-01T00:00:00Z",
			},
		},
		{
			name:   "unknown field rejected",
			schema: baseSchema,
			data: map[string]any{
				"user_id": "alice",
				"amount":  json.Number("42.5"),
				"unknown": "value",
			},
			wantErr: "unknown column",
		},
		{
			name:   "type mismatch",
			schema: baseSchema,
			data: map[string]any{
				"user_id": json.Number("123"),      // json.Number is valid for String
				"amount":  []any{"arrays", "fail"}, // invalid for Float64
			},
			wantErr: "type mismatch",
		},
		{
			name:   "missing required column",
			schema: baseSchema,
			data: map[string]any{
				"user_id": "alice",
				// amount is missing, not nullable, no default
			},
			wantErr: "missing required column",
		},
		{
			name:   "missing nullable column is allowed",
			schema: baseSchema,
			data: map[string]any{
				"user_id": "alice",
				"amount":  json.Number("42.5"),
				// notes is omitted
			},
		},
		{
			name:   "missing default column is allowed",
			schema: baseSchema,
			data: map[string]any{
				"user_id": "alice",
				"amount":  json.Number("42.5"),
				// created_at is omitted
			},
		},
		{
			name:   "nil for non-nullable rejected",
			schema: baseSchema,
			data: map[string]any{
				"user_id": nil, // non-nullable
				"amount":  json.Number("42.5"),
			},
			wantErr: "null value for non-nullable",
		},
		{
			name:   "nil for nullable allowed",
			schema: baseSchema,
			data: map[string]any{
				"user_id": "alice",
				"amount":  json.Number("42.5"),
				"notes":   nil,
			},
		},
		{
			name:   "nil for non-nullable with default allowed",
			schema: baseSchema,
			data: map[string]any{
				"user_id":    "alice",
				"amount":     json.Number("42.5"),
				"created_at": nil,
			},
		},
		{
			name:    "nil data triggers missing column",
			schema:  baseSchema,
			data:    nil,
			wantErr: "missing required column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := Validate(tt.schema, tt.data)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIsTypeCompatible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		chType string
		val    any
		want   bool
	}{
		// String and Date types
		{"String accepts string", "String", "hello", true},
		{"String accepts float64", "String", 42.0, true},
		{"String accepts json.Number", "String", json.Number("42"), true},
		{"String accepts bool", "String", true, true},
		{"String rejects array", "String", []any{}, false},
		{"DateTime accepts string", "DateTime64(3, 'UTC')", "2025-01-01", true},
		{"DateTime accepts number", "DateTime64", json.Number("1716570889"), true},

		// Numeric types
		{"UInt64 accepts float64", "UInt64", 42.0, true},
		{"UInt64 accepts json.Number", "UInt64", json.Number("1234567890"), true},
		{"UInt64 accepts string", "UInt64", "1234567890", true},
		{"Decimal accepts string", "Decimal(18,4)", "42.5000", true},
		{"Float64 rejects bool", "Float64", true, false},

		// Bool
		{"Bool accepts bool", "Bool", true, true},
		{"Bool accepts float64", "Bool", 1.0, true},
		{"Bool accepts json.Number", "Bool", json.Number("0"), true},
		{"Bool accepts string", "Bool", "true", true},
		{"Bool rejects array", "Bool", []any{}, false},

		// Enums & IPs
		{"Enum accepts string", "Enum16('a'=1,'b'=2)", "a", true},
		{"Enum accepts number", "Enum8('a'=1)", json.Number("1"), true},
		{"IPv4 accepts string", "IPv4", "192.168.1.1", true},
		{"IPv4 accepts number", "IPv4", json.Number("3232235777"), true},

		// Complex Types
		{"Array accepts slice", "Array(String)", []any{"a", "b"}, true},
		{"Array rejects string", "Array(String)", "not-an-array", false},
		{"Map accepts map", "Map(String, String)", map[string]any{"k": "v"}, true},
		{"Tuple accepts slice", "Tuple(String, Int32)", []any{"a", 1.0}, true},
		{"Tuple accepts map", "Tuple(a String, b Int32)", map[string]any{"a": "x", "b": 1.0}, true},

		// Modifiers (Nullable / LowCardinality)
		{"Nullable accepts valid", "Nullable(String)", "hello", true},
		{"LowCardinality accepts valid", "LowCardinality(String)", "hello", true},
		{"Nested modifiers unwrapped 1", "LowCardinality(Nullable(String))", "hello", true},
		{"Nested modifiers unwrapped 2", "Nullable(LowCardinality(String))", "hello", true},
		{"Nested modifiers reject invalid", "LowCardinality(Nullable(UInt64))", []any{}, false},

		// Fallback
		{"Unknown type accepts everything", "SomeFutureType", []any{"sure"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isTypeCompatible(tt.chType, tt.val)
			assert.Equal(t, tt.want, got, "chType=%q, val=%T(%v)", tt.chType, tt.val, tt.val)
		})
	}
}

func TestIsNumericType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		chType string
		want   bool
	}{
		{"UInt8", true},
		{"UInt256", true},
		{"Int16", true},
		{"Int128", true},
		{"Float32", true},
		{"Float64", true},
		{"Decimal(10,2)", true},
		{"Decimal128(5)", true},
		{"String", false},
		{"Bool", false},
	}

	for _, tt := range tests {
		t.Run(tt.chType, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isNumericType(tt.chType))
		})
	}
}

// TestIsNumericType_Exported checks the exported wrapper the stream row-filter uses:
// it must unwrap Nullable/LowCardinality (in any nesting) before classifying, so a
// Nullable(UInt64) column still compares numerically.
func TestIsNumericType_Exported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		chType string
		want   bool
	}{
		{"UInt64", true},
		{"Nullable(UInt64)", true},
		{"LowCardinality(Int32)", true},
		{"LowCardinality(Nullable(Float64))", true},
		{"Decimal(10,2)", true},
		{"String", false},
		{"Nullable(String)", false},
		{"LowCardinality(String)", false},
		{"DateTime", false},
	}

	for _, tt := range tests {
		t.Run(tt.chType, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsNumericType(tt.chType))
		})
	}
}

// TestIsStringType: only String (under any Nullable/LowCardinality wrapping)
// qualifies — byte comparison is ClickHouse comparison for it. FixedString is
// excluded on purpose (zero-padded storage), as is everything whose text form is
// not canonical (UUID, Enum, DateTime, Bool).
func TestIsStringType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		chType string
		want   bool
	}{
		{"String", true},
		{"Nullable(String)", true},
		{"LowCardinality(String)", true},
		{"LowCardinality(Nullable(String))", true},
		{"FixedString(16)", false},
		{"UUID", false},
		{"Enum8('a' = 1)", false},
		{"DateTime", false},
		{"Bool", false},
		{"UInt64", false},
	}

	for _, tt := range tests {
		t.Run(tt.chType, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsStringType(tt.chType))
		})
	}
}

// TestNumericStorageOf pins the storage classification the stream row-filter
// narrows comparisons with: integer family exact at any width, float bit
// widths, Decimal scale extraction across every declaration form, wrappers
// unwrapped, and ok=false for non-numerics and for a Decimal whose scale can't
// be parsed — the caller must refuse comparison rather than guess a model.
func TestNumericStorageOf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		chType string
		want   NumericStorage
		ok     bool
	}{
		{"UInt64", NumericStorage{Integer: true, IntBits: 64, Unsigned: true}, true},
		{"Int256", NumericStorage{Integer: true, IntBits: 256}, true},
		{"Nullable(UInt32)", NumericStorage{Integer: true, IntBits: 32, Unsigned: true}, true},
		{"Float32", NumericStorage{FloatBits: 32}, true},
		{"LowCardinality(Nullable(Float64))", NumericStorage{FloatBits: 64}, true},
		{"Decimal(10, 2)", NumericStorage{Precision: 10, Scale: 2}, true},
		{"Decimal(10,2)", NumericStorage{Precision: 10, Scale: 2}, true},
		{"Decimal(2, 2)", NumericStorage{Precision: 2, Scale: 2}, true},
		{"Decimal32(4)", NumericStorage{Precision: 9, Scale: 4}, true},
		{"Decimal64(0)", NumericStorage{Precision: 18, Scale: 0}, true},
		{"Decimal256(76)", NumericStorage{Precision: 76, Scale: 76}, true},
		{"Decimal", NumericStorage{}, false},
		{"Decimal(10)", NumericStorage{}, false},
		{"Decimal(10, -1)", NumericStorage{}, false},
		{"Decimal(10, 77)", NumericStorage{}, false},
		{"String", NumericStorage{}, false},
		{"DateTime", NumericStorage{}, false},
		{"Bool", NumericStorage{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.chType, func(t *testing.T) {
			t.Parallel()
			got, ok := NumericStorageOf(tt.chType)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestValidate_RejectsSuppliedComputedColumn: a record naming a MATERIALIZED or
// ALIAS column must be refused where the caller still hears about it. The
// published row carries only insertable columns, so without this the value
// would be silently dropped and the record would insert as though it had never
// been sent — the failure mode that made this class invisible.
func TestValidate_RejectsSuppliedComputedColumn(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{Name: "clicks", Columns: []Column{
		{Name: "id", Type: "UInt64"},
		{Name: "mat", Type: "String", DefaultKind: "MATERIALIZED", HasDefault: true},
		{Name: "ali", Type: "UInt64", DefaultKind: "ALIAS", HasDefault: true},
	}}

	for _, tt := range []struct{ name, col, want string }{
		{"materialized", "mat", "materialized and cannot be inserted"},
		{"alias", "ali", "alias and cannot be inserted"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := Validate(schema, map[string]any{"id": float64(1), tt.col: "x"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.col)
			assert.Contains(t, err.Error(), tt.want)
		})
	}

	// Omitting them is the normal case and must still pass: they are not
	// "missing required columns", they are computed by the server.
	require.NoError(t, Validate(schema, map[string]any{"id": float64(1)}))
}
