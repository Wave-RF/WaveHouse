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
