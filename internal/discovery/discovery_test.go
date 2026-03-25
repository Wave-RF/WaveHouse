package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_ValidData(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "amount", Type: "Float64"},
			{Name: "clicked_at", Type: "DateTime64(3, 'UTC')"},
		},
	}

	data := map[string]any{
		"user_id":    "alice",
		"amount":     42.5,
		"clicked_at": "2025-01-01T00:00:00Z",
	}

	err := Validate(schema, data)
	assert.NoError(t, err)
}

func TestValidate_UnknownField(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
		},
	}

	data := map[string]any{
		"user_id": "alice",
		"unknown": "value",
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestValidate_TypeMismatch(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
		},
	}

	data := map[string]any{
		"user_id": 123.0, // number, not string
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestValidate_MissingRequiredColumn(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "amount", Type: "Float64"},
		},
	}

	data := map[string]any{
		"user_id": "alice",
		// amount is missing and has no default
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column")
}

func TestValidate_NullableColumnCanBeOmitted(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "notes", Type: "Nullable(String)", IsNullable: true},
		},
	}

	data := map[string]any{
		"user_id": "alice",
	}

	assert.NoError(t, Validate(schema, data))
}

func TestValidate_DefaultColumnCanBeOmitted(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "created_at", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		},
	}

	data := map[string]any{
		"user_id": "alice",
	}

	assert.NoError(t, Validate(schema, data))
}

func TestValidate_NullForNonNullable(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
		},
	}

	data := map[string]any{
		"user_id": nil,
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null value for non-nullable")
}

func TestValidate_NullForNullable(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "notes", Type: "Nullable(String)", IsNullable: true},
		},
	}

	data := map[string]any{
		"notes": nil,
	}

	assert.NoError(t, Validate(schema, data))
}

func TestValidate_NullForNonNullableWithDefault(t *testing.T) {
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "created_at", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		},
	}

	data := map[string]any{
		"user_id":    "alice",
		"created_at": nil, // non-nullable but has a default — should be accepted
	}

	assert.NoError(t, Validate(schema, data))
}

func TestIsTypeCompatible_StringTypes(t *testing.T) {
	stringTypes := []string{
		"String", "FixedString(32)", "UUID",
		"DateTime64(3, 'UTC')", "Date", "Date32",
		"Enum8('a'=1)", "IPv4", "IPv6",
	}
	for _, ct := range stringTypes {
		assert.True(t, isTypeCompatible(ct, "hello"), "expected %s to accept string", ct)
		assert.False(t, isTypeCompatible(ct, 42.0), "expected %s to reject float64", ct)
	}
}

func TestIsTypeCompatible_NumericTypes(t *testing.T) {
	numTypes := []string{
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Int8", "Int16", "Int32", "Int64",
		"Float32", "Float64",
		"Decimal(18, 4)",
	}
	for _, ct := range numTypes {
		assert.True(t, isTypeCompatible(ct, 42.0), "expected %s to accept float64", ct)
		assert.False(t, isTypeCompatible(ct, "hello"), "expected %s to reject string", ct)
	}
}

func TestIsTypeCompatible_Bool(t *testing.T) {
	assert.True(t, isTypeCompatible("Bool", true))
	assert.True(t, isTypeCompatible("Bool", false))
	assert.True(t, isTypeCompatible("Bool", 1.0)) // JSON number 1/0
	assert.False(t, isTypeCompatible("Bool", "true"))
}

func TestIsTypeCompatible_Array(t *testing.T) {
	assert.True(t, isTypeCompatible("Array(String)", []any{"a", "b"}))
	assert.False(t, isTypeCompatible("Array(String)", "not-an-array"))
}

func TestIsTypeCompatible_Map(t *testing.T) {
	assert.True(t, isTypeCompatible("Map(String, String)", map[string]any{"k": "v"}))
	assert.False(t, isTypeCompatible("Map(String, String)", "not-a-map"))
}

func TestIsTypeCompatible_LowCardinality(t *testing.T) {
	assert.True(t, isTypeCompatible("LowCardinality(String)", "hello"))
	assert.False(t, isTypeCompatible("LowCardinality(String)", 42.0))
}

func TestIsTypeCompatible_NullableUnwrap(t *testing.T) {
	assert.True(t, isTypeCompatible("Nullable(String)", "hello"))
	assert.False(t, isTypeCompatible("Nullable(String)", 42.0))
	assert.True(t, isTypeCompatible("Nullable(Float64)", 42.0))
}

func TestIsNullable(t *testing.T) {
	assert.True(t, isNullable("Nullable(String)"))
	assert.False(t, isNullable("String"))
	assert.False(t, isNullable("LowCardinality(String)"))
}
