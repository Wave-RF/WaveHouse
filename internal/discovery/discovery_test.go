package discovery

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_ValidData(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	assert.True(t, isTypeCompatible("Bool", true))
	assert.True(t, isTypeCompatible("Bool", false))
	assert.True(t, isTypeCompatible("Bool", 1.0)) // JSON number 1/0
	assert.False(t, isTypeCompatible("Bool", "true"))
}

func TestIsTypeCompatible_Array(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Array(String)", []any{"a", "b"}))
	assert.False(t, isTypeCompatible("Array(String)", "not-an-array"))
}

func TestIsTypeCompatible_Map(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Map(String, String)", map[string]any{"k": "v"}))
	assert.False(t, isTypeCompatible("Map(String, String)", "not-a-map"))
}

func TestIsTypeCompatible_LowCardinality(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("LowCardinality(String)", "hello"))
	assert.False(t, isTypeCompatible("LowCardinality(String)", 42.0))
}

func TestIsTypeCompatible_NullableUnwrap(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Nullable(String)", "hello"))
	assert.False(t, isTypeCompatible("Nullable(String)", 42.0))
	assert.True(t, isTypeCompatible("Nullable(Float64)", 42.0))
}

func TestIsNullable(t *testing.T) {
	t.Parallel()
	assert.True(t, isNullable("Nullable(String)"))
	assert.False(t, isNullable("String"))
	assert.False(t, isNullable("LowCardinality(String)"))
}

func TestIsTypeCompatible_Tuple(t *testing.T) {
	t.Parallel()
	// Tuple accepts arrays or objects.
	assert.True(t, isTypeCompatible("Tuple(String, Int32)", []any{"a", 1.0}))
	assert.True(t, isTypeCompatible("Tuple(a String, b Int32)", map[string]any{"a": "x", "b": 1.0}))
	assert.False(t, isTypeCompatible("Tuple(String, Int32)", "not-a-tuple"))
	assert.False(t, isTypeCompatible("Tuple(String)", 42.0))
}

func TestIsTypeCompatible_Enum16(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Enum16('a'=1,'b'=2)", "a"))
	assert.False(t, isTypeCompatible("Enum16('a'=1)", 42.0))
}

func TestIsTypeCompatible_UnknownType(t *testing.T) {
	t.Parallel()
	// Unknown types accept any value (let ClickHouse validate).
	assert.True(t, isTypeCompatible("SomeFutureType", "anything"))
	assert.True(t, isTypeCompatible("SomeFutureType", 42.0))
	assert.True(t, isTypeCompatible("SomeFutureType", true))
}

func TestIsTypeCompatible_Decimal(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Decimal(10,2)", 42.5))
	assert.True(t, isTypeCompatible("Decimal128(5)", 99.0))
	assert.False(t, isTypeCompatible("Decimal(10,2)", "not-a-number"))
}

func TestIsTypeCompatible_IPv4IPv6(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("IPv4", "192.168.1.1"))
	assert.True(t, isTypeCompatible("IPv6", "::1"))
	assert.False(t, isTypeCompatible("IPv4", 42.0))
	assert.False(t, isTypeCompatible("IPv6", true))
}

func TestValidate_NilData(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "test",
		Columns: []Column{
			{Name: "id", Type: "UInt64"},
		},
	}
	err := Validate(schema, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column")
}

func TestValidate_EmptyData_AllDefaults(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "test",
		Columns: []Column{
			{Name: "id", Type: "UInt64", HasDefault: true},
			{Name: "name", Type: "String", IsNullable: true},
		},
	}
	assert.NoError(t, Validate(schema, map[string]any{}))
}

func TestIsNumericType(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{
		"UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
		"Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
		"Float32", "Float64",
	} {
		assert.True(t, isNumericType(typ), "expected %q to be numeric", typ)
	}
	assert.False(t, isNumericType("String"))
	assert.False(t, isNumericType("Bool"))
}

func TestNewSchemaRegistry_ConstructorDefaults(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sr := NewSchemaRegistry(nil, "wavehouse", 30*time.Second, logger)
	require.NotNil(t, sr)
	assert.Equal(t, "wavehouse", sr.database)
	assert.Equal(t, 30*time.Second, sr.refreshInterval)
	assert.Same(t, logger, sr.logger)
	assert.NotNil(t, sr.tables)
	assert.Empty(t, sr.List())
	assert.Nil(t, sr.Get("anything"))
}

func TestNewSchemaRegistryFromMap_PopulatesAndLookups(t *testing.T) {
	t.Parallel()
	clicks := &TableSchema{Name: "clicks", Columns: []Column{{Name: "id", Type: "String"}}}
	users := &TableSchema{Name: "users", Columns: []Column{{Name: "id", Type: "UInt64"}}}

	sr := NewSchemaRegistryFromMap([]*TableSchema{clicks, users})
	require.NotNil(t, sr)

	assert.Same(t, clicks, sr.Get("clicks"))
	assert.Same(t, users, sr.Get("users"))
	assert.Nil(t, sr.Get("missing"))

	got := sr.List()
	assert.Len(t, got, 2)
	names := map[string]bool{}
	for _, s := range got {
		names[s.Name] = true
	}
	assert.True(t, names["clicks"])
	assert.True(t, names["users"])
}

func TestNewSchemaRegistryFromMap_Empty(t *testing.T) {
	t.Parallel()
	sr := NewSchemaRegistryFromMap(nil)
	require.NotNil(t, sr)
	assert.Empty(t, sr.List())
	assert.Nil(t, sr.Get("x"))
}

// TestStartAutoRefresh_ExitsOnContextCancel verifies the ticker loop exits
// cleanly when the context is cancelled, without calling Refresh (nil conn
// would otherwise panic).
func TestStartAutoRefresh_ExitsOnContextCancel(t *testing.T) {
	t.Parallel()
	sr := NewSchemaRegistryFromMap(nil)
	// Long interval so the ticker never fires before cancel.
	sr.refreshInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sr.StartAutoRefresh(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("StartAutoRefresh did not return after ctx cancel")
	}
}
