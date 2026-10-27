// Package chconntest reads back what a test's code under test put on a
// query context for clickhouse-go. The driver keeps its query options in
// unexported fields with no getter, so this reads them by reflection. It is
// for tests only: a driver upgrade that renames a field fails the test that
// uses it, rather than letting it pass on a zero value.
package chconntest

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel/trace"
)

// QueryOptions returns the trace context and the settings clickhouse.Context
// put on ctx, both zero when it put nothing.
func QueryOptions(ctx context.Context, t *testing.T) (trace.SpanContext, clickhouse.Settings) {
	t.Helper()
	// clickhouse.Context stores its options under one unexported key; a
	// context it made itself has that key in its own valueCtx.
	probe := reflect.ValueOf(clickhouse.Context(context.Background())).Elem()
	key := readField(t, probe, "key")
	opts, ok := ctx.Value(key).(clickhouse.QueryOptions)
	if !ok {
		return trace.SpanContext{}, nil
	}
	v := reflect.ValueOf(&opts).Elem()
	span, _ := readField(t, v, "span").(trace.SpanContext)
	settings, _ := readField(t, v, "settings").(clickhouse.Settings)
	return span, settings
}

// readField is v's field name, unexported or not. v must be addressable.
func readField(t *testing.T, v reflect.Value, name string) any {
	t.Helper()
	f := v.FieldByName(name)
	if !f.IsValid() {
		t.Fatalf("chconntest: %s has no field %q; the clickhouse-go internals changed", v.Type(), name)
	}
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface() //nolint:gosec // G103: reads a driver field that has no getter, in tests only
}
