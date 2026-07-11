package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// NopLogger returns a *slog.Logger that discards all output.
// Use in tests to suppress noisy log output from embedded NATS, etc.
func NopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// NewTestSchemaRegistry creates a SchemaRegistry pre-loaded with the given
// table schemas, without a real ClickHouse: a mock connection serves the
// schemas as system.columns rows (UTC as the server zone) and the registry is
// built by the real discovery path — NewSchemaRegistry + Refresh — so
// timestamp column specs are precomputed exactly as in production.
//
// The registry holds schemas rebuilt from those rows (Name, Type, HasDefault;
// IsNullable derived from the type string), not the caller's structs.
func NewTestSchemaRegistry(t testing.TB, tables []*discovery.TableSchema) *discovery.SchemaRegistry {
	t.Helper()
	reg := discovery.NewSchemaRegistry(&schemaConn{tables: tables}, "test", time.Hour, NopLogger())
	require.NoError(t, reg.Refresh(context.Background()))
	return reg
}

// schemaConn is a mock driver.Conn serving exactly the two queries Refresh
// issues: the SELECT timezone() probe (always "UTC") and the system.columns
// scan (rows synthesized from tables). The nil embedded interface panics on
// any other method — nothing else is part of Refresh's contract.
type schemaConn struct {
	driver.Conn
	tables []*discovery.TableSchema
}

func (c *schemaConn) QueryRow(context.Context, string, ...any) driver.Row {
	return utcRow{}
}

func (c *schemaConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	r := &columnRows{}
	for _, t := range c.tables {
		for _, col := range t.Columns {
			kind := ""
			if col.HasDefault {
				kind = "DEFAULT"
			}
			r.rows = append(r.rows, [4]string{t.Name, col.Name, col.Type, kind})
		}
	}
	return r, nil
}

type utcRow struct{ driver.Row }

func (utcRow) Scan(dest ...any) error {
	if len(dest) == 1 {
		if s, ok := dest[0].(*string); ok {
			*s = "UTC"
			return nil
		}
	}
	return fmt.Errorf("unexpected timezone scan into %d dest(s)", len(dest))
}

// columnRows plays a system.columns result set: one [table, name, type,
// default_kind] row per column, in the given order.
type columnRows struct {
	driver.Rows
	rows [][4]string
	next int
}

func (r *columnRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *columnRows) Scan(dest ...any) error {
	row := r.rows[r.next-1]
	if len(dest) != len(row) {
		return fmt.Errorf("expected %d scan dests, got %d", len(row), len(dest))
	}
	for i, d := range dest {
		s, ok := d.(*string)
		if !ok {
			return fmt.Errorf("dest %d: expected *string, got %T", i, d)
		}
		*s = row[i]
	}
	return nil
}

func (*columnRows) Close() error { return nil }
func (*columnRows) Err() error   { return nil }

// AssertJSONResponse checks that rec has the expected status code and that
// the response body, decoded as JSON, matches expected (compared as Go values).
func AssertJSONResponse(t *testing.T, rec *httptest.ResponseRecorder, expectedStatus int, expected any) {
	t.Helper()
	assert.Equal(t, expectedStatus, rec.Code, "unexpected status code")

	var got any
	err := json.Unmarshal(rec.Body.Bytes(), &got)
	require.NoError(t, err, "response body is not valid JSON: %s", rec.Body.String())
	assert.Equal(t, expected, got)
}

// AssertJSONContains checks that rec has the expected status code and that
// the response body contains the expected key-value pairs.
func AssertJSONContains(t *testing.T, rec *httptest.ResponseRecorder, expectedStatus int, expectedKeys map[string]any) {
	t.Helper()
	assert.Equal(t, expectedStatus, rec.Code, "unexpected status code")

	var got map[string]any
	err := json.Unmarshal(rec.Body.Bytes(), &got)
	require.NoError(t, err, "response body is not valid JSON: %s", rec.Body.String())
	for k, v := range expectedKeys {
		assert.Equal(t, v, got[k], "key %q mismatch", k)
	}
}

// assertJSONErrorResponse verifies that a recorded response is a well-formed
// JSON error body with the exact headers writeJSONError guarantees:
// Content-Type: application/json and X-Content-Type-Options: nosniff.
// Pinned strict so any handler that bypasses writeJSONError and emits a
// different Content-Type fails the assertion across every call site.
func AssertJSONErrorResponse(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body), "error body must be valid JSON")
	_, hasError := body["error"]
	assert.True(t, hasError, "JSON error body should contain an 'error' field")
}

// AssertBodyContains checks that rec has the expected status code and that
// the response body contains the expected substring.
func AssertBodyContains(t *testing.T, rec *httptest.ResponseRecorder, expectedStatus int, expectedSubstring string) {
	t.Helper()
	assert.Equal(t, expectedStatus, rec.Code, "unexpected status code")
	assert.Contains(t, rec.Body.String(), expectedSubstring, "response body does not contain expected substring")
}

// AssertBodyEquals checks that rec has the expected status code and that
// the response body equals the expected string.
func AssertBodyEquals(t *testing.T, rec *httptest.ResponseRecorder, expectedStatus int, expectedBody string) {
	t.Helper()
	assert.Equal(t, expectedStatus, rec.Code, "unexpected status code")
	assert.Equal(t, expectedBody, rec.Body.String(), "response body does not equal expected")
}
