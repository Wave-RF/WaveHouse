package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
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
// The registry holds schemas rebuilt from those rows (Name, Type, HasDefault,
// DefaultKind, DefaultExpression, Position, DDL; IsNullable derived from the
// type string), not the caller's structs.
func NewTestSchemaRegistry(t testing.TB, tables []*discovery.TableSchema) *discovery.SchemaRegistry {
	t.Helper()
	reg := discovery.NewSchemaRegistry(&schemaConn{tables: tables}, func() string { return "test" }, func() time.Duration { return time.Hour }, NopLogger())
	require.NoError(t, reg.Refresh(context.Background()))
	return reg
}

// TestServerVersion is the ClickHouse version NewTestSchemaRegistry's mock
// connection reports, so a test can assert against ServerVersion() without
// hardcoding the same literal twice.
const TestServerVersion = "24.8.1.1"

// schemaConn is a mock driver.Conn serving exactly the queries Refresh issues:
// the SELECT timezone() (always "UTC") and SELECT version() probes, the
// system.columns scan (rows synthesized from tables), and the system.tables DDL
// scan. The nil embedded interface panics on any other method — nothing else is
// part of Refresh's contract.
type schemaConn struct {
	driver.Conn
	tables []*discovery.TableSchema
}

func (c *schemaConn) QueryRow(_ context.Context, q string, _ ...any) driver.Row {
	if strings.Contains(q, "version()") {
		return scalarRow{val: TestServerVersion}
	}
	return UTCRow{}
}

func (c *schemaConn) Query(_ context.Context, q string, _ ...any) (driver.Rows, error) {
	if strings.Contains(q, "system.tables") {
		r := &tableRows{}
		for _, t := range c.tables {
			ddl := t.DDL
			if ddl == "" {
				ddl = "CREATE TABLE test." + t.Name + " (…) ENGINE = MergeTree"
			}
			r.rows = append(r.rows, [2]string{t.Name, ddl})
		}
		return r, nil
	}
	r := &columnRows{}
	for _, t := range c.tables {
		for i, col := range t.Columns {
			// An explicit DefaultKind wins, so a fixture can declare a
			// MATERIALIZED/ALIAS/EPHEMERAL column; HasDefault alone still means
			// a plain DEFAULT, as it always has.
			kind := col.DefaultKind
			if kind == "" && col.HasDefault {
				kind = "DEFAULT"
			}
			r.rows = append(r.rows, columnRow{
				vals: [5]string{t.Name, col.Name, col.Type, kind, col.DefaultExpression},
				// Declaration order is the slice order the caller wrote, so the
				// synthesized position matches what a real system.columns scan
				// (ORDER BY position) would have produced.
				position: uint64(i + 1),
			})
		}
	}
	return r, nil
}

// UTCRow is a driver.Row stub answering Refresh's single-string probes with
// "UTC" — for any mock driver.Conn that must satisfy the schema-refresh path
// without caring which probe it is answering.
type UTCRow struct{ driver.Row }

func (UTCRow) Scan(dest ...any) error {
	if len(dest) == 1 {
		if s, ok := dest[0].(*string); ok {
			*s = "UTC"
			return nil
		}
	}
	return fmt.Errorf("unexpected scalar probe scan into %d dest(s)", len(dest))
}

// scalarRow answers a one-column probe with a canned string.
type scalarRow struct {
	driver.Row
	val string
}

func (r scalarRow) Scan(dest ...any) error {
	if len(dest) == 1 {
		if s, ok := dest[0].(*string); ok {
			*s = r.val
			return nil
		}
	}
	return fmt.Errorf("unexpected scalar probe scan into %d dest(s)", len(dest))
}

// columnRow is one synthesized system.columns row: the five string columns
// Refresh scans plus the numeric position.
type columnRow struct {
	vals     [5]string // table, name, type, default_kind, default_expression
	position uint64
}

// columnRows plays a system.columns result set, in the given order.
type columnRows struct {
	driver.Rows
	rows []columnRow
	next int
}

func (r *columnRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *columnRows) Scan(dest ...any) error {
	row := r.rows[r.next-1]
	if len(dest) != len(row.vals)+1 {
		return fmt.Errorf("expected %d scan dests, got %d", len(row.vals)+1, len(dest))
	}
	for i, want := range row.vals {
		s, ok := dest[i].(*string)
		if !ok {
			return fmt.Errorf("dest %d: expected *string, got %T", i, dest[i])
		}
		*s = want
	}
	pos, ok := dest[len(row.vals)].(*uint64)
	if !ok {
		return fmt.Errorf("dest %d: expected *uint64, got %T", len(row.vals), dest[len(row.vals)])
	}
	*pos = row.position
	return nil
}

func (*columnRows) Close() error { return nil }
func (*columnRows) Err() error   { return nil }

// tableRows plays a system.tables result set: one {name, create_table_query}
// pair per table.
type tableRows struct {
	driver.Rows
	rows [][2]string
	next int
}

func (r *tableRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *tableRows) Scan(dest ...any) error {
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

func (*tableRows) Close() error { return nil }
func (*tableRows) Err() error   { return nil }

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
