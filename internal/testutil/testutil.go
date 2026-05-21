package testutil

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

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
// table schemas. Does not require a real ClickHouse connection.
func NewTestSchemaRegistry(tables ...*discovery.TableSchema) *discovery.SchemaRegistry {
	reg := discovery.NewSchemaRegistryFromMap(tables)
	return reg
}

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
