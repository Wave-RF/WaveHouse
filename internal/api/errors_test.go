package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSONError_SetsContentTypeAndStatus(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSONError(w, http.StatusBadRequest, "boom")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "boom", body["error"])
}

func TestWriteJSONError_EscapesSpecialCharacters(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSONError(w, http.StatusInternalServerError, `oops "quoted" \n`)

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, `oops "quoted" \n`, body["error"])
}

// assertJSONErrorResponse verifies that a recorded response is a well-formed
// JSON error body with the exact headers writeJSONError guarantees:
// Content-Type: application/json and X-Content-Type-Options: nosniff.
// Pinned strict so any handler that bypasses writeJSONError and emits a
// different Content-Type fails the assertion across every call site.
func assertJSONErrorResponse(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body), "error body must be valid JSON")
	_, hasError := body["error"]
	assert.True(t, hasError, "JSON error body should contain an 'error' field")
}
