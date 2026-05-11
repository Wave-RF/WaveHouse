package api

import (
	"encoding/json"
	"net/http"
)

// writeJSONError writes a JSON error response with the correct Content-Type
// header. Unlike http.Error, which forces Content-Type: text/plain, this
// helper guarantees every error body emitted by the API is identified as
// application/json so strict clients and SDKs can parse it consistently.
//
// The Content-Type is "application/json" without a charset parameter to
// match the success-path handlers and RFC 8259 (which does not define a
// charset for application/json — JSON is required to be UTF-8 already).
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
