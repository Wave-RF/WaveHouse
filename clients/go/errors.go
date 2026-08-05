package wavehouse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Error is the structured error returned by all SDK operations. Use
// [errors.As] to extract it from wrapped errors.
type Error struct {
	// Status is the HTTP status code (0 for network/abort errors).
	Status int `json:"status"`
	// Code is a machine-readable error code (e.g. "HTTP_400", "NETWORK_ERROR", "ABORTED").
	Code string `json:"code"`
	// Message is a human-readable description.
	Message string `json:"message"`
	// Details contains the full parsed error body, if available.
	Details map[string]any `json:"details,omitempty"`
	// Retryable indicates whether the request can be retried.
	Retryable bool `json:"retryable"`
}

func (e *Error) Error() string {
	if e.Status > 0 {
		return fmt.Sprintf("wavehouse: %s (%d): %s", e.Code, e.Status, e.Message)
	}
	return fmt.Sprintf("wavehouse: %s: %s", e.Code, e.Message)
}

// IsRetryable reports whether err wraps a retryable [*Error].
func IsRetryable(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Retryable
	}
	return false
}

// parseErrorResponse creates an Error from an HTTP response.
func parseErrorResponse(res *http.Response) *Error {
	var body map[string]any
	if res.Body != nil {
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20)) // cap at 1 MiB
		_ = json.Unmarshal(raw, &body)
	}

	msg := ""
	if s, ok := body["error"].(string); ok {
		msg = s
	} else if s, ok := body["message"].(string); ok {
		msg = s
	} else {
		msg = http.StatusText(res.StatusCode)
	}

	retryable := res.StatusCode == http.StatusServiceUnavailable || res.StatusCode >= 500
	return &Error{
		Status:    res.StatusCode,
		Code:      fmt.Sprintf("HTTP_%d", res.StatusCode),
		Message:   msg,
		Details:   body,
		Retryable: retryable,
	}
}

// networkError creates an Error from a transport-level failure.
func networkError(cause error) *Error {
	msg := "unknown network error"
	if cause != nil {
		msg = cause.Error()
	}
	return &Error{
		Status:    0,
		Code:      "NETWORK_ERROR",
		Message:   msg,
		Retryable: true,
	}
}
