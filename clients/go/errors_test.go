package wavehouse

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseErrorResponse_JSONError(t *testing.T) {
	res := &http.Response{
		StatusCode: 404,
		Body:       io.NopCloser(strings.NewReader(`{"error":"unknown table: foo"}`)),
		Header:     http.Header{},
	}
	e := parseErrorResponse(res)
	if e.Status != 404 {
		t.Fatalf("want status 404, got %d", e.Status)
	}
	if e.Code != "HTTP_404" {
		t.Fatalf("want code HTTP_404, got %s", e.Code)
	}
	if e.Message != "unknown table: foo" {
		t.Fatalf("want message 'unknown table: foo', got %s", e.Message)
	}
	if e.Retryable {
		t.Fatal("4xx should not be retryable")
	}
}

func TestParseErrorResponse_MessageField(t *testing.T) {
	res := &http.Response{
		StatusCode: 400,
		Body:       io.NopCloser(strings.NewReader(`{"message":"bad request"}`)),
		Header:     http.Header{},
	}
	e := parseErrorResponse(res)
	if e.Message != "bad request" {
		t.Fatalf("want 'bad request', got %s", e.Message)
	}
}

func TestParseErrorResponse_FallsBackToStatusText(t *testing.T) {
	res := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader(`{"code":123}`)),
		Header:     http.Header{},
	}
	e := parseErrorResponse(res)
	if e.Message != "Internal Server Error" {
		t.Fatalf("want status text fallback, got %s", e.Message)
	}
}

func TestParseErrorResponse_NonJSONBody(t *testing.T) {
	res := &http.Response{
		StatusCode: 502,
		Body:       io.NopCloser(strings.NewReader("plain text")),
		Header:     http.Header{},
	}
	e := parseErrorResponse(res)
	if e.Message != "Bad Gateway" {
		t.Fatalf("want 'Bad Gateway', got %s", e.Message)
	}
	if e.Details != nil {
		t.Fatal("details should be nil for non-JSON body")
	}
}

func TestParseErrorResponse_5xxRetryable(t *testing.T) {
	tests := []struct {
		status    int
		retryable bool
	}{
		{400, false},
		{403, false},
		{500, true},
		{503, true},
	}
	for _, tt := range tests {
		res := &http.Response{
			StatusCode: tt.status,
			Body:       io.NopCloser(strings.NewReader(`{"error":"test"}`)),
			Header:     http.Header{},
		}
		e := parseErrorResponse(res)
		if e.Retryable != tt.retryable {
			t.Errorf("status %d: want retryable=%v, got %v", tt.status, tt.retryable, e.Retryable)
		}
	}
}

func TestNetworkError(t *testing.T) {
	e := networkError(errors.New("connection refused"))
	if e.Code != "NETWORK_ERROR" {
		t.Fatalf("want NETWORK_ERROR, got %s", e.Code)
	}
	if e.Message != "connection refused" {
		t.Fatalf("want 'connection refused', got %s", e.Message)
	}
	if !e.Retryable {
		t.Fatal("network errors should be retryable")
	}
	if e.Status != 0 {
		t.Fatalf("want status 0, got %d", e.Status)
	}
}

func TestError_ErrorMethod(t *testing.T) {
	e := &Error{Status: 404, Code: "HTTP_404", Message: "not found"}
	got := e.Error()
	if !strings.Contains(got, "HTTP_404") || !strings.Contains(got, "not found") {
		t.Fatalf("unexpected Error() output: %s", got)
	}

	e2 := &Error{Status: 0, Code: "NETWORK_ERROR", Message: "timeout"}
	got2 := e2.Error()
	if !strings.Contains(got2, "NETWORK_ERROR") {
		t.Fatalf("unexpected Error() output: %s", got2)
	}
}

func TestIsRetryable(t *testing.T) {
	if !IsRetryable(&Error{Retryable: true}) {
		t.Fatal("want true for retryable error")
	}
	if IsRetryable(&Error{Retryable: false}) {
		t.Fatal("want false for non-retryable error")
	}
	if IsRetryable(errors.New("plain error")) {
		t.Fatal("want false for non-wavehouse error")
	}
}

func TestErrorsAs(t *testing.T) {
	err := error(&Error{Status: 403, Code: "HTTP_403", Message: "forbidden"})
	var e *Error
	if !errors.As(err, &e) {
		t.Fatal("errors.As should find *Error")
	}
	if e.Status != 403 {
		t.Fatalf("want 403, got %d", e.Status)
	}
}
