package wavehouse

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseErrorResponse(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantMsg    string
		wantCode   string
		wantRetry  bool
		nilDetails bool
	}{
		{
			name:     "JSONError",
			status:   404,
			body:     `{"error":"unknown table: foo"}`,
			wantMsg:  "unknown table: foo",
			wantCode: "HTTP_404",
		},
		{
			name:    "MessageField",
			status:  400,
			body:    `{"message":"bad request"}`,
			wantMsg: "bad request",
		},
		{
			name:      "FallsBackToStatusText",
			status:    500,
			body:      `{"code":123}`,
			wantMsg:   "Internal Server Error",
			wantRetry: true,
		},
		{
			name:       "NonJSONBody",
			status:     502,
			body:       "plain text",
			wantMsg:    "Bad Gateway",
			wantRetry:  true,
			nilDetails: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := &http.Response{
				StatusCode: tt.status,
				Body:       io.NopCloser(strings.NewReader(tt.body)),
				Header:     http.Header{},
			}
			e := parseErrorResponse(res)
			if e.Status != tt.status {
				t.Fatalf("want status %d, got %d", tt.status, e.Status)
			}
			if tt.wantCode != "" && e.Code != tt.wantCode {
				t.Fatalf("want code %s, got %s", tt.wantCode, e.Code)
			}
			if e.Message != tt.wantMsg {
				t.Fatalf("want message %q, got %q", tt.wantMsg, e.Message)
			}
			if e.Retryable != tt.wantRetry {
				t.Fatalf("want retryable=%v, got %v", tt.wantRetry, e.Retryable)
			}
			if tt.nilDetails && e.Details != nil {
				t.Fatal("details should be nil")
			}
		})
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
