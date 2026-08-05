package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// httpContext carries per-client state needed by every request.
type httpContext struct {
	baseURL    string
	auth       func(ctx context.Context) (string, error)
	maxRetries int
	httpClient *http.Client
}

// requestOptions describes a single HTTP request.
type requestOptions struct {
	method      string
	path        string
	body        any    // JSON-serialized if non-nil
	rawBody     string // sent verbatim if non-empty (takes precedence over body)
	contentType string // overrides Content-Type (default "application/json")
	params      url.Values
}

// doRequest is the internal fetch wrapper with auth, retry, and backoff.
// It decodes the response body into dst (unless dst is nil).
func doRequest(hctx httpContext, ctx context.Context, opts requestOptions, dst any) error {
	reqURL := buildURL(hctx.baseURL, opts.path, opts.params)
	ct := opts.contentType
	if ct == "" {
		ct = "application/json"
	}

	// Serialize body once so every retry sends identical bytes.
	var bodyBytes []byte
	if opts.rawBody != "" {
		bodyBytes = []byte(opts.rawBody)
	} else if opts.body != nil {
		var err error
		bodyBytes, err = json.Marshal(opts.body)
		if err != nil {
			return fmt.Errorf("wavehouse: marshal request body: %w", err)
		}
	}

	// Resolve auth once per request (not per attempt).
	var authHeader string
	if hctx.auth != nil {
		token, err := hctx.auth(ctx)
		if err != nil {
			return fmt.Errorf("wavehouse: auth provider: %w", err)
		}
		if token != "" {
			authHeader = "Bearer " + token
		}
	}

	var lastErr error
	maxAttempts := hctx.maxRetries + 1

	// Retries all methods including POST. For /v1/ingest, at-least-once delivery
	// is the documented contract; dedup is the server-side safety net.
	for attempt := range maxAttempts {
		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(ctx, opts.method, reqURL, bodyReader)
		if err != nil {
			return fmt.Errorf("wavehouse: build request: %w", err)
		}
		req.Header.Set("Content-Type", ct)
		req.Header.Set("Accept", "application/json")
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}

		res, err := hctx.httpClient.Do(req)
		if err != nil {
			// Context cancellation — return immediately, no retry.
			if ctx.Err() != nil {
				return &Error{
					Status:    0,
					Code:      "ABORTED",
					Message:   "Request aborted",
					Retryable: false,
				}
			}
			lastErr = networkError(err)
			if attempt < maxAttempts-1 {
				if sleepErr := sleepWithContext(ctx, backoff(attempt)); sleepErr != nil {
					return &Error{Status: 0, Code: "ABORTED", Message: "Request aborted", Retryable: false}
				}
			}
			continue
		}

		if res.StatusCode >= 200 && res.StatusCode < 300 {
			defer res.Body.Close()
			if dst == nil {
				_, _ = io.Copy(io.Discard, res.Body)
				return nil
			}
			raw, readErr := io.ReadAll(res.Body)
			if readErr != nil {
				return networkError(readErr)
			}
			if len(raw) == 0 {
				return nil
			}
			if err := json.Unmarshal(raw, dst); err != nil {
				return &Error{
					Status:    0,
					Code:      "NETWORK_ERROR",
					Message:   fmt.Errorf("decode response: %w", err).Error(),
					Retryable: false,
				}
			}
			return nil
		}

		apiErr := parseErrorResponse(res)
		res.Body.Close()

		// 503 with Retry-After: wait the specified duration.
		if res.StatusCode == http.StatusServiceUnavailable {
			if ra := res.Header.Get("Retry-After"); ra != "" && attempt < maxAttempts-1 {
				delay := 30 * time.Second
				if secs, parseErr := strconv.Atoi(ra); parseErr == nil && secs > 0 {
					delay = time.Duration(secs) * time.Second
				} else if parsed, parseErr := http.ParseTime(ra); parseErr == nil {
					if d := time.Until(parsed); d > 0 {
						delay = d
					}
				}
				if sleepErr := sleepWithContext(ctx, delay); sleepErr != nil {
					return &Error{Status: 0, Code: "ABORTED", Message: "Request aborted", Retryable: false}
				}
				lastErr = apiErr
				continue
			}
		}

		// Retryable server errors (5xx).
		if apiErr.Retryable && attempt < maxAttempts-1 {
			if sleepErr := sleepWithContext(ctx, backoff(attempt)); sleepErr != nil {
				return &Error{Status: 0, Code: "ABORTED", Message: "Request aborted", Retryable: false}
			}
			lastErr = apiErr
			continue
		}

		return apiErr
	}

	return lastErr
}

func buildURL(base, path string, params url.Values) string {
	u := base + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	return u
}

func backoff(attempt int) time.Duration {
	ms := 1000 * math.Pow(2, float64(attempt))
	if ms > 30000 {
		ms = 30000
	}
	return time.Duration(ms) * time.Millisecond
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// errIs checks if err wraps a *Error with the given code.
func errIs(err error, code string) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == code
	}
	return false
}
