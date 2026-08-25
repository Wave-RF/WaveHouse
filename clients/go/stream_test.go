package wavehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sseSender opens w as a flushed text/event-stream response and returns a
// frame writer — the preamble every SSE handler in this file starts with. The
// writer reports false once the client has gone away.
func sseSender(t *testing.T, w http.ResponseWriter) func(frame string) bool {
	fl, ok := w.(http.Flusher)
	if !ok {
		t.Error("response writer is not a flusher")
		return func(string) bool { return false }
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl.Flush()
	return func(frame string) bool {
		_, err := io.WriteString(w, frame)
		fl.Flush()
		return err == nil
	}
}

// sseServer serves the given SSE frames on any request, then holds the
// connection open until the client disconnects.
func sseServer(t *testing.T, frames []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		send := sseSender(t, w)
		for _, f := range frames {
			send(f)
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func sseFrame(ts, page string) string {
	return "id: " + ts + "\n" +
		`data: {"table_name":"clicks","received_timestamp":"` + ts + `","data":{"page":"` + page + `"}}` +
		"\n\n"
}

func streamClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	return NewClient(Config{BaseURL: srv.URL, HTTPClient: srv.Client()})
}

// firstAPIError subscribes to sc and returns the first error it reports,
// failing the test if none arrives or it isn't a typed *Error.
func firstAPIError(t *testing.T, sc *StreamController) *Error {
	t.Helper()
	errCh := make(chan error, 4)
	sc.Subscribe(&StreamSubscriber{Error: func(err error) { errCh <- err }})
	select {
	case err := <-errCh:
		var apiErr *Error
		if !errors.As(err, &apiErr) {
			t.Fatalf("want *Error, got %T: %v", err, err)
		}
		return apiErr
	case <-time.After(5 * time.Second):
		t.Fatal("error never surfaced")
		return nil
	}
}

func recvEvent(t *testing.T, ch <-chan StreamEvent) StreamEvent {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream event")
		return StreamEvent{}
	}
}

func TestStream_SubscribeReceivesEvents(t *testing.T) {
	srv := sseServer(t, []string{
		sseFrame("2026-01-01T00:00:01Z", "/home"),
		sseFrame("2026-01-01T00:00:02Z", "/about"),
	})
	stream := streamClient(t, srv).From("clicks").Stream(nil)
	defer stream.Close()

	got := make(chan StreamEvent, 8)
	stream.Subscribe(&StreamSubscriber{
		Next: func(e StreamEvent) { got <- e },
	})

	e1 := recvEvent(t, got)
	if e1.Table != "clicks" || e1.Data["page"] != "/home" {
		t.Fatalf("unexpected first event: %+v", e1)
	}
	e2 := recvEvent(t, got)
	if e2.Data["page"] != "/about" || e2.Timestamp != "2026-01-01T00:00:02Z" {
		t.Fatalf("unexpected second event: %+v", e2)
	}
}

func TestStream_EventsChannel(t *testing.T) {
	srv := sseServer(t, []string{sseFrame("2026-01-01T00:00:01Z", "/home")})
	stream := streamClient(t, srv).From("clicks").Stream(nil)

	ch := stream.Events()
	e := recvEvent(t, ch)
	if e.Data["page"] != "/home" {
		t.Fatalf("unexpected event: %+v", e)
	}

	stream.Close()
	select {
	case _, open := <-ch:
		if open {
			// A buffered event may arrive before close; drain once more.
			if _, open2 := <-ch; open2 {
				t.Fatal("events channel not closed after Close")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("events channel never closed after Close")
	}
}

func TestStream_Connected(t *testing.T) {
	srv := sseServer(t, nil)
	stream := streamClient(t, srv).From("clicks").Stream(nil)
	defer stream.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.Connected(ctx); err != nil {
		t.Fatalf("Connected: %v", err)
	}
	if s := stream.Status(); s != StatusLive {
		t.Fatalf("want live, got %s", s)
	}
}

func TestStream_FilteredDeliversMatchesAndProjects(t *testing.T) {
	srv := sseServer(t, []string{
		sseFrame("2026-01-01T00:00:01Z", "/home"),
		sseFrame("2026-01-01T00:00:02Z", "/miss"),
		sseFrame("2026-01-01T00:00:03Z", "/home"),
	})
	stream := streamClient(t, srv).From("clicks").
		Select("page").
		Where("page", OpEq, "/home").
		Stream(nil)
	defer stream.Close()

	got := make(chan StreamEvent, 8)
	stream.Subscribe(&StreamSubscriber{Next: func(e StreamEvent) { got <- e }})

	for range 2 {
		e := recvEvent(t, got)
		if e.Data["page"] != "/home" {
			t.Fatalf("filter leaked event: %+v", e)
		}
		if len(e.Data) != 1 {
			t.Fatalf("projection kept extra columns: %+v", e.Data)
		}
	}
	select {
	case e := <-got:
		t.Fatalf("unexpected third event: %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestStream_FilteredCloseUnderLoad exercises the wrapper-close path while the
// inner stream is still delivering — the send-on-closed-channel regression.
func TestStream_FilteredCloseUnderLoad(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		send := sseSender(t, w)
		for i := 0; ; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			if !send(sseFrame(fmt.Sprintf("2026-01-01T00:00:%02dZ", i%60), "/home")) {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	stream := streamClient(t, srv).From("clicks").
		Select().
		Where("page", OpEq, "/home").
		Stream(nil)

	var n atomic.Int64
	stream.Subscribe(&StreamSubscriber{Next: func(StreamEvent) { n.Add(1) }})
	// Also exercise the Events() channel feed path during close.
	_ = stream.Events()

	deadline := time.Now().Add(5 * time.Second)
	for n.Load() < 10 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n.Load() == 0 {
		t.Fatal("no events delivered before close")
	}
	stream.Close() // must not panic or race with in-flight emits

	select {
	case <-stream.done:
	case <-time.After(5 * time.Second):
		t.Fatal("filtered stream goroutine never exited")
	}
}

// TestStream_EventsBufferBeforeFirstEventsCall pins TS parity: the channel
// buffers from construction, so events emitted before the first Events() call
// are still delivered once the consumer starts reading.
func TestStream_EventsBufferBeforeFirstEventsCall(t *testing.T) {
	sc := &StreamController{eventCh: make(chan StreamEvent, 256)}
	sc.emitEvent(StreamEvent{Table: "clicks", Data: map[string]any{"page": "/"}})

	select {
	case e := <-sc.Events():
		if e.Table != "clicks" {
			t.Fatalf("want event for clicks, got %+v", e)
		}
	default:
		t.Fatal("event emitted before Events() was not buffered")
	}
}

// ---------------------------------------------------------------------------
// Client-side filter engine
// ---------------------------------------------------------------------------

// TestEvaluateFilter covers the operator semantics the client-side filter
// engine has to reproduce. The timestamp block exists because the server
// canonicalizes every top-level DateTime value to RFC 3339 UTC before
// publishing (#402), so a payload and a caller's filter constant routinely
// spell the same instant differently; comparing those as text disagrees with
// the server's row filter, which compares DateTime columns chronologically.
func TestEvaluateFilter(t *testing.T) {
	// The canonicalized payload value, and the same instant in +02:00 — which
	// sorts ABOVE it lexically ("06" > "04") while being chronologically equal.
	const canonical = "2026-06-21T04:00:00Z"
	const sameInstantOffset = "2026-06-21T06:00:00+02:00"
	const oneSecondLater = "2026-06-21T06:00:01+02:00"

	tests := []struct {
		name     string
		actual   any
		op       string
		expected any
		want     bool
	}{
		{"EqNumericCrossType", float64(10), "eq", 10, true},
		{"EqString", "a", "eq", "a", true},
		{"EqMismatch", "a", "eq", "b", false},
		{"Neq", "a", "neq", "b", true},
		{"GtTrue", float64(11), "gt", 10, true},
		{"GtFalse", float64(10), "gt", 10, false},
		{"Gte", float64(10), "gte", 10, true},
		{"Lt", float64(9), "lt", 10, true},
		{"LteString", "a", "lte", "b", true},
		{"GtIncomparable", "a", "gt", 10, false},
		// Narrow/unsigned codegen-struct fields must compare, not silently drop.
		{"GtUnsignedOperand", float64(10), "gt", uint32(5), true},
		{"InAnySlice", "b", "in", []any{"a", "b"}, true},
		{"InTypedSlice", float64(2), "in", []int{1, 2}, true},
		{"InMiss", "c", "in", []any{"a", "b"}, false},
		{"InNotASlice", "a", "in", "a", false},
		{"Like", "hello world", "like", "hello%", true},
		{"LikeCaseInsensitive", "HELLO", "like", "hello", true},
		{"LikeUnderscore", "cat", "like", "c_t", true},
		{"LikeAnchored", "xhello", "like", "hello%", false},
		{"NotLike", "abc", "not_like", "x%", true},
		{"LikeNonString", 5, "like", "5", false},
		{"UnknownOp", "a", "regex", "a", false},

		// Timestamps compare as instants, not as text.
		{"TSEqualAcrossOffsets", canonical, "eq", sameInstantOffset, true},
		{"TSNeqFalseAcrossOffsets", canonical, "neq", sameInstantOffset, false},
		{"TSGteAtSameInstant", canonical, "gte", sameInstantOffset, true},
		{"TSLteAtSameInstant", canonical, "lte", sameInstantOffset, true},
		{"TSGtFalseAtSameInstant", canonical, "gt", sameInstantOffset, false},
		{"TSLtSeesLaterOffsetInstant", canonical, "lt", oneSecondLater, true},
		{"TSGtFalseAgainstLaterInstant", canonical, "gt", oneSecondLater, false},
		{"TSInMatchesAcrossOffsets", canonical, "in", []any{"2020-01-01T00:00:00Z", sameInstantOffset}, true},

		// Same-offset spellings must keep working exactly as before.
		{"TSGtWithinUTC", "2026-06-21T04:00:01Z", "gt", canonical, true},
		{"TSLtWithinUTC", "2026-06-21T03:59:59Z", "lt", canonical, true},
		{"TSEqIdenticalText", canonical, "eq", canonical, true},
		{"TSFractionalSecondsOrder", "2026-06-21T04:00:00.500Z", "gt", canonical, true},

		// A zone-less constant names an instant only relative to the column's
		// declared timezone, which a stream subscriber does not have. It must
		// not be silently read as UTC — ordering fails closed.
		{"TSZonelessFailsClosedOnGt", canonical, "gt", "2026-06-21 03:00:00", false},
		{"TSZonelessFailsClosedOnLt", canonical, "lt", "2026-06-21 05:00:00", false},
		// A ',' fraction is ISO 8601 but not ClickHouse, so it is not an instant.
		{"TSCommaFractionIsNotAnInstant", canonical, "eq", "2026-06-21T04:00:00,000Z", false},

		// nil only equals nil: a column missing from the payload must not match
		// the literal string "<nil>" through equalValues' fmt.Sprint fallback.
		{"NilEqualsNil", nil, "eq", nil, true},
		{"NilIsNotTheStringNil", nil, "eq", "<nil>", false},
		{"TheStringNilIsNotNil", "<nil>", "eq", nil, false},
		{"NilIsNotEmptyString", nil, "eq", "", false},
		{"NilIsNotZero", nil, "eq", 0, false},

		// Non-timestamp strings keep lexicographic ordering; numbers are untouched.
		{"PlainStringsOrderLexically", "banana", "gt", "apple", true},
		{"PlainStringsCompareEqual", "apple", "eq", "apple", true},
		{"NumbersOrderNumerically", 100.0, "gt", 9.0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cf := compileFilters([]QueryFilter{{Column: "c", Op: tt.op, Value: tt.expected}})[0]
			if got := evaluateFilter(tt.actual, tt.op, tt.expected, cf.re); got != tt.want {
				t.Errorf("evaluateFilter(%v, %q, %v) = %v, want %v", tt.actual, tt.op, tt.expected, got, tt.want)
			}
		})
	}
}

func TestMatchesFilters_AllMustMatch(t *testing.T) {
	row := map[string]any{"page": "/home", "score": float64(10)}
	both := []QueryFilter{
		{Column: "page", Op: "eq", Value: "/home"},
		{Column: "score", Op: "gt", Value: 5},
	}
	if !matchesFilters(row, compileFilters(both)) {
		t.Fatal("want match when every filter passes")
	}
	oneFails := append(append([]QueryFilter(nil), both...), QueryFilter{Column: "score", Op: "gt", Value: 99})
	if matchesFilters(row, compileFilters(oneFails)) {
		t.Fatal("want no match when any filter fails")
	}
	if !matchesFilters(row, nil) {
		t.Fatal("want match with no filters")
	}
}

func TestToFloat64(t *testing.T) {
	for _, v := range []any{
		float64(1), float32(1),
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		json.Number("1"),
	} {
		if f, ok := toFloat64(v); !ok || f != 1 {
			t.Fatalf("toFloat64(%T) = (%v, %v)", v, f, ok)
		}
	}
	if _, ok := toFloat64("1"); ok {
		t.Fatal("strings must not convert")
	}
}

func TestProjectColumns(t *testing.T) {
	row := map[string]any{"a": 1, "b": 2, "c": 3}
	got := projectColumns(row, []string{"a", "c", "missing"})
	if len(got) != 2 || got["a"] != 1 || got["c"] != 3 {
		t.Fatalf("unexpected projection: %+v", got)
	}
}

// A connect that fails before Subscribe returns still has to reach the
// subscriber; without the replay in Subscribe this is a scheduling race that
// only shows up on a loaded machine.
func TestStream_SubscribeReplaysAMissedTerminalError(t *testing.T) {
	stream := NewClient(Config{BaseURL: "ws://example.invalid"}).From("clicks").Stream(nil)
	defer stream.Close()
	<-stream.done // the connect has already failed and emitted

	errCh := make(chan error, 1)
	stream.Subscribe(&StreamSubscriber{Error: func(err error) { errCh <- err }})
	select {
	case err := <-errCh:
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.Code != "SSE_CONNECT_ERROR" {
			t.Fatalf("want SSE_CONNECT_ERROR, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a subscriber registered after the failure never saw it")
	}
}

// TestStream_TerminalConnectFailures: every way a connection can fail in a way
// reconnecting cannot fix. Each case must surface a specific, non-retryable
// code, close the stream, and leave Connected failing — the generic retryable
// SSE_ERROR would spin here instead.
func TestStream_TerminalConnectFailures(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.HandlerFunc
		baseURL  string // overrides the test server URL when non-empty
		auth     func(context.Context) (string, error)
		wantCode string
	}{
		{
			name: "a 403 the caller's credentials cannot fix",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			},
			wantCode: "HTTP_403",
		},
		{
			name: "200 that is not an event stream",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "<html><body>Please sign in</body></html>")
			},
			wantCode: "SSE_BAD_CONTENT_TYPE",
		},
		{
			name: "200 with no Content-Type at all",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header()["Content-Type"] = nil
				w.WriteHeader(http.StatusOK)
			},
			wantCode: "SSE_BAD_CONTENT_TYPE",
		},
		{
			name: "credentialed request is redirected",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Redirect(w, &http.Request{}, "https://elsewhere.example/v1/stream", http.StatusFound)
			},
			auth:     StaticToken("secret-token"),
			wantCode: "SSE_REDIRECT",
		},
		{
			name:     "baseURL scheme cannot carry SSE",
			handler:  func(http.ResponseWriter, *http.Request) {},
			baseURL:  "ws://example.invalid",
			wantCode: "SSE_CONNECT_ERROR",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			t.Cleanup(srv.Close)

			base := srv.URL
			if tc.baseURL != "" {
				base = tc.baseURL
			}
			client := NewClient(Config{BaseURL: base, Auth: tc.auth, HTTPClient: srv.Client()})

			stream := client.From("clicks").Stream(nil)
			defer stream.Close()

			apiErr := firstAPIError(t, stream)
			if apiErr.Code != tc.wantCode {
				t.Fatalf("want code %s, got %s", tc.wantCode, apiErr)
			}
			if apiErr.Retryable {
				t.Fatalf("%s must not be retryable", apiErr.Code)
			}

			select {
			case <-stream.done:
			case <-time.After(5 * time.Second):
				t.Fatalf("stream never closed after terminal %s", tc.wantCode)
			}
			if s := stream.Status(); s != StatusClosed {
				t.Fatalf("want closed, got %s", s)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := stream.Connected(ctx); err == nil {
				t.Fatal("Connected must fail on a terminally-closed stream")
			}
		})
	}
}

// TestStream_ReconnectResumesFromLastEventID: the gap-fill contract. The
// initial request carries StreamOptions.Since; after the connection drops,
// the reconnect carries ?since=<last seen event id>.
func TestStream_ReconnectResumesFromLastEventID(t *testing.T) {
	var mu sync.Mutex
	var sinceParams []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sinceParams = append(sinceParams, r.URL.Query().Get("since"))
		n := len(sinceParams)
		mu.Unlock()

		send := sseSender(t, w)
		if n == 1 {
			send(sseFrame("2026-01-01T00:00:01Z", "/home"))
			return // server closes → client must reconnect with since=<id>
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	stream := streamClient(t, srv).From("clicks").Stream(&StreamOptions{Since: "seed-id"})
	defer stream.Close()

	got := make(chan StreamEvent, 4)
	stream.Subscribe(&StreamSubscriber{Next: func(e StreamEvent) { got <- e }})
	recvEvent(t, got)

	// Reconnect happens after ~backoff(0) (≈1s with jitter).
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(sinceParams)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream never reconnected")
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if sinceParams[0] != "seed-id" {
		t.Fatalf("initial request: want since=seed-id, got %q", sinceParams[0])
	}
	if sinceParams[1] != "2026-01-01T00:00:01Z" {
		t.Fatalf("reconnect: want since=<last event id>, got %q", sinceParams[1])
	}
}

// The SSE transport builds its URL separately from buildURL (stream.go), so a
// BaseURL path prefix needs its own guard. See TestBaseURLPathPrefixIsPreserved.
func TestStreamBaseURLPathPrefixIsPreserved(t *testing.T) {
	gotPath := make(chan string, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/warehouse/v1/stream", func(w http.ResponseWriter, r *http.Request) {
		select {
		case gotPath <- r.URL.Path:
		default:
		}
		sseSender(t, w)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux) // anything off-prefix 404s
	t.Cleanup(srv.Close)

	client := NewClient(Config{
		BaseURL:    srv.URL + "/api/warehouse",
		Options:    &ClientOptions{},
		HTTPClient: srv.Client(),
	})
	sc := client.From("clicks").Stream(nil)
	t.Cleanup(sc.Close)

	select {
	case p := <-gotPath:
		if p != "/api/warehouse/v1/stream" {
			t.Fatalf("want prefixed stream path, got %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream never reached the prefixed path")
	}
}

// TestStream_RedirectFollowedWhenUncredentialed: the refusal is scoped to
// requests carrying a credential. Without one there is nothing to leak or
// silently downgrade, so the redirect is followed as usual.
func TestStream_RedirectFollowedWhenUncredentialed(t *testing.T) {
	target := sseServer(t, []string{sseFrame("2026-01-01T00:00:01Z", "/home")})

	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/v1/stream?table=clicks", http.StatusFound)
	}))
	t.Cleanup(front.Close)

	stream := streamClient(t, front).From("clicks").Stream(nil)
	defer stream.Close()

	events := make(chan StreamEvent, 4)
	stream.Subscribe(&StreamSubscriber{Next: func(e StreamEvent) { events <- e }})

	select {
	case e := <-events:
		if e.Table != "clicks" {
			t.Fatalf("want table clicks, got %s", e.Table)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("redirect was not followed for an uncredentialed stream")
	}
}

// TestStream_MalformedFrameIsTypedAndRetryable: a bad frame must emit no
// event, and must arrive as an *Error so errors.As and IsRetryable work on it
// — a bare fmt.Errorf would leave callers string-matching.
func TestStream_MalformedFrameIsTypedAndRetryable(t *testing.T) {
	srv := sseServer(t, []string{"id: 1\ndata: {not json\n\n"})
	stream := streamClient(t, srv).From("clicks").Stream(nil)
	defer stream.Close()

	errCh := make(chan error, 4)
	events := make(chan StreamEvent, 4)
	stream.Subscribe(&StreamSubscriber{
		Next:  func(e StreamEvent) { events <- e },
		Error: func(err error) { errCh <- err },
	})

	select {
	case err := <-errCh:
		var apiErr *Error
		if !errors.As(err, &apiErr) {
			t.Fatalf("want *Error, got %T: %v", err, err)
		}
		if apiErr.Code != "SSE_PARSE_ERROR" || !strings.Contains(apiErr.Message, "malformed SSE message") {
			t.Fatalf("want a malformed-SSE parse error, got %s: %v", apiErr.Code, err)
		}
		if !IsRetryable(err) {
			t.Fatal("a malformed frame must stay retryable")
		}
		if strings.Contains(apiErr.Message, "not json") {
			t.Fatal("payload must not be echoed into the error message")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("error never surfaced")
	}
	// The error is emitted from the same handleSSEData call that would have
	// emitted an event, so by now a leaked event would already be queued.
	select {
	case e := <-events:
		t.Fatalf("malformed data emitted an event: %+v", e)
	default:
	}
}

// TestStream_ConfiguredHeadersReachTheStream: ClientOptions.Headers apply to
// SSE, not just REST — and the SDK's own headers still win a collision.
func TestStream_ConfiguredHeadersReachTheStream(t *testing.T) {
	seen := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- r.Header.Clone():
		default:
		}
		sseSender(t, w)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	client := NewClient(Config{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Options: &ClientOptions{Headers: map[string]string{
			"X-Operator-Key": "op-secret",
			"accept":         "application/json", // must lose to the SDK's own Accept
		}},
	})
	stream := client.From("clicks").Stream(nil)
	defer stream.Close()

	select {
	case h := <-seen:
		if got := h.Get("X-Operator-Key"); got != "op-secret" {
			t.Fatalf("want configured header on the stream request, got %q", got)
		}
		if got := h.Get("Accept"); got != "text/event-stream" {
			t.Fatalf("SDK Accept must win, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream request never arrived")
	}
}

// TestStream_FilterMatchesCanonicalizedPayload: the end-to-end shape of the
// instant-comparison bug — a caller filters on a non-UTC spelling and the
// server delivers the canonicalized one.
func TestStream_FilterMatchesCanonicalizedPayload(t *testing.T) {
	frame := `event: message
id: 2026-06-21T04:00:00Z
data: {"table_name":"clicks","received_timestamp":"2026-06-21T04:00:00Z","data":{"page":"/home","event_ts":"2026-06-21T04:00:00Z"}}

`
	srv := sseServer(t, []string{frame})

	stream := streamClient(t, srv).From("clicks").
		SelectAll().
		Where("event_ts", OpGte, "2026-06-21T06:00:00+02:00").
		Stream(nil)
	defer stream.Close()

	events := make(chan StreamEvent, 4)
	stream.Subscribe(&StreamSubscriber{Next: func(e StreamEvent) { events <- e }})

	select {
	case e := <-events:
		if e.Data["page"] != "/home" {
			t.Fatalf("unexpected row: %v", e.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a row chronologically equal to the filter constant was withheld")
	}
}
