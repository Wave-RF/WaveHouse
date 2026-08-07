package wavehouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sseServer serves the given SSE frames on any request, then holds the
// connection open until the client disconnects.
func sseServer(t *testing.T, frames []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, ok := w.(http.Flusher)
		if !ok {
			t.Error("response writer is not a flusher")
			return
		}
		w.WriteHeader(200)
		fl.Flush()
		for _, f := range frames {
			_, _ = io.WriteString(w, f)
			fl.Flush()
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
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		w.WriteHeader(200)
		fl.Flush()
		for i := 0; ; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			_, err := io.WriteString(w, sseFrame(fmt.Sprintf("2026-01-01T00:00:%02dZ", i%60), "/home"))
			if err != nil {
				return
			}
			fl.Flush()
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

func TestStream_HandleMalformedSSEData(t *testing.T) {
	// chanRequested must be true or emitEvent skips the channel entirely and
	// the no-emit assertion below would pass vacuously.
	sc := &StreamController{eventCh: make(chan StreamEvent, 1), chanRequested: true}
	sc.handleSSEData("not json", "id1") // must not panic or emit
	select {
	case e := <-sc.eventCh:
		t.Fatalf("malformed data emitted event: %+v", e)
	default:
	}
}

// ---------------------------------------------------------------------------
// Client-side filter engine
// ---------------------------------------------------------------------------

func TestEvaluateFilter(t *testing.T) {
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

func TestCompareOrdered(t *testing.T) {
	if c, ok := compareOrdered(float64(1), 2); !ok || c != -1 {
		t.Fatalf("numeric compare: got (%d, %v)", c, ok)
	}
	if c, ok := compareOrdered("b", "a"); !ok || c != 1 {
		t.Fatalf("string compare: got (%d, %v)", c, ok)
	}
	if _, ok := compareOrdered(map[string]any{}, 1); ok {
		t.Fatal("incomparable types must return ok=false")
	}
}

func TestToFloat64(t *testing.T) {
	for _, v := range []any{float64(1), float32(1), int(1), int64(1)} {
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

// TestStream_NonRetryableConnectErrorIsTerminal: a 403 must close the stream
// (no infinite reconnect) and surface the API error to Error subscribers.
func TestStream_NonRetryableConnectErrorIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)

	stream := streamClient(t, srv).From("clicks").Stream(nil)
	defer stream.Close()

	errCh := make(chan error, 4)
	stream.Subscribe(&StreamSubscriber{Error: func(err error) { errCh <- err }})

	select {
	case err := <-errCh:
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden || apiErr.Retryable {
			t.Fatalf("want non-retryable HTTP_403, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("error never surfaced")
	}

	select {
	case <-stream.done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream never closed after non-retryable connect error")
	}
	if s := stream.Status(); s != StatusClosed {
		t.Fatalf("want closed, got %s", s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.Connected(ctx); err == nil {
		t.Fatal("Connected must fail on a terminally-closed stream")
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

		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		w.WriteHeader(200)
		fl.Flush()
		if n == 1 {
			_, _ = io.WriteString(w, sseFrame("2026-01-01T00:00:01Z", "/home"))
			fl.Flush()
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
