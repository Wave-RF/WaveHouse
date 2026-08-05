package wavehouse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

// StreamController manages a live SSE event stream. Use Subscribe for
// callback-based consumption or Events for channel-based consumption.
type StreamController struct {
	mu          sync.Mutex
	status      StreamStatus
	subscribers []*StreamSubscriber
	eventCh     chan StreamEvent // ponytail: single buffered channel for Go-native consumption
	cancel      context.CancelFunc
	done        chan struct{}
	closed      bool
}

// newStreamController opens an SSE connection for the given table.
func newStreamController(hctx httpContext, table string, opts *StreamOptions) *StreamController {
	ctx, cancel := context.WithCancel(context.Background())
	sc := &StreamController{
		status:  StatusConnecting,
		eventCh: make(chan StreamEvent, 256),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go sc.run(ctx, hctx, table, opts)
	return sc
}

// Status returns the current connection status.
func (sc *StreamController) Status() StreamStatus {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.status
}

// Subscribe registers callbacks for stream events. Returns an unsubscribe
// function. The subscriber's Status callback fires immediately with the
// current status.
func (sc *StreamController) Subscribe(sub *StreamSubscriber) func() {
	sc.mu.Lock()
	sc.subscribers = append(sc.subscribers, sub)
	currentStatus := sc.status
	sc.mu.Unlock()

	// Benign race: setStatus also calls the subscriber, so a stale
	// status here is immediately followed by the correct one.
	if sub.Status != nil {
		sub.Status(currentStatus)
	}

	return func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		for i, s := range sc.subscribers {
			if s == sub {
				sc.subscribers = append(sc.subscribers[:i], sc.subscribers[i+1:]...)
				break
			}
		}
	}
}

// Events returns a read-only channel that receives stream events.
// The channel is closed when the stream closes.
func (sc *StreamController) Events() <-chan StreamEvent {
	return sc.eventCh
}

// Connected blocks until the stream reaches "live" status or the context
// expires. Returns an error if the stream closes before connecting.
func (sc *StreamController) Connected(ctx context.Context) error {
	sc.mu.Lock()
	if sc.status == StatusLive {
		sc.mu.Unlock()
		return nil
	}
	if sc.status == StatusClosed || sc.closed {
		sc.mu.Unlock()
		return fmt.Errorf("stream is closed")
	}
	sc.mu.Unlock()

	// Poll — simple and correct.
	// ponytail: condition variable if polling shows up in profiles.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sc.done:
			return fmt.Errorf("stream closed before connecting")
		case <-ticker.C:
			sc.mu.Lock()
			s := sc.status
			sc.mu.Unlock()
			if s == StatusLive {
				return nil
			}
			if s == StatusClosed {
				return fmt.Errorf("stream closed before connecting")
			}
		}
	}
}

// Close shuts down the stream and releases resources. Non-blocking so it is
// safe to call from subscriber callbacks (which run on the stream goroutine).
func (sc *StreamController) Close() {
	sc.mu.Lock()
	if sc.closed {
		sc.mu.Unlock()
		return
	}
	sc.closed = true
	sc.mu.Unlock()

	sc.cancel()
	// Don't block on <-sc.done: callbacks execute on the stream goroutine,
	// so waiting here would deadlock if Close is called from a callback.
}

func (sc *StreamController) setStatus(s StreamStatus) {
	sc.mu.Lock()
	if s == sc.status {
		sc.mu.Unlock()
		return
	}
	sc.status = s
	subs := append([]*StreamSubscriber(nil), sc.subscribers...)
	sc.mu.Unlock()

	for _, sub := range subs {
		if sub.Status != nil {
			sub.Status(s)
		}
	}
}

func (sc *StreamController) emitEvent(event StreamEvent) {
	sc.mu.Lock()
	subs := append([]*StreamSubscriber(nil), sc.subscribers...)
	sc.mu.Unlock()

	for _, sub := range subs {
		if sub.Next != nil {
			sub.Next(event)
		}
	}

	// Non-blocking send to the channel.
	select {
	case sc.eventCh <- event:
	default:
		log.Printf("[wavehouse] stream event dropped: channel buffer full")
	}
}

func (sc *StreamController) emitError(err error) {
	sc.mu.Lock()
	subs := append([]*StreamSubscriber(nil), sc.subscribers...)
	sc.mu.Unlock()

	for _, sub := range subs {
		if sub.Error != nil {
			sub.Error(err)
		}
	}
}

// run is the SSE connection loop with reconnect/backoff.
func (sc *StreamController) run(ctx context.Context, hctx httpContext, table string, opts *StreamOptions) {
	defer func() {
		sc.setStatus(StatusClosed)
		close(sc.eventCh)
		close(sc.done)
	}()

	since := ""
	if opts != nil {
		since = opts.Since
	}

	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}

		lastID, err := sc.connect(ctx, hctx, table, since)
		// Persist the last event ID so the next reconnect resumes from it.
		if lastID != "" {
			since = lastID
		}
		if ctx.Err() != nil {
			return
		}

		if err != nil {
			sc.emitError(&Error{
				Status:    0,
				Code:      "SSE_ERROR",
				Message:   err.Error(),
				Retryable: true,
			})
		}

		sc.setStatus(StatusReconnecting)
		delay := backoff(attempt)
		attempt++

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// connect opens a single SSE connection and reads events until it closes.
// Returns the last seen event ID (empty if none) and any error.
func (sc *StreamController) connect(ctx context.Context, hctx httpContext, table, since string) (string, error) {
	u, err := url.Parse(hctx.baseURL + "/v1/stream")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("table", table)
	if since != "" {
		q.Set("since", since)
	}

	// Auth: Go SDK uses Authorization header (not ?token= like browser EventSource).
	var authHeader string
	if hctx.auth != nil {
		token, err := hctx.auth(ctx)
		if err != nil {
			return "", fmt.Errorf("auth: %w", err)
		}
		if token != "" {
			authHeader = "Bearer " + token
		}
	}

	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := hctx.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("SSE connect failed: HTTP %d", resp.StatusCode)
	}

	sc.setStatus(StatusLive)

	// Parse SSE frames.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // 16 MiB max, matching server ingest cap
	var eventID, dataLine string
	lastID := since

	for scanner.Scan() {
		if ctx.Err() != nil {
			return lastID, nil
		}

		line := scanner.Text()

		if line == "" {
			// Empty line = end of event frame.
			if dataLine != "" {
				sc.handleSSEData(dataLine, eventID)
				// Track last event ID for reconnect gap-fill.
				if eventID != "" {
					lastID = eventID
				}
			}
			eventID = ""
			dataLine = ""
			continue
		}

		if strings.HasPrefix(line, ":") {
			// Comment (keepalive or connected). Skip.
			continue
		}

		if strings.HasPrefix(line, "id:") {
			eventID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		} else if strings.HasPrefix(line, "data:") {
			trimmed := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if dataLine == "" {
				dataLine = trimmed
			} else {
				dataLine = dataLine + "\n" + trimmed
			}
		}
	}

	return lastID, scanner.Err()
}

// sseMessage matches the server's SSE event JSON shape.
type sseMessage struct {
	TableName         string         `json:"table_name"`
	ReceivedTimestamp string         `json:"received_timestamp"`
	Data              map[string]any `json:"data"`
}

func (sc *StreamController) handleSSEData(data, eventID string) {
	var msg sseMessage
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		log.Printf("[wavehouse] SSE received malformed message: %s", data)
		return
	}

	event := StreamEvent{
		Table:     msg.TableName,
		Timestamp: msg.ReceivedTimestamp,
		Data:      msg.Data,
	}
	sc.emitEvent(event)
}

// newFilteredStreamController wraps a StreamController with client-side
// filtering and column projection.
func newFilteredStreamController(inner *StreamController, filters []QueryFilter, columns []string) *StreamController {
	ctx, cancel := context.WithCancel(context.Background())
	sc := &StreamController{
		status:  inner.Status(),
		eventCh: make(chan StreamEvent, 256),
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	go func() {
		defer func() {
			sc.setStatus(StatusClosed)
			close(sc.eventCh)
			close(sc.done)
		}()

		inner.Subscribe(&StreamSubscriber{
			Next: func(event StreamEvent) {
				if !matchesFilters(event.Data, filters) {
					return
				}
				if len(columns) > 0 {
					event.Data = projectColumns(event.Data, columns)
				}
				sc.emitEvent(event)
			},
			Status: func(s StreamStatus) {
				sc.setStatus(s)
			},
			Error: func(err error) {
				sc.emitError(err)
			},
		})

		select {
		case <-ctx.Done():
			inner.Close()
		case <-inner.done:
		}
	}()

	return sc
}

// matchesFilters evaluates all filters against a data row (AND).
func matchesFilters(row map[string]any, filters []QueryFilter) bool {
	for _, f := range filters {
		val := row[f.Column]
		if !evaluateFilter(val, f.Op, f.Value) {
			return false
		}
	}
	return true
}

func evaluateFilter(actual any, op string, expected any) bool {
	switch op {
	case "eq":
		return equalValues(actual, expected)
	case "neq":
		return !equalValues(actual, expected)
	case "gt":
		c, ok := compareOrdered(actual, expected)
		return ok && c > 0
	case "gte":
		c, ok := compareOrdered(actual, expected)
		return ok && c >= 0
	case "lt":
		c, ok := compareOrdered(actual, expected)
		return ok && c < 0
	case "lte":
		c, ok := compareOrdered(actual, expected)
		return ok && c <= 0
	case "in":
		return evaluateIn(actual, expected)
	case "like", "not_like":
		aStr, aOK := actual.(string)
		eStr, eOK := expected.(string)
		if !aOK || !eOK {
			return false
		}
		return (op == "like") == matchLike(aStr, eStr)
	default:
		return false
	}
}

// equalValues compares two values for equality, normalizing numeric types
// (JSON decodes numbers as float64, but callers may pass int).
func equalValues(a, b any) bool {
	if af, aOK := toFloat64(a); aOK {
		if bf, bOK := toFloat64(b); bOK {
			return af == bf
		}
	}
	// fmt.Sprint is safe for all types (no panic on maps/slices).
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// evaluateIn checks whether actual is contained in the expected slice.
// Handles both []any and typed slices (e.g., []string, []int).
func evaluateIn(actual, expected any) bool {
	if arr, ok := expected.([]any); ok {
		for _, v := range arr {
			if equalValues(actual, v) {
				return true
			}
		}
		return false
	}
	// Handle typed slices via reflection.
	rv := reflect.ValueOf(expected)
	if rv.Kind() == reflect.Slice {
		for i := range rv.Len() {
			if equalValues(actual, rv.Index(i).Interface()) {
				return true
			}
		}
	}
	return false
}

var likeRegexCache sync.Map // pattern string → *regexp.Regexp

// matchLike converts a SQL LIKE pattern to a regex and tests it
// (case-insensitive, matching the TS SDK).
func matchLike(actual, pattern string) bool {
	if cached, ok := likeRegexCache.Load(pattern); ok {
		return cached.(*regexp.Regexp).MatchString(actual)
	}
	escaped := regexp.QuoteMeta(pattern)
	escaped = strings.ReplaceAll(escaped, "%", ".*")
	escaped = strings.ReplaceAll(escaped, "_", ".")
	re, err := regexp.Compile("(?i)^" + escaped + "$")
	if err != nil {
		return false
	}
	likeRegexCache.Store(pattern, re)
	return re.MatchString(actual)
}

// compareOrdered returns (-1, 0, or 1) and true for comparable ordered types,
// or (0, false) when the types cannot be compared.
func compareOrdered(actual, expected any) (int, bool) {
	if a, aOK := toFloat64(actual); aOK {
		if b, bOK := toFloat64(expected); bOK {
			switch {
			case a < b:
				return -1, true
			case a > b:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	if aStr, ok := actual.(string); ok {
		if bStr, ok := expected.(string); ok {
			switch {
			case aStr < bStr:
				return -1, true
			case aStr > bStr:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func projectColumns(row map[string]any, columns []string) map[string]any {
	result := make(map[string]any, len(columns))
	for _, col := range columns {
		if v, ok := row[col]; ok {
			result[col] = v
		}
	}
	return result
}
