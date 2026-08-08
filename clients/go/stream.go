package wavehouse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	eventCh     chan StreamEvent // single buffered channel for Go-native consumption
	dropLogOnce sync.Once
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
// The channel is closed when the stream closes. Events buffer into it from
// stream construction (matching the TS SDK), so events that arrive before
// the first Events() call are not lost. A Subscribe-only consumer that never
// calls Events() at most fills the 256-slot buffer and trips the one-time
// drop log.
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
	// TODO: switch to a condition variable if polling shows up in profiles.
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
	// StatusClosed is terminal: a filtered wrapper's inner controller can
	// have copied its subscriber slice before unsub, so a stale Status
	// callback may land after Close — it must not resurrect the status.
	if s == sc.status || sc.status == StatusClosed {
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

// snapshotSubs copies the subscriber list under mu so callbacks run unlocked.
// setStatus keeps its own inline copy: there the snapshot must share the
// critical section with the status write to keep callback order consistent.
func (sc *StreamController) snapshotSubs() []*StreamSubscriber {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return append([]*StreamSubscriber(nil), sc.subscribers...)
}

func (sc *StreamController) emitEvent(event StreamEvent) {
	for _, sub := range sc.snapshotSubs() {
		if sub.Next != nil {
			sub.Next(event)
		}
	}

	// Non-blocking send to the channel, which buffers from construction (TS
	// parity) so events emitted before the first Events() call survive.
	// Guarded by mu so the send and closeEventCh serialize — a late event can
	// never hit a closed channel.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed {
		return
	}
	select {
	case sc.eventCh <- event:
	default:
		sc.dropLogOnce.Do(func() {
			log.Printf("[wavehouse] stream event dropped: Events() channel buffer full (further drops not logged)")
		})
	}
}

func (sc *StreamController) emitError(err error) {
	for _, sub := range sc.snapshotSubs() {
		if sub.Error != nil {
			sub.Error(err)
		}
	}
}

// closeEventCh marks the controller closed and closes the events channel.
// Must serialize with emitEvent's send via mu.
func (sc *StreamController) closeEventCh() {
	sc.mu.Lock()
	sc.closed = true
	close(sc.eventCh)
	sc.mu.Unlock()
}

// run is the SSE connection loop with reconnect/backoff.
func (sc *StreamController) run(ctx context.Context, hctx httpContext, table string, opts *StreamOptions) {
	defer func() {
		sc.setStatus(StatusClosed)
		sc.closeEventCh()
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

		lastID, live, err := sc.connect(ctx, hctx, table, since)
		// Persist the last event ID so the next reconnect resumes from it.
		if lastID != "" {
			since = lastID
		}
		if ctx.Err() != nil {
			return
		}

		// A connection that reached "live" resets the backoff so a long-lived
		// stream doesn't inherit a maxed-out delay on its first drop.
		if live {
			attempt = 0
		}

		if err != nil {
			// A non-retryable API error (401/403/404, ...) is terminal:
			// reconnecting can't fix a bad token or a missing table, and the
			// TS SDK's EventSource likewise ends up closed on a non-200.
			// Emit it and exit — the deferred cleanup sets StatusClosed.
			var apiErr *Error
			if errors.As(err, &apiErr) && !apiErr.Retryable {
				sc.emitError(apiErr)
				return
			}
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
// Returns the last seen event ID (empty if none), whether the connection
// reached the live state, and any error.
func (sc *StreamController) connect(ctx context.Context, hctx httpContext, table, since string) (string, bool, error) {
	u, err := url.Parse(hctx.baseURL + "/v1/stream")
	if err != nil {
		return "", false, err
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
			return "", false, fmt.Errorf("auth: %w", err)
		}
		if token != "" {
			authHeader = "Bearer " + token
		}
	}

	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := hctx.httpClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", false, parseErrorResponse(resp)
	}

	sc.setStatus(StatusLive)

	// Parse SSE frames.
	scanner := bufio.NewScanner(resp.Body)
	// 16 MiB max line: generous headroom over the ~1 MiB NATS MaxPayload
	// ceiling on a single event envelope (oversized records are rejected at
	// ingest publish and never reach the stream).
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var eventID, dataLine string
	lastID := since

	for scanner.Scan() {
		if ctx.Err() != nil {
			return lastID, true, nil
		}

		line := scanner.Text()

		if line == "" {
			// Empty line = end of event frame.
			if dataLine != "" {
				sc.handleSSEData(dataLine)
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

	return lastID, true, scanner.Err()
}

// sseMessage matches the server's SSE event JSON shape.
type sseMessage struct {
	TableName         string         `json:"table_name"`
	ReceivedTimestamp string         `json:"received_timestamp"`
	Data              map[string]any `json:"data"`
}

func (sc *StreamController) handleSSEData(data string) {
	var msg sseMessage
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		// Delivered via the subscriber Error callback rather than the
		// process-global logger, so consumers control visibility and a
		// malformed-frame flood can't spam host-application logs. Payload
		// deliberately omitted: event data can carry tenant/PII fields.
		sc.emitError(fmt.Errorf("malformed SSE message (%d bytes): %w", len(data), err))
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
	compiled := compileFilters(filters)
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
			// closeEventCh serializes with any in-flight emitEvent (which
			// runs on the inner controller's goroutine), so the channel is
			// never closed under a pending send.
			sc.closeEventCh()
			close(sc.done)
		}()

		unsub := inner.Subscribe(&StreamSubscriber{
			Next: func(event StreamEvent) {
				if !matchesFilters(event.Data, compiled) {
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
			unsub()
			inner.Close()
		case <-inner.done:
			// Inner closed on its own — still unsubscribe so the closed
			// controller doesn't retain a reference to this wrapper.
			unsub()
		}
	}()

	return sc
}

// compiledFilter pairs a filter with its precompiled LIKE regex (nil for
// every other operator, or when the pattern isn't a string / doesn't compile).
type compiledFilter struct {
	QueryFilter
	re *regexp.Regexp
}

// compileFilters precompiles LIKE/NOT LIKE patterns once per stream. A
// controller's filters never change after construction, so this replaces a
// per-event compile (and avoids any process-global pattern cache).
func compileFilters(filters []QueryFilter) []compiledFilter {
	out := make([]compiledFilter, len(filters))
	for i, f := range filters {
		out[i] = compiledFilter{QueryFilter: f}
		if f.Op == "like" || f.Op == "not_like" {
			if pattern, ok := f.Value.(string); ok {
				out[i].re = compileLike(pattern)
			}
		}
	}
	return out
}

// compileLike converts a SQL LIKE pattern to a case-insensitive anchored
// regex (matching the TS SDK). Returns nil if the pattern doesn't compile.
func compileLike(pattern string) *regexp.Regexp {
	escaped := regexp.QuoteMeta(pattern)
	escaped = strings.ReplaceAll(escaped, "%", ".*")
	escaped = strings.ReplaceAll(escaped, "_", ".")
	re, err := regexp.Compile("(?i)^" + escaped + "$")
	if err != nil {
		return nil
	}
	return re
}

// matchesFilters evaluates all filters against a data row (AND).
func matchesFilters(row map[string]any, filters []compiledFilter) bool {
	for _, f := range filters {
		val := row[f.Column]
		if !evaluateFilter(val, f.Op, f.Value, f.re) {
			return false
		}
	}
	return true
}

func evaluateFilter(actual any, op string, expected any, re *regexp.Regexp) bool {
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
		aStr, ok := actual.(string)
		if !ok || re == nil {
			return false
		}
		return (op == "like") == re.MatchString(aStr)
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
// Reflection handles []any and typed slices (e.g., []string, []int) alike.
func evaluateIn(actual, expected any) bool {
	rv := reflect.ValueOf(expected)
	if rv.Kind() != reflect.Slice {
		return false
	}
	for i := range rv.Len() {
		if equalValues(actual, rv.Index(i).Interface()) {
			return true
		}
	}
	return false
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
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	// All int/uint widths in two cases (codegen structs use the narrow ones).
	rv := reflect.ValueOf(v)
	switch {
	case rv.CanInt():
		return float64(rv.Int()), true
	case rv.CanUint():
		return float64(rv.Uint()), true
	}
	return 0, false
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
