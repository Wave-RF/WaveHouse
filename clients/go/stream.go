package wavehouse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mime"
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

// newController builds a controller whose event channel buffers from
// construction, so events predating the first Events call are not lost.
func newController(status StreamStatus, cancel context.CancelFunc) *StreamController {
	return &StreamController{
		status:  status,
		eventCh: make(chan StreamEvent, 256),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
}

// newStreamController opens an SSE connection for the given table.
func newStreamController(hctx httpContext, table string, opts *StreamOptions) *StreamController {
	ctx, cancel := context.WithCancel(context.Background())
	sc := newController(StatusConnecting, cancel)
	go sc.run(ctx, hctx, table, opts)
	return sc
}

// Status returns the current connection status.
func (sc *StreamController) Status() StreamStatus {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.status
}

// Subscribe registers callbacks and returns an unsubscribe function. The
// subscriber's Status callback fires immediately with the current status.
func (sc *StreamController) Subscribe(sub *StreamSubscriber) func() {
	sc.mu.Lock()
	sc.subscribers = append(sc.subscribers, sub)
	currentStatus := sc.status
	sc.mu.Unlock()

	// Benign race: setStatus also calls the subscriber, so a stale status here
	// is immediately followed by the correct one.
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

// Events returns a read-only channel of stream events, closed when the stream
// closes. A consumer that never reads it fills the buffer and trips a
// one-time drop log.
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

	// Deliberately does not wait on sc.done: callbacks run on the stream
	// goroutine, so waiting would deadlock a Close made from a callback.
	sc.cancel()
}

func (sc *StreamController) setStatus(s StreamStatus) {
	sc.mu.Lock()
	// StatusClosed is terminal: a stale Status callback can land after Close
	// and must not resurrect the status.
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
// setStatus keeps its own inline copy so the snapshot shares a critical
// section with the status write.
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

	// Guarded by mu so the send and closeEventCh serialize: a late event can
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

// closeEventCh marks the controller closed and closes the events channel. It
// must serialize with emitEvent's send via mu.
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

		// Reaching "live" resets the backoff so a long-lived stream doesn't
		// inherit a maxed-out delay on its first drop.
		if live {
			attempt = 0
		}

		if err != nil {
			// connect classifies its own failures, so Retryable decides
			// whether to reconnect: a non-retryable error is terminal.
			var apiErr *Error
			if errors.As(err, &apiErr) {
				sc.emitError(apiErr)
				if !apiErr.Retryable {
					return
				}
			} else {
				sc.emitError(sseError(0, "SSE_ERROR", err.Error(), true))
			}
		}

		sc.setStatus(StatusReconnecting)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff(attempt)):
		}
		attempt++
	}
}

// connect reads one SSE connection until it closes, returning the last seen
// event ID, whether the connection ever reached the live state, and any error.
func (sc *StreamController) connect(ctx context.Context, hctx httpContext, table, since string) (string, bool, error) {
	u, err := url.Parse(hctx.baseURL + "/v1/stream")
	if err != nil {
		return "", false, sseError(0, "SSE_CONNECT_ERROR", fmt.Sprintf("invalid baseURL: %v", err), false)
	}
	// A non-HTTP scheme can never carry SSE, so retrying one just spins.
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false, sseError(0, "SSE_CONNECT_ERROR",
			fmt.Sprintf("baseURL scheme %q is not http or https", u.Scheme), false)
	}
	q := u.Query()
	q.Set("table", table)
	if since != "" {
		q.Set("since", since)
	}

	// Auth travels in the Authorization header, not ?token= as browser
	// EventSource requires.
	var authHeader string
	if hctx.auth != nil {
		token, err := hctx.auth(ctx)
		if err != nil {
			// Retryable: a token endpoint having a bad minute shouldn't tear
			// down a healthy long-lived stream.
			return "", false, sseError(0, "SSE_AUTH_ERROR", err.Error(), true)
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
	applyConfiguredHeaders(req.Header, hctx.headers)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	client := hctx.httpClient
	credentialed := authHeader != "" || len(hctx.headers) > 0
	if credentialed {
		// Never follow a redirect while carrying a credential: net/http drops
		// Authorization across hosts but forwards custom headers verbatim.
		// Copied so a caller-supplied client keeps its own CheckRedirect.
		c := *client
		c.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
		client = &c
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", false, errAborted
		}
		return "", false, sseError(0, "SSE_NETWORK_ERROR", err.Error(), true)
	}
	defer func() { _ = resp.Body.Close() }()

	if credentialed && resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return "", false, sseError(resp.StatusCode, "SSE_REDIRECT", fmt.Sprintf(
			"stream endpoint redirected to %q and the SDK did not follow it; redirects are refused while the request carries a credential",
			resp.Header.Get("Location")), false)
	}

	if resp.StatusCode != http.StatusOK {
		return "", false, parseErrorResponse(resp)
	}

	// A 200 that isn't an event stream means an intermediary answered; without
	// this the stream would sit in StatusLive delivering nothing.
	if ct := resp.Header.Get("Content-Type"); !isEventStream(ct) {
		shown := ct
		if shown == "" {
			shown = "(none)"
		}
		return "", false, sseError(resp.StatusCode, "SSE_BAD_CONTENT_TYPE",
			fmt.Sprintf("expected Content-Type text/event-stream, got %s", shown), false)
	}

	sc.setStatus(StatusLive)

	scanner := bufio.NewScanner(resp.Body)
	// 16 MiB max line: headroom over the ~1 MiB NATS MaxPayload ceiling on a
	// single event envelope.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var eventID, dataLine string
	lastID := since

	for scanner.Scan() {
		if ctx.Err() != nil {
			return lastID, true, nil
		}

		line := scanner.Text()

		if line == "" {
			// End of an event frame.
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

		if strings.HasPrefix(line, ":") { // comment: keepalive or connected
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

	if scanErr := scanner.Err(); scanErr != nil {
		return lastID, true, sseError(0, "SSE_READ_ERROR", scanErr.Error(), true)
	}
	return lastID, true, nil
}

// sseError builds a stream [*Error] with the SDK's SSE_* taxonomy.
func sseError(status int, code, msg string, retryable bool) *Error {
	return &Error{Status: status, Code: code, Message: msg, Retryable: retryable}
}

// isEventStream reports whether a Content-Type header names text/event-stream,
// ignoring any parameters (charset, boundary) and case.
func isEventStream(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return mediaType == "text/event-stream"
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
		// Reported through the subscriber rather than the global logger, and
		// without the payload, which can carry tenant/PII fields.
		sc.emitError(sseError(0, "SSE_PARSE_ERROR",
			fmt.Sprintf("malformed SSE message (%d bytes): %v", len(data), err), true))
		return
	}

	sc.emitEvent(StreamEvent{Table: msg.TableName, Timestamp: msg.ReceivedTimestamp, Data: msg.Data})
}

// newFilteredStreamController wraps a StreamController with client-side
// filtering and column projection.
func newFilteredStreamController(inner *StreamController, filters []QueryFilter, columns []string) *StreamController {
	compiled := compileFilters(filters)
	ctx, cancel := context.WithCancel(context.Background())
	sc := newController(inner.Status(), cancel)

	go func() {
		defer func() {
			sc.setStatus(StatusClosed)
			// closeEventCh serializes with any in-flight emitEvent on the
			// inner goroutine, so the channel never closes under a send.
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
			// Unsubscribe anyway so the closed inner controller doesn't
			// retain a reference to this wrapper.
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

// compileFilters precompiles LIKE/NOT LIKE patterns once per stream, which a
// controller's immutable filter list makes safe.
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
// regex, returning nil if the pattern doesn't compile.
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
		if !evaluateFilter(row[f.Column], f.Op, f.Value, f.re) {
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

// equalValues compares two values for equality, normalizing numeric types and
// comparing timestamps as instants rather than as text (see asInstant).
func equalValues(a, b any) bool {
	// nil only equals nil: otherwise the fmt.Sprint fallback would match a
	// missing column against the literal string "<nil>".
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if af, aOK := toFloat64(a); aOK {
		if bf, bOK := toFloat64(b); bOK {
			return af == bf
		}
	}
	if at, aOK := asInstant(a); aOK {
		if bt, bOK := asInstant(b); bOK {
			return at.Equal(bt)
		}
	}
	// fmt.Sprint is safe for all types (no panic on maps/slices).
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// maxTimeOperandChars mirrors the server's row-filter pre-gate: the longest
// accepted spelling is 35 bytes, so this bounds per-event parse work.
const maxTimeOperandChars = 64

// asInstant reports the instant a value denotes, and only for RFC 3339
// spellings carrying an explicit offset or `Z`. Zone-less spellings are
// rejected on purpose: they name an instant only relative to the column's
// timezone, which a subscriber does not know.
func asInstant(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok || len(s) > maxTimeOperandChars {
		return time.Time{}, false
	}
	// Go's RFC3339Nano accepts a ',' decimal separator per ISO 8601 but
	// ClickHouse does not, so reject a spelling the server would refuse.
	if strings.ContainsRune(s, ',') {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// evaluateIn reports whether actual is contained in the expected slice.
// Reflection handles []any and typed slices alike.
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

// order reports the sign of a-b, mirroring Go's comparison operators (so an
// incomparable float pair such as NaN reports equal rather than ordered).
func order[T float64 | string](a, b T) (int, bool) {
	switch {
	case a < b:
		return -1, true
	case a > b:
		return 1, true
	default:
		return 0, true
	}
}

// compareOrdered returns (-1, 0, or 1) and true for comparable ordered types,
// or (0, false) when the types cannot be compared.
func compareOrdered(actual, expected any) (int, bool) {
	if a, aOK := toFloat64(actual); aOK {
		if b, bOK := toFloat64(expected); bOK {
			return order(a, b)
		}
	}
	// Timestamps compare chronologically and fail closed when only one side is
	// a provable instant, matching the server's row filter.
	aTime, aIsTime := asInstant(actual)
	bTime, bIsTime := asInstant(expected)
	if aIsTime || bIsTime {
		if !aIsTime || !bIsTime {
			return 0, false
		}
		return aTime.Compare(bTime), true
	}
	if aStr, ok := actual.(string); ok {
		if bStr, ok := expected.(string); ok {
			return order(aStr, bStr)
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
