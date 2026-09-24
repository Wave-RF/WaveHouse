package testutil

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Compile-time interface assertions. Catch breakage early when an interface
// these mocks implement gains a method.
var (
	_ mq.Publisher       = (*MockPublisher)(nil)
	_ mq.Subscriber      = (*MockSubscriber)(nil)
	_ mq.DeadLetterer    = (*MockPublisher)(nil)
	_ mq.Purger          = (*MockPurger)(nil)
	_ mq.DeadLetterStats = (*MockDeadLetterStats)(nil)
	_ http.RoundTripper  = (*MockRoundTripper)(nil)
)

// ── Mock Publisher ───────────────────────────────────────────────

// MockPublisher records all published messages — ingest publishes and
// dead-letter parkings alike — for test assertions.
type MockPublisher struct {
	mu       sync.Mutex
	Messages []PublishedMessage
	Err      error // if set, Publish and DeadLetter return this error
}

// PublishedMessage records a single Publish or DeadLetter call, with the
// headers the options set.
type PublishedMessage struct {
	Topic      mq.Topic
	DeadLetter bool // parked via DeadLetter rather than published via Publish
	Data       []byte
	Headers    mq.Headers
}

func (m *MockPublisher) Publish(_ context.Context, topic mq.Topic, data []byte, opts ...mq.PublishOpt) error {
	return m.record(PublishedMessage{Topic: topic, Data: data}, opts)
}

func (m *MockPublisher) DeadLetter(_ context.Context, msg *mq.Message, opts ...mq.PublishOpt) error {
	return m.record(PublishedMessage{Topic: msg.Topic(), DeadLetter: true, Data: msg.Data}, opts)
}

func (m *MockPublisher) record(pm PublishedMessage, opts []mq.PublishOpt) error {
	if m.Err != nil {
		return m.Err
	}
	headers := mq.Headers{}
	for _, opt := range opts {
		opt(headers)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	pm.Headers = headers
	m.Messages = append(m.Messages, pm)
	return nil
}

func (m *MockPublisher) Close() error { return nil }

// Published returns a snapshot of the publish calls recorded so far.
func (m *MockPublisher) Published() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]PublishedMessage(nil), m.Messages...)
}

// LastMessage returns the most recently published message, or nil.
func (m *MockPublisher) LastMessage() *PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.Messages) == 0 {
		return nil
	}
	msg := m.Messages[len(m.Messages)-1]
	return &msg
}

// ── Mock Subscriber ──────────────────────────────────────────────

// MockSubscriber implements mq.Subscriber for testing.
type MockSubscriber struct {
	Err     error
	Handler func(msg *mq.Message) error
}

func (m *MockSubscriber) Subscribe(_ context.Context, _ string, handler func(msg *mq.Message) error) error {
	m.Handler = handler
	return m.Err
}

func (m *MockSubscriber) Close() error { return nil }

// ── Mock Deduplicator ────────────────────────────────────────────

// MockDeduplicator implements dedupe.Deduplicator for testing.
type MockDeduplicator struct {
	mu   sync.Mutex
	seen map[string]bool
	Err  error // if set, CheckAndMark returns this error
}

func NewMockDeduplicator() *MockDeduplicator {
	return &MockDeduplicator{seen: make(map[string]bool)}
}

func (m *MockDeduplicator) Stats() map[string]int64 {
	// Return empty stats or mock data for testing
	return map[string]int64{
		"pebble_wal_size":    0,
		"pebble_table_count": 0,
	}
}

func (m *MockDeduplicator) CheckAndMark(_ context.Context, eventID string) (bool, error) {
	if m.Err != nil {
		return false, m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen[eventID] {
		return true, nil
	}
	m.seen[eventID] = true
	return false, nil
}

func (m *MockDeduplicator) Close() error { return nil }

// ── Mock Cache ───────────────────────────────────────────────────

// MockCache implements cache.Cache and records invalidated namespaces for testing.
type MockCache struct {
	cache.Cache   // Embed to satisfy remaining interface methods silently
	InvNamespaces []cache.Namespace
	// InvTenants records every InvalidateTenant, in order.
	InvTenants []tenant.ID
	InvErr     error
	mu         sync.Mutex
}

func (m *MockCache) Invalidate(_ context.Context, namespaces []cache.Namespace) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InvNamespaces = append(m.InvNamespaces, namespaces...)
	return uint64(len(namespaces)), m.InvErr
}

func (m *MockCache) InvalidateTenant(_ context.Context, id tenant.ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InvTenants = append(m.InvTenants, id)
	return m.InvErr
}

// GetTenants returns the tenants InvalidateTenant was called for, in order.
func (m *MockCache) GetTenants() []tenant.ID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]tenant.ID(nil), m.InvTenants...)
}

func (m *MockCache) GetNamespaces() []cache.Namespace {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]cache.Namespace(nil), m.InvNamespaces...) // Return copy
}

// ── Mock mq.Message ──────────────────────────────────────────────

// MockMessage builds mq.Messages whose ack-family callbacks flip the flags
// below, so a test can assert what the worker did with a message.
type MockMessage struct {
	MsgData  []byte
	MsgTopic mq.Topic

	// Configurable errors returned from the matching ack-family methods.
	AckErr       error
	NakErr       error
	DoubleAckErr error

	// State flags — atomic because production calls these from goroutines.
	Acked       atomic.Bool
	Naked       atomic.Bool
	DoubleAcked atomic.Bool
}

// Message returns an mq.Message wired to this mock's flags. Every call returns
// a new Message sharing the same flags.
func (m *MockMessage) Message() *mq.Message {
	return mq.NewMessage(context.Background(), m.MsgTopic, m.MsgData, time.Time{},
		func(context.Context) error {
			m.DoubleAcked.Store(true)
			return m.DoubleAckErr
		},
		func() error {
			m.Acked.Store(true)
			return m.AckErr
		},
		func() error {
			m.Naked.Store(true)
			return m.NakErr
		},
	)
}

// ── Mock mq.Purger ───────────────────────────────────────────────

// MockPurger implements mq.Purger and records every call.
type MockPurger struct {
	Purged bool  // what PurgeAcked reports
	Err    error // if set, PurgeAcked returns this error

	mu    sync.Mutex
	Calls []PurgeCall
}

// PurgeCall records one PurgeAcked call.
type PurgeCall struct {
	Consumer  string
	OlderThan time.Time
}

func (m *MockPurger) PurgeAcked(_ context.Context, consumer string, olderThan time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, PurgeCall{Consumer: consumer, OlderThan: olderThan})
	return m.Purged, m.Err
}

// ── Mock mq.DeadLetterStats ──────────────────────────────────────

// MockDeadLetterStats implements mq.DeadLetterStats with a canned answer.
type MockDeadLetterStats struct {
	Counts mq.DeadLetterCounts
	Err    error
}

func (m *MockDeadLetterStats) DeadLetterCounts(context.Context, string) (mq.DeadLetterCounts, error) {
	return m.Counts, m.Err
}

// ── Mock http.RoundTripper ───────────────────────────────────────

// MockRoundTripper intercepts HTTP requests for the components that take a
// custom *http.Client (today: the ClickHouse inserter). Fn lets a test
// hand-roll a response; if Fn is nil, returns 200 OK.
//
// Every request is captured (with body slurped + replaced so the handler
// can still read it) — useful for asserting request shape after the fact.
type MockRoundTripper struct {
	Fn func(req *http.Request) (*http.Response, error)

	mu       sync.Mutex
	hits     atomic.Int32
	requests []CapturedRequest
}

// CapturedRequest is a frozen snapshot of an outgoing HTTP request.
type CapturedRequest struct {
	Method string
	URL    string
	Header http.Header
	Body   []byte
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	m.hits.Add(1)

	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		_ = req.Body.Close()
		// Replace so the handler-side Fn can also read the body if it wants to.
		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	m.mu.Lock()
	m.requests = append(m.requests, CapturedRequest{
		Method: req.Method,
		URL:    req.URL.String(),
		Header: req.Header.Clone(),
		Body:   body,
	})
	m.mu.Unlock()

	if m.Fn != nil {
		return m.Fn(req)
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString("OK")),
	}, nil
}

func (m *MockRoundTripper) Hits() int32 { return m.hits.Load() }

func (m *MockRoundTripper) Captured() []CapturedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]CapturedRequest(nil), m.requests...)
}
