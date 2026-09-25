package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
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
	// ErrAfter lets that many calls succeed before Err applies, to fail a
	// batch part-way through.
	ErrAfter int
	calls    int
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.Err != nil && m.calls > m.ErrAfter {
		return m.Err
	}
	headers := mq.Headers{}
	for _, opt := range opts {
		opt(headers)
	}
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

// MockDeduplicator implements dedupe.Deduplicator in memory, with per-phase
// error injection and a record of what was committed and released.
type MockDeduplicator struct {
	mu        sync.Mutex
	committed map[dedupe.Key]bool
	retention map[dedupe.Key]time.Duration // each commit's retention
	pending   map[dedupe.Key]string
	tokens    int
	// Err, if set, fails Reserve — after ErrAfter calls have succeeded;
	// CommitErr and ReleaseErr fail their phase.
	Err        error
	ErrAfter   int
	CommitErr  error
	ReleaseErr error
	Released   []dedupe.Claim // every claim Release was given
	// Calls to each phase, for tests that count round trips.
	Reserves, Commits int
}

var _ dedupe.Deduplicator = (*MockDeduplicator)(nil)

func NewMockDeduplicator() *MockDeduplicator {
	return &MockDeduplicator{committed: map[dedupe.Key]bool{}, retention: map[dedupe.Key]time.Duration{}, pending: map[dedupe.Key]string{}}
}

// Reserve answers Duplicate for a key repeated in one call, as Managed does.
func (m *MockDeduplicator) Reserve(_ context.Context, keys []dedupe.Key, _ time.Duration) ([]dedupe.Claim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Reserves++
	if m.Err != nil && m.Reserves > m.ErrAfter {
		return nil, m.Err
	}
	claims := make([]dedupe.Claim, 0, len(keys))
	seen := make(map[dedupe.Key]bool, len(keys))
	for _, k := range keys {
		repeat := seen[k]
		seen[k] = true
		switch {
		case repeat, m.committed[k]:
			claims = append(claims, dedupe.Claim{Key: k, Status: dedupe.Duplicate})
		case m.pending[k] != "":
			claims = append(claims, dedupe.Claim{Key: k, Status: dedupe.InFlight})
		default:
			m.tokens++
			tok := fmt.Sprint(m.tokens)
			m.pending[k] = tok
			claims = append(claims, dedupe.Claim{Key: k, Status: dedupe.Claimed, Token: tok})
		}
	}
	return claims, nil
}

func (m *MockDeduplicator) Commit(_ context.Context, claims []dedupe.Claim, retention time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Commits++
	if m.CommitErr != nil {
		return m.CommitErr
	}
	for _, c := range claims {
		if c.Status == dedupe.Claimed {
			m.committed[c.Key] = true
			m.retention[c.Key] = retention
			delete(m.pending, c.Key)
		}
	}
	return nil
}

func (m *MockDeduplicator) Release(_ context.Context, claims []dedupe.Claim) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Released = append(m.Released, claims...)
	if m.ReleaseErr != nil {
		return m.ReleaseErr
	}
	for _, c := range claims {
		if c.Status == dedupe.Claimed && m.pending[c.Key] == c.Token {
			delete(m.pending, c.Key)
		}
	}
	return nil
}

// Hold claims k as another in-flight request would, so Reserve answers
// InFlight for it.
func (m *MockDeduplicator) Hold(k dedupe.Key) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending[k] = "held"
}

// Committed reports whether k was committed.
func (m *MockDeduplicator) Committed(k dedupe.Key) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.committed[k]
}

// Retention is the retention k was last committed with.
func (m *MockDeduplicator) Retention(k dedupe.Key) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retention[k]
}

// Pending reports whether k is claimed and neither committed nor released.
func (m *MockDeduplicator) Pending(k dedupe.Key) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pending[k] != ""
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
	OlderThan map[tenant.ID]time.Time
}

func (m *MockPurger) PurgeAcked(_ context.Context, consumer string, olderThan map[tenant.ID]time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, PurgeCall{Consumer: consumer, OlderThan: olderThan})
	return m.Purged, m.Err
}

// ── Mock mq.DeadLetterStats ──────────────────────────────────────

// MockDeadLetterStats implements mq.DeadLetterStats with a canned answer,
// recording the tenant and table each call asked about.
type MockDeadLetterStats struct {
	Counts mq.DeadLetterCounts
	Err    error

	mu     sync.Mutex
	Tenant tenant.ID
	Table  string
}

func (m *MockDeadLetterStats) DeadLetterCounts(_ context.Context, id tenant.ID, table string) (mq.DeadLetterCounts, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Tenant, m.Table = id, table
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
