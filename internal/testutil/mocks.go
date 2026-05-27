package testutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Compile-time interface assertions. Catch breakage early if the underlying
// upstream interface gains methods our mocks don't override.
var (
	_ jetstream.Msg     = (*MockJetStreamMsg)(nil)
	_ http.RoundTripper = (*MockRoundTripper)(nil)
)

// ── Mock Publisher ───────────────────────────────────────────────

// MockPublisher records all published messages for test assertions.
type MockPublisher struct {
	mu       sync.Mutex
	Messages []PublishedMessage
	Err      error // if set, Publish returns this error
}

// PublishedMessage records a single publish call.
type PublishedMessage struct {
	Subject string
	Data    []byte
}

func (m *MockPublisher) Publish(_ context.Context, subject string, data []byte, opts ...mq.PublishOpt) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Messages = append(m.Messages, PublishedMessage{Subject: subject, Data: data})
	return nil
}

func (m *MockPublisher) Close() error { return nil }

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

func (m *MockSubscriber) Subscribe(_ context.Context, _, _ string, handler func(msg *mq.Message) error) error {
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

// MockCache implements cache.Cache and records invalidation keys for testing.
type MockCache struct {
	cache.Cache // Embed to satisfy remaining interface methods silently
	InvKeys     []string
	InvErr      error
	mu          sync.Mutex
}

func (m *MockCache) InvalidateCache(ctx context.Context, keys []string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InvKeys = append(m.InvKeys, keys...)
	return uint64(len(keys)), m.InvErr
}

func (m *MockCache) GetKeys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.InvKeys...) // Return copy
}

// ── Mock jetstream.Msg ───────────────────────────────────────────

// MockJetStreamMsg implements jetstream.Msg for unit tests. Only the methods
// production code actually exercises (Data/Subject/Headers/Ack/Nak/DoubleAck)
// are overridden — calls to anything else panic, which is the loud-fail
// signal we want from a unit test mock.
type MockJetStreamMsg struct {
	MsgData    []byte
	MsgSubject string
	MsgHeaders nats.Header

	// Configurable errors returned from the matching ack-family methods.
	AckErr       error
	NakErr       error
	DoubleAckErr error

	// State flags — atomic because production calls these from goroutines.
	Acked       atomic.Bool
	Naked       atomic.Bool
	DoubleAcked atomic.Bool
}

func (m *MockJetStreamMsg) Data() []byte         { return m.MsgData }
func (m *MockJetStreamMsg) Subject() string      { return m.MsgSubject }
func (m *MockJetStreamMsg) Headers() nats.Header { return m.MsgHeaders }
func (m *MockJetStreamMsg) Reply() string        { return "" }

func (m *MockJetStreamMsg) Ack() error {
	m.Acked.Store(true)
	return m.AckErr
}

func (m *MockJetStreamMsg) Nak() error {
	m.Naked.Store(true)
	return m.NakErr
}

func (m *MockJetStreamMsg) DoubleAck(_ context.Context) error {
	m.DoubleAcked.Store(true)
	return m.DoubleAckErr
}

// The remaining jetstream.Msg methods are intentionally unimplemented —
// they panic to signal "the production code touched a path this test
// wasn't expecting." Implement on demand.
func (m *MockJetStreamMsg) NakWithDelay(_ time.Duration) error {
	panic("MockJetStreamMsg.NakWithDelay not implemented")
}
func (m *MockJetStreamMsg) InProgress() error { panic("MockJetStreamMsg.InProgress not implemented") }
func (m *MockJetStreamMsg) Term() error       { panic("MockJetStreamMsg.Term not implemented") }
func (m *MockJetStreamMsg) TermWithReason(string) error {
	panic("MockJetStreamMsg.TermWithReason not implemented")
}

func (m *MockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	panic("MockJetStreamMsg.Metadata not implemented")
}

// ── Mock JetStream (Stream / Consumer / PublishMsg) ──────────────

// MockJetStream embeds jetstream.JetStream so unused methods panic. Override
// only what your test touches: StreamFn / ConsumerFn for sweeper-style code,
// PublishMsg recorder (Published + PubErr) for DLQ-style code.
type MockJetStream struct {
	jetstream.JetStream

	StreamFn     func(ctx context.Context, name string) (jetstream.Stream, error)
	ConsumerFn   func(ctx context.Context, stream, consumer string) (jetstream.Consumer, error)
	PublishMsgFn func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)

	mu        sync.Mutex
	published []*nats.Msg
	PubErr    error // if set, PublishMsg returns this error (and skips recording)
}

func (m *MockJetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if m.StreamFn == nil {
		return nil, errors.New("MockJetStream.Stream: StreamFn not set")
	}
	return m.StreamFn(ctx, name)
}

func (m *MockJetStream) Consumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error) {
	if m.ConsumerFn == nil {
		return nil, errors.New("MockJetStream.Consumer: ConsumerFn not set")
	}
	return m.ConsumerFn(ctx, stream, consumer)
}

func (m *MockJetStream) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if m.PublishMsgFn != nil {
		return m.PublishMsgFn(ctx, msg, opts...)
	}
	if m.PubErr != nil {
		return nil, m.PubErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, msg)
	return &jetstream.PubAck{}, nil
}

// Published returns a snapshot of PublishMsg calls recorded so far.
func (m *MockJetStream) Published() []*nats.Msg {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*nats.Msg(nil), m.published...)
}

// ── Mock jetstream.Stream ────────────────────────────────────────

// MockStream embeds jetstream.Stream so unused methods panic. Provide Msgs
// for static GetMsg lookup, or GetMsgFn for richer behaviour. Purge calls
// are recorded into Purged.
type MockStream struct {
	jetstream.Stream

	InfoVal *jetstream.StreamInfo
	InfoErr error

	Msgs     map[uint64]*jetstream.RawStreamMsg
	GetMsgFn func(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)

	mu       sync.Mutex
	Purged   []jetstream.StreamPurgeOpt
	PurgeErr error
}

func (m *MockStream) Info(_ context.Context, _ ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return m.InfoVal, m.InfoErr
}

func (m *MockStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if m.GetMsgFn != nil {
		return m.GetMsgFn(ctx, seq, opts...)
	}
	msg, ok := m.Msgs[seq]
	if !ok {
		return nil, errors.New("MockStream.GetMsg: message not found")
	}
	return msg, nil
}

func (m *MockStream) Purge(_ context.Context, opts ...jetstream.StreamPurgeOpt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Purged = append(m.Purged, opts...)
	return m.PurgeErr
}

// ── Mock jetstream.Consumer ──────────────────────────────────────

// MockConsumer embeds jetstream.Consumer so unused methods panic.
type MockConsumer struct {
	jetstream.Consumer

	InfoVal *jetstream.ConsumerInfo
	InfoErr error
}

func (m *MockConsumer) Info(_ context.Context) (*jetstream.ConsumerInfo, error) {
	return m.InfoVal, m.InfoErr
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
