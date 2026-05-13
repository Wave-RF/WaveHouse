package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
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

func (m *MockPublisher) Publish(_ context.Context, subject string, data []byte) error {
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

// Ensure MockCache implements cache.Cache at compile time.
var _ cache.Cache = (*MockCache)(nil)

type mockEntry struct {
	data []byte
	tags []string
}

// MockCache implements cache.Cache for testing.
type MockCache struct {
	mu              sync.Mutex
	store           map[string]mockEntry
	InvalidatedTags []string
	Err             error
}

func NewMockCache() *MockCache {
	return &MockCache{store: make(map[string]mockEntry)}
}

func (m *MockCache) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	if m.Err != nil {
		return nil, 0, m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store[key]
	if !ok {
		return nil, 0, nil
	}
	return v.data, 300 * time.Second, nil
}

func (m *MockCache) Set(_ context.Context, key string, value []byte, _ time.Duration, tags []string) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = mockEntry{data: value, tags: tags}
	return nil
}

func (m *MockCache) InvalidateByTags(_ context.Context, tags []string) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.InvalidatedTags = append(m.InvalidatedTags, tags...)

	for key, entry := range m.store {
		for _, targetTag := range tags {
			for _, entryTag := range entry.tags {
				if targetTag == entryTag {
					delete(m.store, key)
					goto NextKey // Skip checking other tags for this key once deleted
				}
			}
		}
	NextKey:
	}
	return nil
}

func (m *MockCache) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InvalidatedTags = nil
	m.store = make(map[string]mockEntry)
}

func (m *MockCache) Close() error { return nil }

// MockConn satisfies the driver.Conn interface.
// We embed driver.Conn so we don't have to implement all 30+ methods.
type MockConn struct {
	driver.Conn
	// QueryFn allows us to define custom behavior per-test
	QueryFn func(ctx context.Context, query string, args ...any) (driver.Rows, error)
	// ExecFn allows us to define custom behavior for Exec calls
	ExecFn func(ctx context.Context, query string, args ...any) error
}

func (m *MockConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if m.QueryFn != nil {
		return m.QueryFn(ctx, query, args...)
	}
	return &MockRows{}, nil
}

func (m *MockConn) Exec(ctx context.Context, query string, args ...any) error {
	if m.ExecFn != nil {
		return m.ExecFn(ctx, query, args...)
	}
	return nil
}

// MockRows satisfies the driver.Rows interface for mocked results.
type MockRows struct {
	driver.Rows
}

func (m *MockRows) Close() error                     { return nil }
func (m *MockRows) Next() bool                       { return false } // Returns no rows by default
func (m *MockRows) ColumnTypes() []driver.ColumnType { return nil }
