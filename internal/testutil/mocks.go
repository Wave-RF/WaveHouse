package testutil

import (
	"context"
	"sync"
	"time"

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

// MockCache implements cache.Cache for testing.
type MockCache struct {
	mu    sync.Mutex
	store map[string][]byte
	Err   error // if set, all operations return this error
}

func NewMockCache() *MockCache {
	return &MockCache{store: make(map[string][]byte)}
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
	return v, 300 * time.Second, nil
}

func (m *MockCache) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = value
	return nil
}

func (m *MockCache) Close() error { return nil }
