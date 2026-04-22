package api

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Hub manages broadcast fan-out from a single MQ subscription to N clients.
type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan []byte]struct{} // topic -> set of channels
}

// NewHub creates a broadcast hub.
func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[string]map[chan []byte]struct{}),
	}
}

// Subscribe registers a channel to receive messages for a topic.
func (h *Hub) Subscribe(topic string, ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subscribers[topic] == nil {
		h.subscribers[topic] = make(map[chan []byte]struct{})
	}
	h.subscribers[topic][ch] = struct{}{}
}

// Unsubscribe removes a channel from a topic and closes it.
// The caller must not send to or read from ch after calling Unsubscribe.
func (h *Hub) Unsubscribe(topic string, ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	subs, ok := h.subscribers[topic]
	if !ok {
		return
	}
	if _, found := subs[ch]; !found {
		return
	}
	delete(subs, ch)
	if len(subs) == 0 {
		delete(h.subscribers, topic)
	}
	// Only close the channel if it is no longer registered under any topic.
	for _, s := range h.subscribers {
		if _, exists := s[ch]; exists {
			return
		}
	}
	close(ch)
}

// Broadcast sends an event to all subscribers of a topic.
func (h *Hub) Broadcast(topic string, msg *mq.Message) {
	ctx := msg.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	envelope := struct {
		TraceHeaders map[string]string `json:"trace_headers"`
		Payload      []byte            `json:"payload"`
	}{
		TraceHeaders: carrier,
		Payload:      msg.Data, // This is the raw ingest.EventMessage JSON
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	sent := make(map[chan []byte]struct{})

	// Exact match.
	for ch := range h.subscribers[topic] {
		sent[ch] = struct{}{}
		select {
		case ch <- data:
		default:
		}
	}

	// Wildcard match: iterate all subscriber patterns.
	for pattern, chs := range h.subscribers {
		if pattern == topic {
			continue // already handled above
		}
		if !matchTopic(pattern, topic) {
			continue
		}
		for ch := range chs {
			if _, dup := sent[ch]; dup {
				continue
			}
			sent[ch] = struct{}{}
			select {
			case ch <- data:
			default:
			}
		}
	}
}

// matchTopic checks whether a NATS-style pattern matches a subject.
// Tokens are separated by ".".
//   - "*" matches exactly one token
//   - ">" as the last pattern token matches one or more remaining tokens
func matchTopic(pattern, subject string) bool {
	pTokens := strings.Split(pattern, ".")
	sTokens := strings.Split(subject, ".")

	for i, pt := range pTokens {
		if pt == ">" {
			// ">" must be the last token and matches 1+ remaining subject tokens.
			return i < len(sTokens)
		}
		if i >= len(sTokens) {
			return false
		}
		if pt != "*" && pt != sTokens[i] {
			return false
		}
	}
	return len(pTokens) == len(sTokens)
}
