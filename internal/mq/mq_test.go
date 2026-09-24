package mq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessage_AckNak(t *testing.T) {
	t.Parallel()

	var doubleAcked, acked, naked int
	msg := NewMessage(
		t.Context(),
		Topic{Tenant: "0", Table: "x"},
		[]byte("hi"),
		time.Unix(1000, 0),
		func(ctx context.Context) error { doubleAcked++; return nil },
		func() error { acked++; return nil },
		func() error { naked++; return nil },
	)

	assert.Equal(t, Topic{Tenant: "0", Table: "x"}, msg.Topic())
	assert.Equal(t, "0.x", msg.TopicKey())
	assert.Equal(t, []byte("hi"), msg.Data)
	assert.Equal(t, int64(1000), msg.Timestamp.Unix())

	_ = msg.DoubleAck(t.Context())
	_ = msg.Ack()
	_ = msg.Nak()
	assert.Equal(t, 1, doubleAcked)
	assert.Equal(t, 1, acked)
	assert.Equal(t, 1, naked)
}

func TestMessage_NilCallbacks(t *testing.T) {
	t.Parallel()

	// Ack/Nak on a message with nil callbacks must be a no-op, not panic.
	msg := NewMessage(t.Context(), Topic{Tenant: "0", Table: "s"}, nil, time.Now(), nil, nil, nil)
	assert.NotPanics(t, func() { _ = msg.DoubleAck(t.Context()) })
	assert.NotPanics(t, func() { _ = msg.Ack() })
	assert.NotPanics(t, func() { _ = msg.Nak() })
}

func TestHeaders(t *testing.T) {
	t.Parallel()

	h := Headers{}
	assert.Empty(t, h.Get("missing"))

	h.Add("k", "1")
	h.Add("k", "2")
	assert.Equal(t, "1", h.Get("k"), "Get returns the first value")
	assert.Equal(t, []string{"1", "2"}, h["k"], "Add appends")

	h.Set("k", "3")
	assert.Equal(t, []string{"3"}, h["k"], "Set replaces")

	// Exact-key, like nats.Header.
	assert.Empty(t, h.Get("K"))
}

func TestWithHeader(t *testing.T) {
	t.Parallel()

	h := Headers{}
	WithHeader("X-A", "1")(h)
	WithHeader("X-A", "2")(h)
	WithHeader("X-B", "b")(h)
	assert.Equal(t, Headers{"X-A": {"1", "2"}, "X-B": {"b"}}, h)
}
