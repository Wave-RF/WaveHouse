package mq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// OnSettled runs once, on the first settlement that succeeds, whichever of
// the four it is; a failed one does not count, and NakWithDelay without a
// delayed form falls back through the wrapped Nak.
func TestMessage_OnSettled(t *testing.T) {
	t.Parallel()
	fail := errors.New("no answer")
	for name, settle := range map[string]func(*Message) error{
		"double ack":     func(m *Message) error { return m.DoubleAck(context.Background()) },
		"ack":            (*Message).Ack,
		"nak":            (*Message).Nak,
		"nak with delay": func(m *Message) error { return m.NakWithDelay(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var calls, fired int
			var err error
			cb := func() error { calls++; return err }
			m := NewMessage(context.Background(), Topic{Tenant: "acme", Table: "t"}, nil, time.Now(),
				func(context.Context) error { return cb() }, cb, cb)
			m.OnSettled(func() { fired++ })
			err = fail
			require.ErrorIs(t, settle(m), fail)
			assert.Zero(t, fired, "a failed settlement does not count")
			err = nil
			require.NoError(t, settle(m))
			require.NoError(t, settle(m))
			assert.Equal(t, 1, fired)
			assert.Equal(t, 3, calls, "every call reaches the broker")
		})
	}
	m := NewMessage(context.Background(), Topic{Tenant: "acme", Table: "t"}, nil, time.Now(), nil, nil, nil)
	fired := 0
	m.OnSettled(func() { fired++ })
	require.NoError(t, m.Ack())
	assert.Equal(t, 1, fired, "a message without callbacks settles at once")
}
