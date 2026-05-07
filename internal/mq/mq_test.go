package mq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStreamName(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "WAVEHOUSE", StreamName())
}

func TestMessage_AckNak(t *testing.T) {
	t.Parallel()

	var doubleAcked, acked, naked int
	msg := NewMessage(
		t.Context(),
		"ingest.x",
		[]byte("hi"),
		time.Unix(1000, 0),
		func(ctx context.Context) error { doubleAcked++; return nil },
		func() error { acked++; return nil },
		func() error { naked++; return nil },
	)

	assert.Equal(t, "ingest.x", msg.Subject)
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
	msg := NewMessage(t.Context(), "s", nil, time.Now(), nil, nil, nil)
	assert.NotPanics(t, func() { _ = msg.DoubleAck(t.Context()) })
	assert.NotPanics(t, func() { _ = msg.Ack() })
	assert.NotPanics(t, func() { _ = msg.Nak() })
}
