package mq

import (
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

	var acked, naked int
	msg := NewMessage(
		t.Context(),
		"ingest.x",
		[]byte("hi"),
		time.Unix(1000, 0),
		func() { acked++ },
		func() { naked++ },
	)

	assert.Equal(t, "ingest.x", msg.Subject)
	assert.Equal(t, []byte("hi"), msg.Data)
	assert.Equal(t, int64(1000), msg.Timestamp.Unix())

	msg.Ack()
	msg.Nak()
	assert.Equal(t, 1, acked)
	assert.Equal(t, 1, naked)
}

func TestMessage_NilCallbacks(t *testing.T) {
	t.Parallel()

	// Ack/Nak on a message with nil callbacks must be a no-op, not panic.
	msg := NewMessage(t.Context(), "s", nil, time.Now(), nil, nil)
	assert.NotPanics(t, msg.Ack)
	assert.NotPanics(t, msg.Nak)
}
