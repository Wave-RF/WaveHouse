package mq

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A durable deleted on several tenants' queues ends each delivery; a caller
// that drained the first report must not see the next.
func TestEmbeddedNATS_Consume_ReportsOnceHoweverManyDeliveriesEnd(t *testing.T) {
	e := newTestEmbedded(t, "acme", "globex")
	cons, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: "once", MaxAckPending: 10})
	require.NoError(t, err)
	c := cons.(*workerConsumer)

	c.fail(errors.New("acme ended"))
	require.EqualError(t, <-c.failed, "acme ended")
	c.fail(errors.New("globex ended"))
	select {
	case err := <-c.failed:
		t.Fatalf("a second failure was reported: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}
