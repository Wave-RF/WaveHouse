package mq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A durable deleted on several tenants' queues ends each delivery; a caller
// that drained the first report must not see the next.
func TestEmbeddedNATS_Consume_ReportsOnceHoweverManyDeliveriesEnd(t *testing.T) {
	e := newTestEmbedded(t, "acme", "globex")
	ctx := t.Context()
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "doomed", MaxAckPending: 10})
	require.NoError(t, err)
	stop, failed, err := cons.Consume(func(*Message) {}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)

	require.NoError(t, e.js.DeleteConsumer(ctx, "INGEST_globex", "doomed"))
	select {
	case err := <-failed:
		require.ErrorIs(t, err, ErrDeliveryEnded)
	case <-time.After(5 * time.Second):
		t.Fatal("delivery ended underneath the consumer and nothing was reported")
	}
	require.NoError(t, e.js.DeleteConsumer(ctx, "INGEST_acme", "doomed"))
	select {
	case err := <-failed:
		t.Fatalf("a second failure was reported: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}
