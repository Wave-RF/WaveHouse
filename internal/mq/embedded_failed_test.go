package mq

import (
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/require"
)

// A durable deleted on several tenants' queues ends each delivery; a caller
// that drained the first report must not see the next.
func TestEmbeddedNATS_Consume_ReportsOnceHoweverManyDeliveriesEnd(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx := t.Context()
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "doomed", MaxAckPending: 10})
	require.NoError(t, err)
	delivered := make(chan struct{}, 2)
	stop, failed, err := cons.Consume(func(*Message) { delivered <- struct{}{} }, 4)
	require.NoError(t, err)
	t.Cleanup(stop)
	// A delivery on each tenant proves both pulls are live: a durable deleted
	// before its pull reaches the server ends nothing the client sees.
	for _, id := range []tenant.ID{"acme", "globex"} {
		require.NoError(t, e.Publish(ctx, Topic{Tenant: id, Table: "t"}, []byte("x")))
	}
	for range 2 {
		select {
		case <-delivered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a delivery on each tenant")
		}
	}

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
