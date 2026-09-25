package mq_test

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/mqtest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedNATS_Conformance(t *testing.T) {
	mqtest.Run(t, mqtest.Harness{
		New: func(t *testing.T) mq.Broker {
			e, err := mq.NewEmbedded(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = e.Close() })
			for _, id := range []tenant.ID{mqtest.Acme, mqtest.Globex} {
				require.NoError(t, e.SetMaxBytes(t.Context(), id, 64<<20))
			}
			return e
		},
		DeleteIngestDurable: func(t *testing.T, b mq.Broker, durable string) {
			require.NoError(t, mq.DeleteDurable(t.Context(), b.(*mq.EmbeddedNATS), durable))
		},
		// A tiny budget, then publishes until the tenant's own stream refuses
		// even the smallest event, so no later one fits.
		Fill: func(t *testing.T, b mq.Broker, id tenant.ID) {
			require.NoError(t, b.SetMaxBytes(t.Context(), id, 4<<10))
			for _, size := range []int{1 << 10, 1} {
				payload := make([]byte, size)
				for i := 0; ; i++ {
					require.Less(t, i, 1<<10, "the queue never filled")
					err := b.Publish(t.Context(), mq.Topic{Tenant: id, Table: "f"}, payload)
					if err != nil {
						require.ErrorIs(t, err, mq.ErrQueueFull)
						break
					}
				}
			}
		},
		Caps: mqtest.Caps{
			PerTenantBudget:    true,
			PurgesAcked:        true,
			UnbudgetedNotFound: true,
			ConfiguresDurables: true,
		},
	})
}
