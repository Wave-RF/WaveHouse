// The embedded broker's run lives here rather than in internal/mq so it is a
// test binary of its own, clear of that package's 15s budget.
package mqtest_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/mqtest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedNATS_Conformance(t *testing.T) {
	mqtest.Run(t, mqtest.Harness{
		New: func(t *testing.T) mq.Broker {
			e, err := mq.NewEmbedded(storeDir(t))
			require.NoError(t, err)
			t.Cleanup(func() { _ = e.Close() })
			for _, id := range []tenant.ID{mqtest.Acme, mqtest.Globex} {
				require.NoError(t, e.SetMaxBytes(t.Context(), id, 64<<20))
			}
			return e
		},
		// Closing the broker ends every tenant's delivery at once, the
		// connection-closed half of the #587 path; internal/mq's own tests
		// delete the durable, one tenant's queue and then another's.
		EndDelivery: func(t *testing.T, b mq.Broker) {
			require.NoError(t, b.Close())
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

// storeDir is a temporary store directory whose removal retries briefly: under
// parallel load a consumer's state file can land after Close has returned,
// which fails t.TempDir's one-shot RemoveAll. The retrying cleanup runs first
// (cleanups are LIFO), leaving t.TempDir an empty directory to remove.
func storeDir(t *testing.T) string {
	dir := filepath.Join(t.TempDir(), "store")
	var err error
	t.Cleanup(func() {
		for range 50 {
			if err = os.RemoveAll(dir); err == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("remove %s: %v", dir, err)
	})
	return dir
}
