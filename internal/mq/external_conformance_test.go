//go:build integration

// The external broker's tests run in `make test-integration`, not the unit
// suite: each starts a NATS server with the shipped topology, which the
// internal/mq unit binary's 15s budget cannot absorb (#617).

package mq_test

import (
	"sync"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/mqtest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/require"
)

func TestExternalNATS_Conformance(t *testing.T) {
	var (
		mu       sync.Mutex
		fixtures = map[mq.Broker]*mq.ExternalFixture{}
	)
	fixtureOf := func(b mq.Broker) *mq.ExternalFixture {
		mu.Lock()
		defer mu.Unlock()
		return fixtures[b]
	}
	mqtest.Run(t, mqtest.Harness{
		New: func(t *testing.T) mq.Broker {
			f := mq.NewExternalFixture(t)
			b := f.Broker(t)
			for _, id := range []tenant.ID{mqtest.Acme, mqtest.Globex} {
				require.NoError(t, b.SetMaxBytes(t.Context(), id, 64<<20))
			}
			mu.Lock()
			fixtures[b] = f
			mu.Unlock()
			return b
		},
		// The operator deletes the durable on every partition.
		EndDelivery: func(t *testing.T, b mq.Broker) {
			fixtureOf(b).DeleteIngestDurable(t)
		},
		// The tenant's partition shrunk to a few KiB, then filled.
		Fill: func(t *testing.T, b mq.Broker, id tenant.ID) {
			fixtureOf(b).FillPartition(t, b, id)
		},
		Caps: mqtest.Caps{
			PerTenantBudget:    false,
			PurgesAcked:        false,
			UnbudgetedNotFound: false,
			ConfiguresDurables: false,
		},
	})
}
