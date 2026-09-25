//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// Two full replicas on one NATS elect exactly one sweeper between them
// through the shipped lease bucket, as the restricted wavehouse user, and the
// lease moves to the other replica when the holder stops.
func TestCoordNATS_OneSweeperAcrossReplicas(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	natsURL := startNATS(t)
	op, err := natstest.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(op.Close)
	require.NoError(t, op.ApplyShipped(ctx))
	root, err := writeTestSettings(e.ch)
	require.NoError(t, err)

	replicas := map[string]*natsProcess{}
	for range 2 {
		p := bootNATSProcess(t, natsURL, root, config.AllRoles()...)
		replicas[p.id] = p
	}
	holder := func() string {
		h, err := op.LeaseHolder(ctx, natstest.CoordBucket, "sweeper")
		require.NoError(t, err)
		return h
	}
	require.Eventually(t, func() bool { return replicas[holder()] != nil }, 10*time.Second, 50*time.Millisecond, "one replica is elected")
	first := holder()
	// The other campaigns every 2s (coord.RetryPeriod) and must not win.
	time.Sleep(5 * time.Second)
	require.Equal(t, first, holder(), "the elected sweeper keeps its lease while it runs")

	replicas[first].stop()
	select {
	case err := <-replicas[first].runDone:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("the holder did not stop")
	}
	require.Eventually(t, func() bool { h := holder(); return h != first && replicas[h] != nil }, 10*time.Second, 50*time.Millisecond,
		"the lease moves to the other replica once the holder stops")
	assert.NotEqual(t, first, holder())
}
