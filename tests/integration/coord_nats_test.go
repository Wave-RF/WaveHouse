//go:build integration

package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// Two full replicas on one NATS elect exactly one sweeper between them
// through the shipped lease bucket, as the restricted wavehouse user, and the
// lease moves to the other replica when the holder stops.
func TestCoordNATS_OneSweeperAcrossReplicas(t *testing.T) {
	t.Parallel()
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

// The lease bucket is the operator's: boot waits for it with the rest of the
// topology and then refuses, naming it.
func TestCoordNATS_MissingBucketRefusesBoot(t *testing.T) {
	t.Parallel()
	srv := natstest.Start(t)
	require.NoError(t, srv.Operator.DeleteBucket(t.Context(), natstest.CoordBucket))
	pw := filepath.Join(t.TempDir(), "nats-password")
	require.NoError(t, os.WriteFile(pw, []byte(natstest.Password(natstest.WaveHouseUser)), 0o600))
	root, err := writeTestSettings(env(t).ch)
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir: t.TempDir(),
		Server:  config.Server{Port: 1, ShutdownTimeout: 1},
		MQ: config.MQ{Backend: config.MQNATS, NATS: config.MQNATSConfig{
			URLs: []string{srv.URL()}, User: natstest.WaveHouseUser, PasswordFile: pw,
			SubjectPrefix: "wh", Partitions: 4, IngestConsumer: "wh-ingest",
			ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: 300 * time.Millisecond,
		}},
		Cache:      config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:     config.Dedupe{Backend: config.DedupePebble},
		Coord:      config.Coord{Backend: config.CoordNATS},
		Roles:      []config.Role{config.RoleSweeper},
		InstanceID: "boot",
		Settings:   config.Settings{Dir: root},
	}
	require.NoError(t, cfg.Validate())
	_, err = app.New(t.Context(), app.Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrTopology)
	assert.ErrorContains(t, err, "kv bucket wh_coord")
}
