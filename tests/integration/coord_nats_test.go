//go:build integration

package tests

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// Two full replicas on one NATS, through the shipped lease bucket as the
// restricted wavehouse user, each hold a shard claim membership lease, and
// take no sweeper lease: under nats retention is the operator's streams', so
// no sweeper is wired. When one stops, its membership lease is resigned.
func TestCoordNATS_ReplicasShareTheShards(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	natsURL := startNATS(t)
	op, err := natstest.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(op.Close)
	require.NoError(t, op.ApplyShipped(ctx))
	root, err := writeTestSettings(e.ch)
	require.NoError(t, err)
	t.Cleanup(func() { removeTestSettings(root) })

	replicas := map[string]*natsProcess{}
	for range 2 {
		p := bootNATSProcess(t, natsURL, root, config.AllRoles()...)
		replicas[p.id] = p
	}
	members := func() map[string]bool {
		out := map[string]bool{}
		for j := range 32 {
			h, err := op.LeaseHolder(ctx, natstest.CoordBucket, "ingest.m"+strconv.Itoa(j))
			require.NoError(t, err)
			if h != "" {
				out[h] = true
			}
		}
		return out
	}
	require.Eventually(t, func() bool { m := members(); return len(m) == 2 }, 15*time.Second, 50*time.Millisecond, "each replica is a member: %v", members())
	for id := range members() {
		assert.Contains(t, replicas, id)
	}
	sweeper, err := op.LeaseHolder(ctx, natstest.CoordBucket, "sweeper")
	require.NoError(t, err)
	assert.Empty(t, sweeper, "no sweeper runs under nats")

	var first string
	for id := range replicas {
		first = id
		break
	}
	replicas[first].stop()
	select {
	case err := <-replicas[first].runDone:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("the replica did not stop")
	}
	require.Eventually(t, func() bool { m := members(); return len(m) == 1 && !m[first] }, 10*time.Second, 50*time.Millisecond,
		"the stopped replica resigned its membership: %v", members())
}

// The lease bucket is the operator's: boot waits for it with the rest of the
// topology and then refuses, naming it.
func TestCoordNATS_MissingBucketRefusesBoot(t *testing.T) {
	srv := natstest.Start(t)
	require.NoError(t, srv.Operator.DeleteBucket(t.Context(), natstest.CoordBucket))
	pw := filepath.Join(t.TempDir(), "nats-password")
	require.NoError(t, os.WriteFile(pw, []byte(natstest.Password(natstest.WaveHouseUser)), 0o600))
	root, err := writeTestSettings(env(t).ch)
	require.NoError(t, err)
	t.Cleanup(func() { removeTestSettings(root) })
	cfg := &config.Config{
		DataDir: t.TempDir(),
		Server:  config.Server{Port: 1, ShutdownTimeout: 1},
		MQ: config.MQ{Backend: config.MQNATS, NATS: config.MQNATSConfig{
			URLs: []string{srv.URL()}, User: natstest.WaveHouseUser, PasswordFile: pw,
			SubjectPrefix: "wh", Partitions: 4, Shards: 8, IngestConsumer: "wh-ingest",
			ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: 300 * time.Millisecond,
		}},
		Cache:      config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:     config.Dedupe{Backend: config.DedupePebble, Lease: 30 * time.Second, ReserveConcurrency: 64},
		Coord:      config.Coord{Backend: config.CoordNATS},
		Roles:      config.AllRoles(),
		InstanceID: "boot",
		Settings:   config.Settings{Dir: root},
	}
	require.NoError(t, cfg.Validate())
	_, err = app.New(t.Context(), app.Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrTopology)
	assert.ErrorContains(t, err, "kv bucket wh_coord")
}
