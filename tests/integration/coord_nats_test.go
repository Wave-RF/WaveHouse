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

// Two full replicas on one NATS, through the shipped lease bucket as the
// restricted wavehouse user, take no sweeper lease: under nats retention is
// the operator's streams', so no sweeper is wired.
func TestCoordNATS_NoSweeperUnderNATS(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	natsURL := startNATS(t)
	op, err := natstest.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(op.Close)
	require.NoError(t, op.ApplyShipped(ctx))
	root, err := writeTestSettings(e.ch)
	require.NoError(t, err)

	for range 2 {
		bootNATSProcess(t, natsURL, root, config.AllRoles()...)
	}
	// A sweeper would campaign within coord.RetryPeriod (2s) of boot.
	assert.Never(t, func() bool {
		h, err := op.LeaseHolder(ctx, natstest.CoordBucket, "sweeper")
		require.NoError(t, err)
		return h != ""
	}, 5*time.Second, 100*time.Millisecond, "no sweeper lease is taken under nats")
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
	cfg := &config.Config{
		DataDir: t.TempDir(),
		Server:  config.Server{Port: 1, ShutdownTimeout: 1},
		MQ: config.MQ{Backend: config.MQNATS, NATS: config.MQNATSConfig{
			URLs: []string{srv.URL()}, User: natstest.WaveHouseUser, PasswordFile: pw,
			SubjectPrefix: "wh", Partitions: 4, IngestConsumer: "wh-ingest",
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
