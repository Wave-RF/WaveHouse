package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// coordNATSConfig is natsConfig with the leases in the shipped bucket, for a
// sweeper-only process named id.
func coordNATSConfig(t *testing.T, url, id string) *config.Config {
	t.Helper()
	cfg := natsConfig(t, url)
	cfg.Coord = config.Coord{Backend: config.CoordNATS}
	cfg.Roles = []config.Role{config.RoleSweeper}
	cfg.InstanceID = id
	return cfg
}

// The lease bucket is the operator's: boot waits for it with the rest of the
// topology and then refuses, naming it.
func TestNew_CoordNATSMissingBucket(t *testing.T) {
	srv := natstest.Start(t)
	require.NoError(t, srv.Operator.DeleteBucket(t.Context(), natstest.CoordBucket))
	guardGlobals(t)
	cfg := coordNATSConfig(t, srv.URL(), "a")
	cfg.MQ.NATS.TopologyWait = 300 * time.Millisecond
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrTopology)
	assert.ErrorContains(t, err, "kv bucket wh_coord")
}
