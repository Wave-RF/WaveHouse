package app

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// natsConfig is testConfig on mq.backend: nats against url, connected as the
// shipped wavehouse user, the block otherwise as config.Load leaves it.
func natsConfig(t *testing.T, url string) *config.Config {
	t.Helper()
	pw := filepath.Join(t.TempDir(), "nats-password")
	require.NoError(t, os.WriteFile(pw, []byte(natstest.Password(natstest.WaveHouseUser)+"\n"), 0o600))
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.MQ = config.MQ{Backend: config.MQNATS, NATS: config.MQNATSConfig{
		URLs: []string{url}, User: natstest.WaveHouseUser, PasswordFile: pw,
		SubjectPrefix: "wh", Partitions: 4, Shards: 8, IngestConsumer: "wh-ingest",
		ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: 10 * time.Second,
	}}
	return cfg
}

// mq.backend: nats wires the external broker in place of the embedded one,
// and a role split that the embedded MQ cannot serve boots on it. The worker
// consumes the operator's durable; the operator deleting it ends the worker,
// and with it Run, naming the component.
func TestNew_NATSBackend(t *testing.T) {
	t.Parallel()
	srv := natstest.Start(t)
	cfg := natsConfig(t, srv.URL())
	cfg.Roles = []config.Role{config.RoleAPI, config.RoleIngest}
	a := newApp(t, cfg, Options{})

	_, ok := a.MQ().(*mq.ExternalNATS)
	require.True(t, ok, "mq.backend: nats wires mq.ExternalNATS, got %T", a.MQ())
	assert.Equal(t, []string{
		"clickhouse", "schema discovery", "dedupe", "mq", "cache", "coord",
		"hub bridge", "keepalive", "ingest worker",
		"auth", "sighup", "settings watcher", "http server",
	}, componentNames(a))
	_, err := os.Stat(filepath.Join(cfg.DataDir, "nats"))
	assert.True(t, os.IsNotExist(err), "nothing is kept under data_dir/nats")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	// Once the worker has bound it, the durable has a pull waiting.
	require.Eventually(t, func() bool {
		c, err := srv.Operator.JetStream().Consumer(ctx, "WH_INGEST_0", "wh-ingest-0")
		return err == nil && c.CachedInfo().NumWaiting > 0
	}, 10*time.Second, 20*time.Millisecond, "the ingest worker pulls from the operator's durable")
	require.NoError(t, srv.Operator.DeleteIngestDurables(ctx))
	err = <-done
	require.ErrorIs(t, err, mq.ErrDeliveryEnded)
	assert.True(t, strings.HasPrefix(err.Error(), "ingest worker: "), "the failing component names itself: %v", err)
}

// Every role under mq.backend: nats wires no sweeper (retention is the
// operator's stream policy there), and a tenant's gap window is checked
// against what the history keeps instead, at boot and after a reload.
func TestNew_NATSWiresNoSweeper(t *testing.T) { //nolint:paralleltest // captures the default logger
	srv := natstest.Start(t)
	cfg := natsConfig(t, srv.URL())
	cfg.Roles = config.AllRoles()
	cfg.Coord.Backend = config.CoordNATS
	day := map[string]any{"keepalive_interval": 30, "keepalive_buckets": 3, "gap_window_minutes": 24 * 60}
	rewriteSettings(t, cfg.Settings.Dir, map[string]any{"stream": day})
	a := newApp(t, cfg, Options{})
	assert.NotContains(t, componentNames(a), "sweeper")

	// Captured after New: the boot check already warned for this window, so
	// reloading it again warns nobody: only a changed window does.
	logs := logtest.Capture(t, slog.LevelWarn)
	warned := func() int { return strings.Count(logs.String(), "keeps less than this tenant's gap window") }
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Zero(t, warned(), "boot checked a day's window against the shipped history already")

	day["gap_window_minutes"] = 2 * 24 * 60
	rewriteSettings(t, cfg.Settings.Dir, map[string]any{"stream": day})
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, 1, warned(), "a reload checks the new window")
}

// A cluster never reached within topology_wait refuses boot as unavailable.
func TestNew_NATSUnreachable(t *testing.T) {
	t.Parallel()
	cfg := natsConfig(t, "nats://"+closedAddr(t))
	cfg.MQ.NATS.TopologyWait = time.Millisecond
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrUnavailable)
	assert.ErrorContains(t, err, "mq open")
}

// The operator's topology missing a piece refuses boot with the finding.
func TestNew_NATSTopologyMissing(t *testing.T) {
	t.Parallel()
	srv := natstest.Start(t)
	require.NoError(t, srv.Operator.JetStream().DeleteStream(t.Context(), "WH_DLQ"))
	cfg := natsConfig(t, srv.URL())
	cfg.MQ.NATS.TopologyWait = time.Millisecond
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrTopology)
	assert.ErrorContains(t, err, "dead-letter stream")
}

// config bounds mq.nats.shards by internal/mq's own bound, which it cannot
// import.
func TestMQNATSShards_BoundMatchesMQ(t *testing.T) {
	t.Parallel()
	assert.Equal(t, mq.MaxNATSShards, config.MaxNATSShards)
}
