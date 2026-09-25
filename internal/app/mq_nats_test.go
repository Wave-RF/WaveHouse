package app

import (
	"context"
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
		SubjectPrefix: "wh", Partitions: 4, IngestConsumer: "wh-ingest",
		ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: 10 * time.Second,
	}}
	return cfg
}

// mq.backend: nats wires the external broker in place of the embedded one,
// and a role split that the embedded MQ cannot serve boots on it. The worker
// consumes the operator's durable; the operator deleting it ends the worker,
// and with it Run, naming the component.
func TestNew_NATSBackend(t *testing.T) {
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
		c, err := srv.Operator.JetStream().Consumer(ctx, "WH_INGEST_0", "wh-ingest")
		return err == nil && c.CachedInfo().NumWaiting > 0
	}, 10*time.Second, 20*time.Millisecond, "the ingest worker pulls from the operator's durable")
	require.NoError(t, srv.Operator.DeleteDurable(ctx, "wh-ingest"))
	err = <-done
	require.ErrorIs(t, err, mq.ErrDeliveryEnded)
	assert.True(t, strings.HasPrefix(err.Error(), "ingest worker: "), "the failing component names itself: %v", err)
}

// A cluster never reached within topology_wait refuses boot as unavailable.
func TestNew_NATSUnreachable(t *testing.T) {
	guardGlobals(t)
	cfg := natsConfig(t, "nats://"+closedAddr(t))
	cfg.MQ.NATS.TopologyWait = 300 * time.Millisecond
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrUnavailable)
	assert.ErrorContains(t, err, "mq open")
}

// The operator's topology missing a piece refuses boot with the finding.
func TestNew_NATSTopologyMissing(t *testing.T) {
	srv := natstest.Start(t)
	require.NoError(t, srv.Operator.JetStream().DeleteStream(t.Context(), "WH_DLQ"))
	guardGlobals(t)
	cfg := natsConfig(t, srv.URL())
	cfg.MQ.NATS.TopologyWait = 300 * time.Millisecond
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorIs(t, err, mq.ErrTopology)
	assert.ErrorContains(t, err, "dead-letter stream")
}
