package mq

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// The external-NATS fixture: an in-process server listening on TCP whose
// accounts, users and permissions are the shipped Helm values' config.merge
// block, verbatim, and whose streams and consumers are the shipped nack
// manifests. So the tests exercise what an operator deploys, not a copy of it.

// fixturePassword is the password every fixture user gets in place of the
// Helm values' secret reference.
func fixturePassword(user string) string { return natstest.Password(user) }

type natsFixture struct {
	server *natsserver.Server
	// opts is what server was started with, for a restart on the same port
	// and store.
	opts *natsserver.Options
	// admin is nack's stand-in: the operator's user, with full access.
	admin jetstream.JetStream
}

// newNATSFixture starts a server configured from the shipped Helm values,
// shut down by the test framework.
func newNATSFixture(t *testing.T) *natsFixture {
	t.Helper()
	dir := t.TempDir()
	conf, err := natstest.ServerConfig(natstest.ShippedValues(), dir)
	require.NoError(t, err)
	confPath := filepath.Join(dir, "nats.conf")
	require.NoError(t, os.WriteFile(confPath, conf, 0o600))
	opts, err := natsserver.ProcessConfigFile(confPath)
	require.NoError(t, err)
	opts.Host, opts.Port, opts.NoSigs, opts.NoLog = "127.0.0.1", -1, true, true

	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "nats server not ready")
	t.Cleanup(s.Shutdown)
	f := &natsFixture{server: s, opts: opts}
	f.admin = f.connect(t, natstest.OperatorUser)
	return f
}

// connect opens a JetStream context as user, closed by the test framework.
// The wavehouse user gets the inbox prefix its permissions allow.
func (f *natsFixture) connect(t *testing.T, user string, opts ...nats.Option) jetstream.JetStream {
	t.Helper()
	opts = append([]nats.Option{nats.UserInfo(user, fixturePassword(user))}, opts...)
	if user == "wavehouse" {
		opts = append(opts, nats.CustomInboxPrefix(natsInboxPrefix(DefaultNATSSubjectPrefix)))
	}
	nc, err := nats.Connect(f.server.ClientURL(), opts...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

// fixtureTopology is a set of stream and consumer configs to create, in
// order: each stream, then its consumers.
type fixtureTopology struct{ *natstest.Manifests }

// shippedTopology is the shipped manifests (N=4) at one replica, which is
// all a single server can hold.
func shippedTopology(t *testing.T) *fixtureTopology {
	t.Helper()
	tp := loadNATSManifests(t, natstest.ShippedManifests())
	tp.SingleReplica()
	return tp
}

// loadNATSManifests parses nack Stream and Consumer resources into the
// JetStream configs nack would create from them.
func loadNATSManifests(t *testing.T, path string) *fixtureTopology {
	t.Helper()
	m, err := natstest.LoadManifests(path)
	require.NoError(t, err)
	return &fixtureTopology{m}
}

// stream is the named stream's config, to mutate before apply.
func (tp *fixtureTopology) stream(t *testing.T, name string) *jetstream.StreamConfig {
	t.Helper()
	i := slices.IndexFunc(tp.Streams, func(s jetstream.StreamConfig) bool { return s.Name == name })
	require.GreaterOrEqual(t, i, 0, "no stream %s in the fixture", name)
	return &tp.Streams[i]
}

// consumer is the one consumer on the named stream, to mutate before apply.
func (tp *fixtureTopology) consumer(t *testing.T, stream string) *jetstream.ConsumerConfig {
	t.Helper()
	require.Len(t, tp.Consumers[stream], 1, "consumers on %s", stream)
	return &tp.Consumers[stream][0]
}

// drop removes the named stream and its consumers.
func (tp *fixtureTopology) drop(name string) {
	tp.Streams = slices.DeleteFunc(tp.Streams, func(s jetstream.StreamConfig) bool { return s.Name == name })
	delete(tp.Consumers, name)
}

// apply creates tp as the operator would, and waits for every history source
// to attach: a row acked on a partition before its source exists never
// reaches the history.
func (f *natsFixture) apply(t *testing.T, tp *fixtureTopology) {
	t.Helper()
	require.NoError(t, f.create(t.Context(), tp))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, tp.AwaitSources(ctx, f.admin))
}

// reset deletes every stream, and with them their consumers.
func (f *natsFixture) reset(t *testing.T) {
	t.Helper()
	names := f.admin.StreamNames(t.Context())
	var all []string
	for name := range names.Name() {
		all = append(all, name)
	}
	require.NoError(t, names.Err())
	for _, name := range all {
		require.NoError(t, f.admin.DeleteStream(t.Context(), name))
	}
}

// create creates tp's streams and consumers without waiting for anything, so
// a goroutine can call it.
func (f *natsFixture) create(ctx context.Context, tp *fixtureTopology) error {
	return tp.Create(ctx, f.admin)
}
