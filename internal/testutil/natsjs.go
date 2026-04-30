package testutil

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// NewJetStream starts an in-process NATS server with JetStream and returns a
// connected jetstream.JetStream handle. Cleanup is registered on t.
//
// Lives in testutil so packages that need KV-backed tests (pipes.Store,
// policy.Store) get one shared embedded-server setup instead of each
// duplicating the natsserver / nats.Connect / jetstream.New dance.
func NewJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()

	opts := &natsserver.Options{
		DontListen: true,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS failed to start")
	}

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	require.NoError(t, err)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})
	return js
}
