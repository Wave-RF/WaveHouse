package mq

import (
	"context"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startListeningNATS boots a real TCP-listening NATS server with JetStream so
// RemoteNATS can dial a URL. Returns the server URL and a cleanup hook.
func startListeningNATS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server failed to start")
	}
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

func TestRemoteNATS_PublishSubscribe(t *testing.T) {
	url := startListeningNATS(t)

	r, err := NewRemote(url, StreamName(), 64<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var mu sync.Mutex
	var payload []byte
	done := make(chan struct{}, 1)

	err = r.Subscribe(ctx, "ingest.remote.x", "remote-consumer", func(msg *Message) error {
		mu.Lock()
		payload = msg.Data
		mu.Unlock()
		msg.Ack()
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, r.Publish(ctx, "ingest.remote.x", []byte("world")))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for remote subscriber callback")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []byte("world"), payload)
}

func TestRemoteNATS_Accessors(t *testing.T) {
	url := startListeningNATS(t)
	r, err := NewRemote(url, StreamName(), 64<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	assert.NotNil(t, r.JetStream())
	assert.NotNil(t, r.NatsConn())
	assert.True(t, r.NatsConn().IsConnected())
}

func TestRemoteNATS_BadURL(t *testing.T) {
	// Connecting to an obviously wrong URL should return an error rather than
	// hanging or panicking.
	_, err := NewRemote("nats://127.0.0.1:1", StreamName(), 1<<20)
	require.Error(t, err)
}
