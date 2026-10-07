//go:build integration

// Package natsspike pins the nats-server behavior the external-NATS topology
// rests on (#613). It runs in `make test-integration` (seconds per test, more
// than the unit suite's per-package timeout spares), and lives under
// internal/mq because only internal/mq may import NATS.
package natsspike

import (
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin why an ingest partition has work-queue
// retention and feeds the history by republish: a row no consumer covers is
// kept, not dropped; the server refuses a second consumer on a subject; and a
// history that is gone or full never holds up ingest.

// server runs an in-process JetStream server on a random TCP port, shut
// down by the test framework.
func server(t *testing.T) *natsserver.Server {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	})
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "server not ready")
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s
}

func connect(t *testing.T, s *natsserver.Server) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

func stream(t *testing.T, js jetstream.JetStream, name string) jetstream.Stream {
	t.Helper()
	s, err := js.Stream(t.Context(), name)
	require.NoError(t, err)
	return s
}

func msgs(t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()
	info, err := stream(t, js, name).Info(t.Context())
	require.NoError(t, err)
	return info.State.Msgs
}

func subjectMsgs(t *testing.T, js jetstream.JetStream, name, filter string) uint64 {
	t.Helper()
	info, err := stream(t, js, name).Info(t.Context(), jetstream.WithSubjectFilter(filter))
	require.NoError(t, err)
	var n uint64
	for _, c := range info.State.Subjects {
		n += c
	}
	return n
}

func publish(t *testing.T, js jetstream.JetStream, subject string, n int) {
	t.Helper()
	for i := range n {
		_, err := js.Publish(t.Context(), subject, fmt.Appendf(nil, "%d", i))
		require.NoError(t, err, "publish %s", subject)
	}
}

func durable(t *testing.T, js jetstream.JetStream, streamName, name string, filters ...string) error {
	t.Helper()
	_, err := js.CreateOrUpdateConsumer(t.Context(), streamName, jetstream.ConsumerConfig{
		Durable: name, AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: -1, FilterSubjects: filters,
	})
	return err
}

// drain fetches and acks every row the durable delivers until a pull comes
// back empty.
func drain(t *testing.T, js jetstream.JetStream, streamName, name string) int {
	t.Helper()
	c, err := js.Consumer(t.Context(), streamName, name)
	require.NoError(t, err)
	n := 0
	for {
		b, err := c.Fetch(100, jetstream.FetchMaxWait(300*time.Millisecond))
		require.NoError(t, err)
		got := 0
		for m := range b.Messages() {
			require.NoError(t, m.DoubleAck(t.Context()))
			got++
		}
		n += got
		if got == 0 {
			return n
		}
	}
}

// A row whose subject no consumer covers: interest retention acks the publish
// and stores nothing; a work queue keeps it for a consumer to come.
func TestS2_WorkQueueKeepsARowNoConsumerCovers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		retention jetstream.RetentionPolicy
		kept      uint64
	}{{jetstream.InterestPolicy, 0}, {jetstream.WorkQueuePolicy, 5}} {
		t.Run(tc.retention.String(), func(t *testing.T) {
			t.Parallel()
			js := connect(t, server(t))
			_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "IN", Subjects: []string{"wh.ingest.0.>"}, Retention: tc.retention})
			require.NoError(t, err)
			require.NoError(t, durable(t, js, "IN", "x", "wh.ingest.0.acme.t1"))
			publish(t, js, "wh.ingest.0.acme.t2", 5) // acked either way
			assert.Equal(t, tc.kept, subjectMsgs(t, js, "IN", "wh.ingest.0.acme.t2"))
		})
	}
}

// A work queue refuses a second consumer whose filter overlaps one it has,
// created or updated into the overlap, and a catch-all beside a filtered one;
// a consumer on a disjoint subject is allowed.
func TestS2_WorkQueueRefusesOverlap(t *testing.T) {
	t.Parallel()
	js := connect(t, server(t))
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "IN", Subjects: []string{"wh.ingest.0.>"}, Retention: jetstream.WorkQueuePolicy})
	require.NoError(t, err)
	require.NoError(t, durable(t, js, "IN", "s0", "wh.ingest.0.0.>"))
	var apiErr *jetstream.APIError
	require.ErrorAs(t, durable(t, js, "IN", "other", "wh.ingest.0.0.>"), &apiErr, "the same subject")
	assert.Equal(t, jetstream.ErrorCode(10100), apiErr.ErrorCode)
	require.Error(t, durable(t, js, "IN", "all", "wh.ingest.0.>"), "a catch-all")
	require.NoError(t, durable(t, js, "IN", "s1", "wh.ingest.0.1.>"), "a disjoint subject")
	require.Error(t, durable(t, js, "IN", "s0", "wh.ingest.0.0.>", "wh.ingest.0.1.>"), "an update into the overlap")
}

// republishSetup is one work-queue partition of 200 rows republishing to a
// history, with a durable on the partition.
func republishSetup(t *testing.T, history jetstream.StreamConfig) jetstream.JetStream {
	t.Helper()
	js := connect(t, server(t))
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "WH_INGEST_0", Subjects: []string{"wh.ingest.0.>"}, Retention: jetstream.WorkQueuePolicy,
		MaxMsgs: 200, Discard: jetstream.DiscardNew,
		RePublish: &jetstream.RePublish{Source: "wh.ingest.0.>", Destination: "wh.hist.>"},
	})
	require.NoError(t, err)
	require.NoError(t, durable(t, js, "WH_INGEST_0", "wh-ingest", "wh.ingest.0.>"))
	history.Name, history.Subjects = "WH_HISTORY", []string{"wh.hist.>"}
	_, err = js.CreateStream(t.Context(), history)
	require.NoError(t, err)
	return js
}

// The history gets every row the partition stores, under the subject with
// the partition token dropped, and the ack empties the partition only.
func TestS2_RepublishFeedsTheHistory(t *testing.T) {
	t.Parallel()
	js := republishSetup(t, jetstream.StreamConfig{MaxAge: time.Hour})
	publish(t, js, "wh.ingest.0.acme.orders", 30)
	publish(t, js, "wh.ingest.0.acme.orders.scope1", 20)
	assert.Equal(t, 50, drain(t, js, "WH_INGEST_0", "wh-ingest"))
	assert.Zero(t, msgs(t, js, "WH_INGEST_0"), "acked rows leave the partition at once")
	require.Eventually(t, func() bool { return msgs(t, js, "WH_HISTORY") == 50 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, uint64(30), subjectMsgs(t, js, "WH_HISTORY", "wh.hist.acme.orders"))
	assert.Equal(t, uint64(20), subjectMsgs(t, js, "WH_HISTORY", "wh.hist.acme.orders.scope1"))
}

// A history that is deleted, or full and refusing, never holds up ingest:
// every publish is stored and every acked row leaves the partition.
func TestS2_HistoryOutageNeverHoldsIngest(t *testing.T) {
	t.Parallel()
	t.Run("deleted", func(t *testing.T) {
		t.Parallel()
		js := republishSetup(t, jetstream.StreamConfig{MaxAge: time.Hour})
		require.NoError(t, js.DeleteStream(t.Context(), "WH_HISTORY"))
		publish(t, js, "wh.ingest.0.acme.t1", 50)
		assert.Equal(t, 50, drain(t, js, "WH_INGEST_0", "wh-ingest"))
		assert.Zero(t, msgs(t, js, "WH_INGEST_0"))
	})
	t.Run("full", func(t *testing.T) {
		t.Parallel()
		js := republishSetup(t, jetstream.StreamConfig{MaxMsgs: 10, Discard: jetstream.DiscardNew})
		for range 20 { // 400 rows through a 200-row partition, beside a 10-row history
			publish(t, js, "wh.ingest.0.acme.t1", 20)
			drain(t, js, "WH_INGEST_0", "wh-ingest")
		}
		assert.Zero(t, msgs(t, js, "WH_INGEST_0"))
		assert.Equal(t, uint64(10), msgs(t, js, "WH_HISTORY"))
	})
}

// The republished copy carries the publish's headers. A publish that expects
// its stream by name therefore reaches the history expecting the partition,
// and the history refuses it: the reason ExternalNATS publishes ingest rows
// without that expectation and checks the ack's stream instead.
func TestS2_RepublishCarriesTheExpectedStream(t *testing.T) {
	t.Parallel()
	js := republishSetup(t, jetstream.StreamConfig{MaxAge: time.Hour})
	_, err := js.Publish(t.Context(), "wh.ingest.0.acme.a", []byte("x"), jetstream.WithExpectStream("WH_INGEST_0"))
	require.NoError(t, err)
	ack, err := js.Publish(t.Context(), "wh.ingest.0.acme.b", []byte("x"), jetstream.WithMsgID("id-1"))
	require.NoError(t, err)
	assert.Equal(t, "WH_INGEST_0", ack.Stream, "the ack names the stream that stored it")
	require.Eventually(t, func() bool { return msgs(t, js, "WH_HISTORY") == 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	assert.Zero(t, subjectMsgs(t, js, "WH_HISTORY", "wh.hist.acme.a"), "the copy expecting WH_INGEST_0 is refused")
	m, err := stream(t, js, "WH_HISTORY").GetLastMsgForSubject(t.Context(), "wh.hist.acme.b")
	require.NoError(t, err)
	assert.Equal(t, "id-1", m.Header.Get(jetstream.MsgIDHeader), "the copy keeps the publish's headers")
	assert.Equal(t, "WH_INGEST_0", m.Header.Get("Nats-Stream"))
}

// A republish destination the stream's own subjects would capture is a
// cycle, refused at create.
func TestS2_RepublishRefusesACycle(t *testing.T) {
	t.Parallel()
	js := connect(t, server(t))
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "BAD", Subjects: []string{"x.>"}, RePublish: &jetstream.RePublish{Source: "x.>", Destination: "x.y.>"},
	})
	require.Error(t, err)
}

// A work-queue partition keeps its last row's timestamp after the row is
// acked away, which the history gauge compares against.
func TestS2_WorkQueueKeepsTheLastTimestamp(t *testing.T) {
	t.Parallel()
	js := republishSetup(t, jetstream.StreamConfig{MaxAge: time.Hour})
	publish(t, js, "wh.ingest.0.acme.t1", 3)
	require.Equal(t, 3, drain(t, js, "WH_INGEST_0", "wh-ingest"))
	info, err := stream(t, js, "WH_INGEST_0").Info(t.Context())
	require.NoError(t, err)
	assert.Zero(t, info.State.Msgs)
	assert.WithinDuration(t, time.Now(), info.State.LastTime, 10*time.Second)
}
