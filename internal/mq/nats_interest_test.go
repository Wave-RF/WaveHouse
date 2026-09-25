package mq

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin the JetStream semantics the external topology
// rests on (risk S1 of the external-NATS design): an interest-retention
// partition stream, a durable explicit-ack consumer on it, and a
// limits-retention history stream that sources the partition. The server
// builds the history's source consumer itself (ack-none), so whether it holds
// rows on the partition, and whether it copies them before the durable's ack
// deletes them, is the server's behaviour and not ours.

// s1Server runs an in-process JetStream server listening on a random TCP port
// over dir, shut down by the test framework.
func s1Server(t *testing.T, dir string) *natsserver.Server {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoSigs: true,
	})
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "server not ready")
	t.Cleanup(s.Shutdown)
	return s
}

func s1Connect(t *testing.T, s *natsserver.Server) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

// s1Topology creates one partition, its wh-ingest durable and a history stream
// sourcing it, as the shipped manifests do. partitionMax is the partition's
// max_bytes; historyAge the history's max_age.
func s1Topology(t *testing.T, js jetstream.JetStream, partitionMax int64, historyAge time.Duration) {
	t.Helper()
	ctx := t.Context()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:                 "WH_INGEST_0",
		Subjects:             []string{"wh.ingest.0.>"},
		Retention:            jetstream.InterestPolicy,
		Discard:              jetstream.DiscardNew,
		MaxBytes:             partitionMax,
		Storage:              jetstream.FileStorage,
		Duplicates:           2 * time.Minute,
		MaxMsgsPerSubject:    1_000_000,
		DiscardNewPerSubject: true,
		DenyPurge:            true,
		DenyDelete:           true,
	})
	require.NoError(t, err)
	_, err = js.CreateConsumer(ctx, "WH_INGEST_0", jetstream.ConsumerConfig{
		Durable:       "wh-ingest",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    -1,
		MaxAckPending: 10_000,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: "wh.ingest.0.>",
	})
	require.NoError(t, err)
	s1History(t, js, historyAge)
}

func s1History(t *testing.T, js jetstream.JetStream, maxAge time.Duration) {
	t.Helper()
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:      "WH_HISTORY",
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    maxAge,
		Storage:   jetstream.FileStorage,
		Sources:   []*jetstream.StreamSource{{Name: "WH_INGEST_0"}},
	})
	require.NoError(t, err)
	// The server builds the source consumer on the partition asynchronously; a
	// row wh-ingest acks before it exists never reaches the history.
	require.Eventually(t, func() bool { return s1Consumers(t, js) == 2 }, 10*time.Second, 10*time.Millisecond,
		"the history's source consumer never appeared on the partition")
}

// s1Consumers counts the consumers on the partition.
func s1Consumers(t *testing.T, js jetstream.JetStream) int {
	t.Helper()
	str, err := js.Stream(t.Context(), "WH_INGEST_0")
	require.NoError(t, err)
	n := 0
	for range str.ListConsumers(t.Context()).Info() {
		n++
	}
	return n
}

func s1Msgs(t *testing.T, js jetstream.JetStream, stream string) uint64 {
	t.Helper()
	s, err := js.Stream(t.Context(), stream)
	require.NoError(t, err)
	info, err := s.Info(t.Context())
	require.NoError(t, err)
	return info.State.Msgs
}

func s1Eventually(t *testing.T, js jetstream.JetStream, stream string, want uint64) {
	t.Helper()
	s1EventuallyWithin(t, js, stream, want, 10*time.Second)
}

func s1EventuallyWithin(t *testing.T, js jetstream.JetStream, stream string, want uint64, d time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool { return s1Msgs(t, js, stream) == want },
		d, 20*time.Millisecond, "%s never held %d messages (holds %d)", stream, want, s1Msgs(t, js, stream))
}

func s1Publish(t *testing.T, js jetstream.JetStream, subject string, n int, size int) {
	t.Helper()
	body := make([]byte, size)
	for i := range n {
		_, err := js.Publish(t.Context(), subject, body, jetstream.WithMsgID(fmt.Sprintf("%s-%d", subject, i)))
		require.NoError(t, err)
	}
}

// s1Pull fetches up to n messages from wh-ingest; ack decides, per subject,
// whether each is acknowledged.
func s1Pull(t *testing.T, js jetstream.JetStream, n int, ack func(subject string) bool) (acked, held int) {
	t.Helper()
	cons, err := js.Consumer(t.Context(), "WH_INGEST_0", "wh-ingest")
	require.NoError(t, err)
	for acked+held < n {
		batch, err := cons.Fetch(n-acked-held, jetstream.FetchMaxWait(2*time.Second))
		require.NoError(t, err)
		got := 0
		for m := range batch.Messages() {
			got++
			if ack(m.Subject()) {
				require.NoError(t, m.DoubleAck(t.Context()))
				acked++
			} else {
				held++
			}
		}
		require.NoError(t, batch.Error())
		require.NotZero(t, got, "wh-ingest delivered nothing")
	}
	return acked, held
}

func ackEvery(string) bool { return true }

// A row stays in the partition until wh-ingest acks it, however long after the
// history has copied it; the ack then deletes it from the partition and leaves
// the history's copy alone.
func TestS1_AckDeletesFromPartitionOnly(t *testing.T) {
	js := s1Connect(t, s1Server(t, t.TempDir()))
	s1Topology(t, js, 64<<20, time.Hour)

	s1Publish(t, js, "wh.ingest.0.acme.events", 100, 100)
	s1Eventually(t, js, "WH_HISTORY", 100)
	// The source consumer has delivered everything; its interest must not be
	// what keeps the rows. wh-ingest's is.
	time.Sleep(200 * time.Millisecond)
	assert.EqualValues(t, 100, s1Msgs(t, js, "WH_INGEST_0"), "rows left the partition before wh-ingest acked them")

	acked, _ := s1Pull(t, js, 100, ackEvery)
	require.Equal(t, 100, acked)
	s1Eventually(t, js, "WH_INGEST_0", 0)
	assert.EqualValues(t, 100, s1Msgs(t, js, "WH_HISTORY"))
}

// wh-ingest acking each row the moment it arrives, as fast as it can, never
// deletes a row the history has not copied yet.
func TestS1_FastAckNeverOutrunsHistory(t *testing.T) {
	js := s1Connect(t, s1Server(t, t.TempDir()))
	s1Topology(t, js, 256<<20, time.Hour)

	const n = 5000
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cons, err := js.Consumer(ctx, "WH_INGEST_0", "wh-ingest")
	require.NoError(t, err)
	cc, err := cons.Consume(func(m jetstream.Msg) { _ = m.Ack() })
	require.NoError(t, err)
	defer cc.Stop()

	s1Publish(t, js, "wh.ingest.0.acme.events", n, 64)
	s1Eventually(t, js, "WH_INGEST_0", 0)
	s1Eventually(t, js, "WH_HISTORY", n)
}

// A history created after rows were published, and before wh-ingest acked
// them, still receives them: the source consumer's interest starts at the
// partition's first row, not at the time it was created.
func TestS1_LateHistoryStillCopies(t *testing.T) {
	js := s1Connect(t, s1Server(t, t.TempDir()))
	ctx := t.Context()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "WH_INGEST_0", Subjects: []string{"wh.ingest.0.>"},
		Retention: jetstream.InterestPolicy, Discard: jetstream.DiscardNew,
		MaxBytes: 64 << 20, Storage: jetstream.FileStorage,
	})
	require.NoError(t, err)
	_, err = js.CreateConsumer(ctx, "WH_INGEST_0", jetstream.ConsumerConfig{
		Durable: "wh-ingest", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: -1,
		MaxAckPending: 10_000, DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	require.NoError(t, err)

	s1Publish(t, js, "wh.ingest.0.acme.events", 50, 64)
	s1History(t, js, time.Hour)
	s1Eventually(t, js, "WH_HISTORY", 50)
	acked, _ := s1Pull(t, js, 50, ackEvery)
	require.Equal(t, 50, acked)
	s1Eventually(t, js, "WH_INGEST_0", 0)
	assert.EqualValues(t, 50, s1Msgs(t, js, "WH_HISTORY"))
}

// With tenant X's rows unacked, tenant Y's acked rows are deleted one by one
// (not held behind X's at a shared ack floor), and the partition's max_bytes
// headroom comes back, so a full partition takes publishes again.
func TestS1_UnackedTenantDoesNotHoldOthers(t *testing.T) {
	js := s1Connect(t, s1Server(t, t.TempDir()))
	const size = 1024
	s1Topology(t, js, 256<<10, time.Hour) // ~256 KiB

	// Interleave X and Y so every Y row sits above an unacked X row.
	body := make([]byte, size)
	x, y := 0, 0
	for i := 0; ; i++ {
		subject := "wh.ingest.0.y.events"
		if i%2 == 0 {
			subject = "wh.ingest.0.x.events"
		}
		_, err := js.Publish(t.Context(), subject, body, jetstream.WithMsgID(fmt.Sprint(i)))
		if err != nil {
			require.ErrorContains(t, err, "maximum bytes exceeded")
			break
		}
		if i%2 == 0 {
			x++
		} else {
			y++
		}
	}
	require.Greater(t, y, 50)
	total := uint64(x + y)
	s1Eventually(t, js, "WH_HISTORY", total)

	acked, held := s1Pull(t, js, x+y, func(s string) bool { return s == "wh.ingest.0.y.events" })
	require.Equal(t, y, acked)
	require.Equal(t, x, held)
	s1Eventually(t, js, "WH_INGEST_0", uint64(x))

	// Y's freed bytes take new publishes.
	s1Publish(t, js, "wh.ingest.0.y.more", y/2, size)
	s1Eventually(t, js, "WH_INGEST_0", uint64(x+y/2))
	assert.EqualValues(t, total+uint64(y/2), s1Msgs(t, js, "WH_HISTORY"))
}

// The history keeps what it copied for its own max_age, independent of the
// partition, and then drops it.
func TestS1_HistoryKeepsForItsMaxAge(t *testing.T) {
	js := s1Connect(t, s1Server(t, t.TempDir()))
	s1Topology(t, js, 64<<20, 2*time.Second)

	s1Publish(t, js, "wh.ingest.0.acme.events", 10, 64)
	acked, _ := s1Pull(t, js, 10, ackEvery)
	require.Equal(t, 10, acked)
	s1Eventually(t, js, "WH_INGEST_0", 0)
	assert.EqualValues(t, 10, s1Msgs(t, js, "WH_HISTORY"))
	s1Eventually(t, js, "WH_HISTORY", 0)
}

// The source consumer holds interest on the partition until the history has
// stored the row: rows wh-ingest acks while the source is not flowing (here,
// in the ~10s after a restart before the history re-attaches its source) stay
// in the partition and reach the history once it does.
func TestS1_SourceHoldsRowsUntilCopied(t *testing.T) {
	dir := t.TempDir()
	s := s1Server(t, dir)
	js := s1Connect(t, s)
	s1Topology(t, js, 64<<20, time.Hour)
	s.Shutdown()
	s.WaitForShutdown()

	js = s1Connect(t, s1Server(t, dir))
	s1Publish(t, js, "wh.ingest.0.acme.events", 10, 64)
	acked, _ := s1Pull(t, js, 10, ackEvery)
	require.Equal(t, 10, acked)
	time.Sleep(200 * time.Millisecond)
	if s1Msgs(t, js, "WH_HISTORY") == 0 {
		assert.EqualValues(t, 10, s1Msgs(t, js, "WH_INGEST_0"), "acked rows left the partition before the history copied them")
	}
	s1EventuallyWithin(t, js, "WH_HISTORY", 10, 60*time.Second)
	s1Eventually(t, js, "WH_INGEST_0", 0)
}

// Deleting the history (or dropping a source from it) removes its source
// consumer, so the partition is never left holding rows for a reader that is
// gone.
func TestS1_HistoryGoneReleasesPartition(t *testing.T) {
	js := s1Connect(t, s1Server(t, t.TempDir()))
	s1Topology(t, js, 64<<20, time.Hour)
	require.NoError(t, js.DeleteStream(t.Context(), "WH_HISTORY"))
	require.Eventually(t, func() bool { return s1Consumers(t, js) == 1 }, 10*time.Second, 10*time.Millisecond)

	s1Publish(t, js, "wh.ingest.0.acme.events", 10, 64)
	acked, _ := s1Pull(t, js, 10, ackEvery)
	require.Equal(t, 10, acked)
	s1Eventually(t, js, "WH_INGEST_0", 0)
}
