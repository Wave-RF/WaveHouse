//go:build integration

// Package natsspike pins the nats-server behavior the external-NATS topology
// rests on (#613). It runs in `make test-integration` (seconds per test, more
// than the unit suite's per-package timeout spares), and lives under
// internal/mq because only internal/mq may import NATS.
package natsspike

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin what the ingest queue's design rests on, on both
// brokers. A work queue keeps a row no consumer covers and refuses a second
// consumer on a subject, so the live feed is the stream's republish to a plain
// subject, and a replay reads the waiting rows by direct get. Its retention
// cannot be changed by an update, so a stream of another retention is deleted
// and created again.

// server runs an in-process JetStream server on a random TCP port, shut
// down by the test framework.
func server(t *testing.T) *natsserver.Server {
	t.Helper()
	dir := t.TempDir()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoSigs: true, NoLog: true,
	})
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "server not ready")
	t.Cleanup(func() { shutdown(t, s, dir) })
	return s
}

// shutdown stops s and empties its store: a deleted stream's directory is
// removed by a goroutine Shutdown does not wait for, which would make
// t.TempDir's own cleanup fail with "directory not empty".
func shutdown(t *testing.T, s *natsserver.Server, dir string) {
	t.Helper()
	s.Shutdown()
	require.Eventually(t, func() bool { return os.RemoveAll(dir) == nil }, 5*time.Second, 10*time.Millisecond)
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

// partition is one work-queue partition republishing each row it stores to
// the live subject, with the partition and shard tokens dropped, and a durable
// on shard 0.
func partition(t *testing.T, cfg jetstream.StreamConfig) jetstream.JetStream {
	t.Helper()
	js := connect(t, server(t))
	cfg.Name, cfg.Subjects, cfg.Retention = "WH_INGEST_0", []string{"wh.ingest.0.>"}, jetstream.WorkQueuePolicy
	cfg.Discard = jetstream.DiscardNew
	cfg.RePublish = &jetstream.RePublish{Source: "wh.ingest.0.*.>", Destination: "wh.live.>"}
	_, err := js.CreateStream(t.Context(), cfg)
	require.NoError(t, err)
	require.NoError(t, durable(t, js, "WH_INGEST_0", "wh-ingest-0", "wh.ingest.0.0.>"))
	return js
}

// live subscribes to the live subject as an API process does: a plain
// subscription, no consumer.
func live(t *testing.T, js jetstream.JetStream) *nats.Subscription {
	t.Helper()
	sub, err := js.Conn().SubscribeSync("wh.live.>")
	require.NoError(t, err)
	require.NoError(t, js.Conn().Flush())
	return sub
}

// A plain subscriber of the republish subject gets every row the partition
// stores, once, in the order it stored them, under the subject without the
// partition and shard, with the stream's sequence and stored time and the
// publisher's own headers: the trace context rides them, and so would an
// expected-stream header, which is why an ingest publish sets none. Acked rows
// leaving the partition break nothing in the sequence.
func TestS2_RepublishFeedsALiveSubject(t *testing.T) {
	t.Parallel()
	js := partition(t, jetstream.StreamConfig{})
	sub := live(t, js)

	for i := range 3 {
		m := nats.NewMsg("wh.ingest.0.0.acme.orders")
		m.Data = fmt.Appendf(nil, "%d", i)
		m.Header = nats.Header{"traceparent": []string{"00-trace-span-01"}}
		_, err := js.PublishMsg(t.Context(), m)
		require.NoError(t, err)
	}
	require.Equal(t, 3, drain(t, js, "WH_INGEST_0", "wh-ingest-0"))
	publish(t, js, "wh.ingest.0.0.acme.orders.scope1", 2)

	wantSubjects := []string{"wh.live.acme.orders", "wh.live.acme.orders", "wh.live.acme.orders", "wh.live.acme.orders.scope1", "wh.live.acme.orders.scope1"}
	wantData := []string{"0", "1", "2", "0", "1"}
	for i := range wantSubjects {
		m, err := sub.NextMsg(5 * time.Second)
		require.NoError(t, err, "row %d", i)
		assert.Equal(t, wantSubjects[i], m.Subject)
		assert.Equal(t, wantData[i], string(m.Data))
		assert.Equal(t, "WH_INGEST_0", m.Header.Get("Nats-Stream"))
		assert.Equal(t, fmt.Sprint(i+1), m.Header.Get("Nats-Sequence"), "the stream's sequence, with no gap")
		stored, err := time.Parse(time.RFC3339Nano, m.Header.Get("Nats-Time-Stamp"))
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), stored, 10*time.Second)
		if i < 3 {
			assert.Equal(t, "00-trace-span-01", m.Header.Get("traceparent"), "the copy keeps the publish's headers")
		}
	}
	_, err := sub.NextMsg(200 * time.Millisecond)
	require.ErrorIs(t, err, nats.ErrTimeout, "each stored row once")
}

// Only a stored row is republished: neither a publish the duplicate window
// drops nor one a full partition refuses reaches the live subject, and a live
// subject nobody reads holds up no publish.
func TestS2_RepublishOnlyWhatIsStored(t *testing.T) {
	t.Parallel()
	js := partition(t, jetstream.StreamConfig{MaxMsgs: 2, Duplicates: time.Minute})
	publish(t, js, "wh.ingest.0.0.acme.unread", 1) // no subscriber yet
	sub := live(t, js)

	_, err := js.Publish(t.Context(), "wh.ingest.0.0.acme.t", []byte("first"), jetstream.WithMsgID("k1"))
	require.NoError(t, err)
	ack, err := js.Publish(t.Context(), "wh.ingest.0.0.acme.t", []byte("repeat"), jetstream.WithMsgID("k1"))
	require.NoError(t, err)
	assert.True(t, ack.Duplicate)
	_, err = js.Publish(t.Context(), "wh.ingest.0.0.acme.t", []byte("refused"))
	require.Error(t, err, "the partition is full")

	m, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "first", string(m.Data))
	_, err = sub.NextMsg(200 * time.Millisecond)
	require.ErrorIs(t, err, nats.ErrTimeout)
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

// directGet sends one batched direct get to stream and returns the rows it
// answered with, as "<sequence>:<data>", and the rows of the subject it left
// unread; found is false when it answered that nothing matched.
func directGet(t *testing.T, js jetstream.JetStream, stream string, req map[string]any) (rows []string, pending string, found bool) {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	nc := js.Conn()
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()
	require.NoError(t, nc.PublishRequest("$JS.API.DIRECT.GET."+stream, inbox, body))
	for {
		m, err := sub.NextMsg(5 * time.Second)
		require.NoError(t, err)
		switch status := m.Header.Get("Status"); status {
		case "":
			rows = append(rows, m.Header.Get("Nats-Sequence")+":"+string(m.Data))
		case "204":
			return rows, m.Header.Get("Nats-Num-Pending"), true
		case "404":
			return nil, "", false
		default:
			t.Fatalf("direct get %s: status %s %s", body, status, m.Header.Get("Description"))
		}
	}
}

// A work queue's waiting rows can be read with no consumer, which it would
// refuse: a batched direct get by subject, from a sequence or a start time,
// answers with the rows still stored, skipping the acked ones, then how many
// it left unread; nothing matching is a 404.
func TestS2_DirectGetReadsAWorkQueuesWaitingRows(t *testing.T) {
	t.Parallel()
	js := partition(t, jetstream.StreamConfig{AllowDirect: true})
	publish(t, js, "wh.ingest.0.0.acme.t1", 3) // sequences 1-3
	publish(t, js, "wh.ingest.0.0.acme.t2", 2) // 4-5
	later := time.Now()
	time.Sleep(20 * time.Millisecond)
	publish(t, js, "wh.ingest.0.0.acme.t1", 2) // 6-7

	_, err := js.CreateOrUpdateConsumer(t.Context(), "WH_INGEST_0", jetstream.ConsumerConfig{
		FilterSubject: "wh.ingest.0.0.acme.t1", AckPolicy: jetstream.AckNonePolicy, InactiveThreshold: time.Minute,
	})
	require.Error(t, err, "a replay cannot hold a consumer of its own on a work queue")

	// The worker writes and acks the first row.
	c, err := js.Consumer(t.Context(), "WH_INGEST_0", "wh-ingest-0")
	require.NoError(t, err)
	b, err := c.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	require.NoError(t, err)
	for m := range b.Messages() {
		require.NoError(t, m.DoubleAck(t.Context()))
	}

	const subject = "wh.ingest.0.0.acme.t1"
	rows, pending, found := directGet(t, js, "WH_INGEST_0", map[string]any{"batch": 100, "seq": 1, "next_by_subj": subject})
	require.True(t, found)
	assert.Equal(t, []string{"2:1", "3:2", "6:0", "7:1"}, rows, "the acked row is gone, the other table's are skipped")
	assert.Equal(t, "0", pending)

	rows, pending, found = directGet(t, js, "WH_INGEST_0", map[string]any{"batch": 2, "seq": 1, "next_by_subj": subject})
	require.True(t, found)
	assert.Equal(t, []string{"2:1", "3:2"}, rows)
	assert.Equal(t, "2", pending, "a short batch says how many it left")

	rows, _, found = directGet(t, js, "WH_INGEST_0", map[string]any{"batch": 100, "start_time": later, "next_by_subj": subject})
	require.True(t, found)
	assert.Equal(t, []string{"6:0", "7:1"}, rows, "from a time")

	_, _, found = directGet(t, js, "WH_INGEST_0", map[string]any{"batch": 100, "start_time": time.Now().Add(time.Hour), "next_by_subj": subject})
	assert.False(t, found, "nothing stored since")
	_, _, found = directGet(t, js, "WH_INGEST_0", map[string]any{"batch": 100, "seq": 1, "next_by_subj": "wh.ingest.0.0.acme.none"})
	assert.False(t, found, "nothing on the subject")
}

// The stream's leader answers a message get for the next row of a subject
// too, one row a request, whether or not the stream allows direct gets: what
// a replay asks last, since a direct get may be answered by a replica that
// trails.
func TestS2_LeaderGetReadsAWorkQueuesNextRow(t *testing.T) {
	t.Parallel()
	js := partition(t, jetstream.StreamConfig{})
	publish(t, js, "wh.ingest.0.0.acme.t2", 1)
	publish(t, js, "wh.ingest.0.0.acme.t1", 2)

	next := func(seq uint64) (*nats.Msg, error) {
		body, err := json.Marshal(map[string]any{"seq": seq, "next_by_subj": "wh.ingest.0.0.acme.t1"})
		require.NoError(t, err)
		return js.Conn().Request("$JS.API.STREAM.MSG.GET.WH_INGEST_0", body, 5*time.Second)
	}
	var got struct {
		Message *struct {
			Seq  uint64 `json:"seq"`
			Data []byte `json:"data"`
		} `json:"message"`
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	resp, err := next(1)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resp.Data, &got))
	require.NotNil(t, got.Message)
	assert.Equal(t, uint64(2), got.Message.Seq)
	assert.Equal(t, "0", string(got.Message.Data))

	got.Message = nil
	resp, err = next(4)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resp.Data, &got))
	assert.Nil(t, got.Message)
	require.NotNil(t, got.Error)
	assert.Equal(t, 404, got.Error.Code)
}

// A stream update cannot change retention to or from work queue, so a stream
// an earlier build created with another retention has to be deleted and
// created again; allow_direct and the republish can be set on a stream that
// exists, which is how an operator moves a deployed partition.
func TestS2_RetentionCannotBeUpdated(t *testing.T) {
	t.Parallel()
	js := connect(t, server(t))
	cfg := jetstream.StreamConfig{Name: "IN", Subjects: []string{"wh.ingest.0.>"}, Retention: jetstream.LimitsPolicy}
	_, err := js.CreateStream(t.Context(), cfg)
	require.NoError(t, err)

	cfg.Retention = jetstream.WorkQueuePolicy
	var apiErr *jetstream.APIError
	_, err = js.UpdateStream(t.Context(), cfg)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, jetstream.ErrorCode(10052), apiErr.ErrorCode)

	require.NoError(t, js.DeleteStream(t.Context(), "IN"))
	cfg.RePublish = &jetstream.RePublish{Source: "wh.ingest.0.*.>", Destination: "wh.hist.>"}
	_, err = js.CreateStream(t.Context(), cfg)
	require.NoError(t, err)
	cfg.AllowDirect = true
	cfg.RePublish = &jetstream.RePublish{Source: "wh.ingest.0.*.>", Destination: "wh.live.>"}
	s, err := js.UpdateStream(t.Context(), cfg)
	require.NoError(t, err)
	assert.True(t, s.CachedInfo().Config.AllowDirect)
	assert.Equal(t, "wh.live.>", s.CachedInfo().Config.RePublish.Destination)

	cfg.Retention = jetstream.LimitsPolicy
	_, err = js.UpdateStream(t.Context(), cfg)
	require.ErrorAs(t, err, &apiErr, "nor back")
	assert.Equal(t, jetstream.ErrorCode(10052), apiErr.ErrorCode)
}
