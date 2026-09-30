//go:build integration

package mq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// shippedFixture is a fixture server with the shipped topology applied.
func shippedFixture(t *testing.T) *natsFixture {
	t.Helper()
	f := newNATSFixture(t)
	f.apply(t, shippedTopology(t))
	return f
}

// restart stops the server and starts it again on the same port and store.
func (f *natsFixture) restart(t *testing.T) {
	t.Helper()
	if f.server.Running() {
		f.stop()
	}
	s, err := natsserver.NewServer(f.opts.Clone())
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "nats server not ready after restart")
	t.Cleanup(s.Shutdown)
	f.server = s
}

// stop shuts the server down, keeping its port for restart.
func (f *natsFixture) stop() {
	f.opts = f.opts.Clone()
	f.opts.Port = f.server.Addr().(*net.TCPAddr).Port
	f.server.Shutdown()
	f.server.WaitForShutdown()
}

// writeSecret writes a secret file, as a mounted Kubernetes Secret would be.
func writeSecret(t *testing.T, secret string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte(secret+"\n"), 0o600))
	return path
}

// The broker rides out a server restart: a publish while the server is down
// is ErrUnavailable, not a 500's plain error, and the next one is stored;
// consumption resumes on its own, and failed stays quiet.
func TestExternalNATS_Reconnect(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, func(c *NATSConfig) { c.Topology.PublishTimeout = 300 * time.Millisecond })
	topic := Topic{Tenant: "acme", Table: "t"}

	cons, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.NoError(t, err)
	got := make(chan string, 16)
	stop, failed, err := cons.Consume(func(m *Message) {
		assert.NoError(t, m.DoubleAck(m.Ctx))
		got <- string(m.Data)
	}, 16)
	require.NoError(t, err)
	t.Cleanup(stop)

	require.NoError(t, e.Publish(t.Context(), topic, []byte("before")))
	require.Equal(t, "before", receive(t, got))

	f.stop()
	require.Eventually(t, func() bool { return !e.connected.Load() }, 5*time.Second, 10*time.Millisecond)
	err = e.Publish(t.Context(), topic, []byte("down"))
	require.ErrorIs(t, err, ErrUnavailable)
	assert.NotErrorIs(t, err, ErrQueueFull)

	f.restart(t)
	require.Eventually(t, func() bool { return e.Publish(t.Context(), topic, []byte("after")) == nil },
		15*time.Second, 100*time.Millisecond, "publishing never resumed")
	for data := receive(t, got); data != "after"; data = receive(t, got) {
		// A publish that timed out before the restart may have been stored.
		assert.Equal(t, "down", data)
	}
	select {
	case err := <-failed:
		t.Fatalf("a reconnect reported failed: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

func receive(t *testing.T, got <-chan string) string {
	t.Helper()
	select {
	case s := <-got:
		return s
	case <-time.After(15 * time.Second):
		t.Fatal("nothing delivered")
		return ""
	}
}

// flakyPublish loses the answers to the first publishes it passes on, as a
// dropped connection or a leader change could, and records each attempt's
// Nats-Msg-Id.
type flakyPublish struct {
	jetstream.JetStream
	lose atomic.Int32
	ids  []string
}

func (j *flakyPublish) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, m, opts...)
	j.ids = append(j.ids, m.Header.Get(jetstream.MsgIDHeader))
	if err == nil && j.lose.Add(-1) >= 0 {
		return nil, context.DeadlineExceeded
	}
	return ack, err
}

// A publish whose answer is lost is sent again with the same Nats-Msg-Id,
// so the partition stores it once; a fresh publish gets a fresh id.
func TestExternalNATS_PublishRetryStoresOnce(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	flaky := &flakyPublish{JetStream: e.js}
	flaky.lose.Store(publishRetries)
	e.js = flaky
	topic := Topic{Tenant: "acme", Table: "t"}
	stream := shippedPartition(mustPartition(topic))

	require.NoError(t, e.Publish(t.Context(), topic, []byte("x")))
	require.Len(t, flaky.ids, publishRetries+1)
	for _, id := range flaky.ids {
		assert.Equal(t, flaky.ids[0], id)
	}
	assert.Equal(t, uint64(1), f.streamMsgs(t, stream))

	require.NoError(t, e.Publish(t.Context(), topic, []byte("y")))
	assert.NotEqual(t, flaky.ids[0], flaky.ids[len(flaky.ids)-1])
	assert.Equal(t, uint64(2), f.streamMsgs(t, stream))

	// Lost every time: the publish gives up as unavailable.
	flaky.lose.Store(publishRetries + 1)
	require.ErrorIs(t, e.Publish(t.Context(), topic, []byte("z")), ErrUnavailable)
}

// A topic at the partition's max_msgs_per_subject is refused as full, like a
// partition at max_bytes, and only that topic.
func TestExternalNATS_TopicAtItsCapIsFull(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	for i := range 4 {
		tp.stream(t, shippedPartition(i)).MaxMsgsPerSubject = 2
	}
	f.apply(t, tp)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "t"}
	for range 2 {
		require.NoError(t, e.Publish(t.Context(), topic, []byte("x")))
	}
	require.ErrorIs(t, e.Publish(t.Context(), topic, []byte("x")), ErrQueueFull)
	require.NoError(t, e.Publish(t.Context(), Topic{Tenant: "acme", Table: "other"}, []byte("x")))
}

// Under nats, WaveHouse does not require sync_always: against a server with
// it off, a publish to a one-replica partition is acked and stored, and boot
// reports the replica count as recommended only. TestShippedValues_SetNoSync
// covers the shipped values.
func TestExternalNATS_PublishesWithoutSyncAlways(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	require.False(t, f.server.JetStreamConfig().SyncAlways)

	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "t"}
	stream := shippedPartition(mustPartition(topic))
	s, err := f.admin.Stream(t.Context(), stream)
	require.NoError(t, err)
	require.Equal(t, 1, s.CachedInfo().Config.Replicas)

	require.NoError(t, e.Publish(t.Context(), topic, []byte("x")))
	assert.Equal(t, uint64(1), f.streamMsgs(t, stream))

	findings, err := verifyNATSTopology(t.Context(), e.js, e.topo)
	require.NoError(t, err)
	for _, got := range findings {
		assert.Equal(t, FindingRecommended, got.Severity, "unexpected finding %v", got)
	}
	assert.True(t, slices.ContainsFunc(findings, func(got Finding) bool {
		return got.Object == "stream "+stream && got.Field == "num_replicas"
	}), "findings: %v", findings)
}

// A partition stream the operator deleted is ErrUnavailable, and the
// topology gauge drops at once; the broker creates nothing.
func TestExternalNATS_MissingPartitionIsUnavailable(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, func(c *NATSConfig) { c.Topology.PublishTimeout = 300 * time.Millisecond })
	topic := Topic{Tenant: "acme", Table: "t"}
	stream := shippedPartition(mustPartition(topic))
	require.True(t, e.topologyOK.Load())

	require.NoError(t, f.admin.DeleteStream(t.Context(), stream))
	err := e.Publish(t.Context(), topic, []byte("x"))
	require.ErrorIs(t, err, ErrUnavailable)
	assert.ErrorContains(t, err, stream)
	assert.False(t, e.topologyOK.Load())
	_, err = f.admin.Stream(t.Context(), stream)
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound, "the broker must not recreate the stream")
}

// Boot refuses a topology the operator never made, listing what is missing.
func TestNewNATS_RefusesAMissingTopology(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	tp.drop("WH_DLQ")
	f.apply(t, tp)
	_, err := NewNATS(t.Context(), NATSConfig{
		URLs: []string{f.server.ClientURL()}, User: "wavehouse", PasswordFile: writeSecret(t, fixturePassword("wavehouse")),
		Topology: NATSTopology{Partitions: 4, Shards: 8}, TopologyWait: 500 * time.Millisecond,
	})
	require.ErrorIs(t, err, ErrTopology)
	var te *TopologyError
	require.ErrorAs(t, err, &te)
	assert.Contains(t, err.Error(), "no stream holds wh.dlq.x")
}

// Boot that never reaches a server is ErrUnavailable, once the wait is out.
func TestNewNATS_Unreachable(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	url := f.server.ClientURL()
	f.server.Shutdown()
	start := time.Now()
	_, err := NewNATS(t.Context(), NATSConfig{URLs: []string{url}, TopologyWait: 500 * time.Millisecond})
	require.ErrorIs(t, err, ErrUnavailable)
	assert.Less(t, time.Since(start), 10*time.Second)
}

// Conflicting auth or half a TLS key pair is refused before dialing.
func TestNewNATS_RefusesConflictingOptions(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]NATSConfig{
		"no urls":       {},
		"two auths":     {URLs: []string{"nats://127.0.0.1:1"}, User: "u", CredsFile: "x.creds"},
		"cert, no key":  {URLs: []string{"nats://127.0.0.1:1"}, TLS: NATSTLS{CertFile: "c.pem"}},
		"bad prefix":    {URLs: []string{"nats://127.0.0.1:1"}, Topology: NATSTopology{Prefix: "a.b"}},
		"password file": {URLs: []string{"nats://127.0.0.1:1"}, User: "u", PasswordFile: "/nonexistent"},
	} {
		_, err := NewNATS(t.Context(), cfg)
		assert.Error(t, err, name)
	}
}

// The re-check reports a topology the operator broke after boot, and its
// repair. The history poll reports how far the history trails the partitions:
// a row stored while the history refused it never reaches it, which shows
// as a gap until the next row republished closes it.
func TestExternalNATS_Recheck(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, func(c *NATSConfig) {
		c.recheckEvery = 50 * time.Millisecond
		c.historyEvery = 50 * time.Millisecond
	})
	require.True(t, e.topologyOK.Load())

	s, err := f.admin.Stream(t.Context(), "WH_DLQ")
	require.NoError(t, err)
	dlq := s.CachedInfo().Config
	require.NoError(t, f.admin.DeleteStream(t.Context(), "WH_DLQ"))
	require.Eventually(t, func() bool { return !e.topologyOK.Load() }, 5*time.Second, 10*time.Millisecond)
	_, err = f.admin.CreateStream(t.Context(), dlq)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return e.topologyOK.Load() }, 5*time.Second, 10*time.Millisecond)

	topic := Topic{Tenant: "acme", Table: "events"}
	require.NoError(t, e.Publish(t.Context(), topic, []byte("1")))
	require.Eventually(t, func() bool { return e.historyBehind.Load() == 0 }, 5*time.Second, 10*time.Millisecond, "healthy")

	// A full history with discard new refuses the next row's copy.
	h, err := f.admin.Stream(t.Context(), "WH_HISTORY")
	require.NoError(t, err)
	history := h.CachedInfo().Config
	full := history
	full.MaxMsgs, full.Discard = int64(h.CachedInfo().State.Msgs), jetstream.DiscardNew
	_, err = f.admin.UpdateStream(t.Context(), full)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond) // a gap the gauge can see
	published := time.Now()
	require.NoError(t, e.Publish(t.Context(), topic, []byte("2")), "ingest does not depend on the history")
	require.Eventually(t, func() bool { return e.historyBehind.Load() > 0 }, 5*time.Second, 10*time.Millisecond,
		"a row the history refused shows as a gap")
	assert.Less(t, time.Duration(e.historyBehind.Load()), time.Since(published)+time.Second, "the gap is the missed row's, bounded")

	_, err = f.admin.UpdateStream(t.Context(), history)
	require.NoError(t, err)
	require.NoError(t, e.Publish(t.Context(), topic, []byte("3")))
	require.Eventually(t, func() bool { return e.historyBehind.Load() == 0 }, 5*time.Second, 10*time.Millisecond,
		"the next row republished closes it")
}

// A publish the ack says another stream stored — the operator replaced a
// partition's stream under the same subjects — is a topology fault.
func TestExternalNATS_PublishStoredElsewhere(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	stream := shippedPartition(mustPartition(topic))
	s, err := f.admin.Stream(t.Context(), stream)
	require.NoError(t, err)
	rogue := s.CachedInfo().Config
	rogue.Name = "ROGUE"
	require.NoError(t, f.admin.DeleteStream(t.Context(), stream))
	_, err = f.admin.CreateStream(t.Context(), rogue)
	require.NoError(t, err)

	err = e.Publish(t.Context(), topic, []byte("x"))
	require.ErrorIs(t, err, ErrUnavailable)
	assert.ErrorContains(t, err, "stored by stream ROGUE")
	assert.False(t, e.topologyOK.Load())
}

// The gauges report the connection, the topology and the history's gap.
func TestExternalNATS_Gauges(t *testing.T) { //nolint:paralleltest // sets the global meter provider
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	f := shippedFixture(t)
	f.broker(t, nil)
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	got := map[string][]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch g := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range g.DataPoints {
					got[m.Name] = append(got[m.Name], float64(dp.Value))
				}
			case metricdata.Gauge[float64]:
				for _, dp := range g.DataPoints {
					got[m.Name] = append(got[m.Name], dp.Value)
				}
			}
		}
	}
	assert.Equal(t, []float64{1}, got["wavehouse_mq_connected"])
	assert.Equal(t, []float64{1}, got["wavehouse_mq_topology_ok"])
	assert.Equal(t, []float64{0}, got["wavehouse_mq_history_behind_seconds"])
}

// PurgeAcked removes nothing. CheckReplayWindows warns for a tenant whose
// gap window the history cannot hold, once per window: the same windows warn
// nobody again, a longer one warns anew, and a window equal to max_age fits.
func TestExternalNATS_ReplayWindowsAgainstTheHistory(t *testing.T) { //nolint:paralleltest // captures the default logger
	logs := logtest.Capture(t, slog.LevelWarn)
	f := shippedFixture(t)
	e := f.broker(t, nil)
	maxAge := time.Duration(e.historyMaxAge.Load())
	require.Positive(t, maxAge, "the shipped history has a max_age")

	purged, err := e.PurgeAcked(t.Context(), workerDurable, map[tenant.ID]time.Time{"acme": time.Now().Add(-2 * maxAge)})
	require.NoError(t, err)
	assert.False(t, purged)
	_, err = e.PurgeAcked(t.Context(), "someone-else", nil)
	require.ErrorIs(t, err, ErrConsumerNotFound)

	warned := func() int { return strings.Count(logs.String(), "keeps less than this tenant's gap window") }
	windows := map[tenant.ID]time.Duration{"acme": 2 * maxAge, "globex": time.Minute, "initech": maxAge}
	e.CheckReplayWindows(windows)
	assert.Equal(t, 1, warned(), "acme only: %s", logs.String())
	assert.Contains(t, logs.String(), `"tenant":"acme"`)
	e.CheckReplayWindows(windows)
	assert.Equal(t, 1, warned(), "the same window warns once")
	windows["acme"] = 3 * maxAge
	e.CheckReplayWindows(windows)
	assert.Equal(t, 2, warned(), "a longer window warns again")

	// The operator shortens the history: the poll checks the last windows again.
	h, err := f.admin.Stream(t.Context(), e.topo.HistoryStream)
	require.NoError(t, err)
	shorter := h.CachedInfo().Config
	shorter.MaxAge = maxAge / 2
	_, err = f.admin.UpdateStream(t.Context(), shorter)
	require.NoError(t, err)
	require.NoError(t, e.readHistory(t.Context()))
	assert.Equal(t, 3, warned(), "initech's window no longer fits: %s", logs.String())
}

// CreateConsumer finds the operator's durable and holds it to the worker's
// ask; it never creates one.
func TestExternalNATS_CreateConsumerChecksTheDurable(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)

	_, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable, AckWait: time.Hour})
	require.ErrorContains(t, err, "ack_wait")
	require.ErrorIs(t, err, ErrConsumerMismatch)
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: "someone-else"})
	require.ErrorIs(t, err, ErrConsumerNotFound)
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: DefaultNATSIngestConsumer, AckWait: time.Minute})
	require.NoError(t, err)

	require.NoError(t, f.admin.DeleteConsumer(t.Context(), shippedPartition(0), "wh-ingest-3"))
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.ErrorIs(t, err, ErrConsumerNotFound)
	_, err = f.admin.Consumer(t.Context(), shippedPartition(0), "wh-ingest-3")
	require.ErrorIs(t, err, jetstream.ErrConsumerNotFound, "the broker must not recreate the durable")

	require.NoError(t, f.admin.DeleteStream(t.Context(), shippedPartition(1)))
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable, Units: []string{shippedPartition(1) + "/wh-ingest-0"}})
	require.ErrorIs(t, err, ErrConsumerNotFound, "a durable whose stream is gone is gone")
}

// The shipped permissions refuse the wavehouse user everything that would
// change the operator's topology.
func TestNATSPermissions_RefuseTopologyChanges(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	js := f.connect(t, "wavehouse", nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	ctx := t.Context()
	partition := shippedPartition(0)
	denied := func(what string, err error) {
		t.Helper()
		require.Error(t, err, what)
		assert.False(t, errors.Is(err, context.Canceled), what)
	}

	s, err := f.admin.Stream(ctx, partition)
	require.NoError(t, err)
	cfg := s.CachedInfo().Config

	denied("create a stream", call(ctx, func(ctx context.Context) error {
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ROGUE", Subjects: []string{"rogue.>"}})
		return err
	}))
	cfg.MaxBytes++
	denied("update a partition", call(ctx, func(ctx context.Context) error {
		_, err := js.UpdateStream(ctx, cfg)
		return err
	}))
	denied("purge a partition", call(ctx, func(ctx context.Context) error {
		ws, err := js.Stream(ctx, partition)
		if err != nil {
			return err
		}
		return ws.Purge(ctx)
	}))
	denied("delete a partition", call(ctx, func(ctx context.Context) error { return js.DeleteStream(ctx, partition) }))
	denied("create a durable on a partition", call(ctx, func(ctx context.Context) error {
		_, err := js.CreateConsumer(ctx, partition, jetstream.ConsumerConfig{Durable: "rogue", AckPolicy: jetstream.AckExplicitPolicy})
		return err
	}))
	denied("delete a shard durable", call(ctx, func(ctx context.Context) error {
		return js.DeleteConsumer(ctx, partition, "wh-ingest-0")
	}))
	denied("forge a history row", call(ctx, func(ctx context.Context) error {
		_, err := js.Publish(ctx, "wh.hist.acme.events", []byte("forged"))
		return err
	}))

	_, err = f.admin.Stream(ctx, "ROGUE")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
	h, err := f.admin.Stream(ctx, "WH_HISTORY")
	require.NoError(t, err)
	assert.Zero(t, h.CachedInfo().State.Msgs, "only the partitions' republish writes the history")
	_, err = f.admin.Consumer(ctx, partition, "wh-ingest-0")
	require.NoError(t, err)
}

// Each partition republishes a row to the history under its topic, the
// partition token dropped, headers and body intact: what the hub and a
// replay read, whatever the partition count.
func TestExternalNATS_HistoryDropsThePartition(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topics := []Topic{{Tenant: "acme", Table: "events"}, {Tenant: "acme", Table: "events", Scope: "eu"}, {Tenant: "globex", Table: "a.b"}}
	for _, topic := range topics {
		require.NoError(t, e.Publish(t.Context(), topic, []byte(topic.Table), WithHeader("X-Test", "kept")))
	}
	h, err := f.admin.Stream(t.Context(), "WH_HISTORY")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, err := h.Info(t.Context(), jetstream.WithSubjectFilter(">"))
		return err == nil && info.State.Msgs == uint64(len(topics))
	}, 5*time.Second, 10*time.Millisecond)
	info, err := h.Info(t.Context(), jetstream.WithSubjectFilter(">"))
	require.NoError(t, err)
	var subjects []string
	for subj := range info.State.Subjects {
		subjects = append(subjects, subj)
	}
	assert.ElementsMatch(t, []string{"wh.hist.acme.events", "wh.hist.acme.events.eu", "wh.hist.globex.a%2Eb"}, subjects)
	m, err := h.GetLastMsgForSubject(t.Context(), "wh.hist.acme.events")
	require.NoError(t, err)
	assert.Equal(t, "events", string(m.Data))
	assert.Equal(t, "kept", m.Header.Get("X-Test"))
}

// call runs a request that a permission violation answers by never
// answering, bounded.
func call(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return fn(ctx)
}

// measure skips a measurement unless WH_MQ_MEASURE is set: it reports numbers
// for the PR record, and asserts nothing a loaded CI machine could fail.
func measure(t *testing.T) {
	t.Helper()
	if os.Getenv("WH_MQ_MEASURE") == "" {
		t.Skip("set WH_MQ_MEASURE=1 to measure")
	}
}

// A publish's latency until the hub's Subscribe sees it, through the
// partition and its republish to the history. Design risk 3 moves the hub off the
// history if p99 passes 50ms.
func TestExternalNATS_MeasurePublishToHub(t *testing.T) {
	measure(t)
	e := shippedFixture(t).broker(t, nil)
	seen := make(chan time.Time, 1)
	require.NoError(t, e.Subscribe(t.Context(), "hub-bridge", func(*Message) error {
		seen <- time.Now()
		return nil
	}))
	topic := Topic{Tenant: "acme", Table: "t"}
	const n = 2000
	lat := make([]time.Duration, 0, n)
	for range n {
		start := time.Now()
		require.NoError(t, e.Publish(t.Context(), topic, []byte(`{"a":1}`)))
		lat = append(lat, (<-seen).Sub(start))
	}
	slices.Sort(lat)
	t.Logf("publish to hub over %d events: p50 %s, p99 %s, max %s", n, lat[n/2], lat[n*99/100], lat[n-1])
}

// One tenant's publish throughput into its partition, which bounds a hot
// tenant (design risk 4).
func TestExternalNATS_MeasurePublishThroughput(t *testing.T) {
	measure(t)
	e := shippedFixture(t).broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "t"}
	payload := make([]byte, 256)
	const workers, each = 32, 500
	start := time.Now()
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				assert.NoError(t, e.Publish(t.Context(), topic, payload))
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("%d publishes of %d bytes by %d callers in %s: %.0f/s", workers*each, len(payload), workers, elapsed, float64(workers*each)/elapsed.Seconds())
}

// The duplicate window must cover every attempt of a retried publish, not
// just two publish timeouts: a window of exactly 2 × PublishTimeout is too
// short for the last retry.
func TestExternalNATS_DuplicateWindowCoversEveryRetry(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	topo := NATSTopology{Partitions: 4, Shards: 8, PublishTimeout: 30 * time.Second}
	for i := range 4 {
		tp.stream(t, shippedPartition(i)).Duplicates = 2 * topo.PublishTimeout
	}
	f.apply(t, tp)
	findings, err := verifyNATSTopology(t.Context(), f.connect(t, "wavehouse"), topo)
	require.NoError(t, err)
	n := 0
	for _, fd := range findings {
		if fd.Field == "duplicate_window" {
			n++
			assert.Equal(t, FindingRequired, fd.Severity)
		}
	}
	assert.Equal(t, 4, n)
}

// A replay sends the events counted when it began and stops: events that
// arrive during it reach an SSE client through its live subscription.
func TestExternalNATS_ReplayDoesNotChaseTheTail(t *testing.T) {
	t.Parallel()
	e := shippedFixture(t).broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "r"}
	for _, d := range []string{"a", "b", "c"} {
		require.NoError(t, e.Publish(t.Context(), topic, []byte(d)))
	}
	require.Eventually(t, func() bool {
		n := 0
		require.NoError(t, e.ReplaySince(t.Context(), topic, time.Time{}, func([]byte) bool { n++; return true }))
		return n == 3
	}, 5*time.Second, 20*time.Millisecond)

	var got []string
	require.NoError(t, e.ReplaySince(t.Context(), topic, time.Time{}, func(data []byte) bool {
		if len(got) == 0 {
			for range 5 {
				assert.NoError(t, e.Publish(t.Context(), topic, []byte("late")))
			}
			time.Sleep(200 * time.Millisecond) // let the history copy them
		}
		got = append(got, string(data))
		return true
	}))
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

// A replay longer than one fetched batch, sent to a slow client, arrives
// whole: its consumer outlives the time a batch takes to send.
func TestExternalNATS_SlowReplayArrivesWhole(t *testing.T) {
	t.Parallel()
	e := shippedFixture(t).broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "slow"}
	const n = replayBatch + 44
	for i := range n {
		require.NoError(t, e.Publish(t.Context(), topic, []byte(strconv.Itoa(i))))
	}
	require.Eventually(t, func() bool {
		got := 0
		require.NoError(t, e.ReplaySince(t.Context(), topic, time.Time{}, func([]byte) bool { got++; return got < n }))
		return got == n
	}, 5*time.Second, 20*time.Millisecond)

	var got []string
	require.NoError(t, e.ReplaySince(t.Context(), topic, time.Time{}, func(data []byte) bool {
		time.Sleep(25 * time.Millisecond) // 256 of these outlast the old 5s threshold
		got = append(got, string(data))
		return true
	}))
	require.Len(t, got, n)
	for i, d := range got {
		require.Equal(t, strconv.Itoa(i), d)
	}
}

// Closing the broker is not a disconnect worth a warning; losing the server
// still is.
func TestExternalNATS_CloseLogsNoWarning(t *testing.T) { //nolint:paralleltest // captures the default logger
	f := shippedFixture(t)
	e := f.broker(t, nil)
	other := f.broker(t, nil)
	cons, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.NoError(t, err)
	_, _, err = cons.Consume(func(*Message) {}, 16)
	require.NoError(t, err)
	require.NoError(t, e.Subscribe(t.Context(), "hub", func(*Message) error { return nil }))

	logs := logtest.Capture(t, slog.LevelWarn)
	require.NoError(t, e.Close())
	assert.Empty(t, logs.String(), "a deliberate close logged at WARN or above")

	f.stop()
	require.Eventually(t, func() bool { return !other.connected.Load() }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "mq: disconnected from nats; reconnecting") },
		5*time.Second, 10*time.Millisecond, "a lost server must still warn")
}

// generatedTopology is `wavehouse mq manifests --partitions n --shards v`
// at one replica.
func generatedTopology(t *testing.T, n, v int) *fixtureTopology {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, WriteNATSManifests(&buf, NATSManifestOptions{Topology: NATSTopology{Partitions: n, Shards: v}, Replicas: 1}))
	path := filepath.Join(t.TempDir(), "manifests.yaml")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	return loadNATSManifests(t, path)
}

// Lowering N from 2 to 1 the way deployment.md says — apply the regenerated
// manifests, roll out the smaller N while an old-N process keeps publishing —
// loses none of partition 1's rows: the new worker drains them through its
// shard durables, which are extra units now, and the operator deleting the
// emptied stream ends only those units' delivery.
func TestExternalNATS_LoweringNDrainsTheRemovedPartition(t *testing.T) { //nolint:paralleltest // captures the default logger
	f := newNATSFixture(t)
	f.apply(t, generatedTopology(t, 2, 2))
	const removed = "WH_INGEST_1"
	topic := topicIn(t, "acme", 1, 2, 2)
	_, shard := natsRoute(topic, 2, 2)
	unit := removed + "/" + natsShardDurable(DefaultNATSIngestConsumer, shard)
	old := f.broker(t, func(c *NATSConfig) { c.Topology.Partitions, c.Topology.Shards = 2, 2 })
	before := []string{"a", "b", "c", "d", "e"}
	for _, row := range before {
		require.NoError(t, old.Publish(t.Context(), topic, []byte(row)))
	}
	require.Equal(t, uint64(len(before)), f.streamMsgs(t, removed))

	require.NoError(t, generatedTopology(t, 1, 2).Apply(t.Context(), f.admin))
	e := f.broker(t, func(c *NATSConfig) { c.Topology.Partitions, c.Topology.Shards = 1, 2 })
	_, extra := e.IngestUnits()
	assert.Equal(t, []string{removed + "/wh-ingest-0", removed + "/wh-ingest-1"}, extra)
	findings, err := verifyNATSTopology(t.Context(), e.js, e.topo)
	require.NoError(t, err)
	assert.True(t, slices.ContainsFunc(findings, func(got Finding) bool {
		return got.Severity == FindingRecommended && got.Object == "consumer "+unit && strings.Contains(got.Problem, "drain its 5 rows")
	}), "no finding names the removed partition's rows among %v", findings)

	logs := logtest.Capture(t, slog.LevelInfo)
	cons, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.NoError(t, err)
	got := make(chan string, 16)
	stop, failed, err := cons.Consume(func(m *Message) {
		assert.NoError(t, m.DoubleAck(m.Ctx))
		got <- string(m.Data)
	}, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	drained := make([]string, 0, len(before))
	for range before {
		drained = append(drained, receive(t, got))
	}
	assert.Equal(t, before, drained)

	// The old-N process is still up during the rollout, publishing to the
	// removed partition.
	require.NoError(t, old.Publish(t.Context(), topic, []byte("during")))
	require.Equal(t, "during", receive(t, got))
	require.NoError(t, old.Close())
	require.Eventually(t, func() bool { return f.streamMsgs(t, removed) == 0 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.Publish(t.Context(), topic, []byte("moved")))
	require.Equal(t, "moved", receive(t, got))

	require.NoError(t, f.admin.DeleteStream(t.Context(), removed))
	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "mq: stopped draining a shard durable outside the configured ones")
	}, 15*time.Second, 50*time.Millisecond, "the removed partition's delivery never ended")
	select {
	case err := <-failed:
		t.Fatalf("deleting a drained removed partition reported failed: %v", err)
	default:
	}
	require.NoError(t, e.Publish(t.Context(), topic, []byte("still")))
	require.Equal(t, "still", receive(t, got))
}

// The queue's units are every partition's shard durables, in partition then
// shard order, and a publish lands on its table's (partition, shard).
func TestExternalNATS_ShardUnits(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	configured, extra := e.IngestUnits()
	require.Len(t, configured, 32)
	assert.Equal(t, "WH_INGEST_0/wh-ingest-0", configured[0])
	assert.Equal(t, "WH_INGEST_3/wh-ingest-7", configured[31])
	assert.Empty(t, extra)

	topic := Topic{Tenant: "acme", Table: "events"}
	require.NoError(t, e.Publish(t.Context(), topic, []byte("x")))
	p, s := natsRoute(topic, 4, 8)
	c, err := f.admin.Consumer(t.Context(), shippedPartition(p), natsShardDurable("wh-ingest", s))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), c.CachedInfo().NumPending, "the row waits on its shard's durable")

	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable, Units: []string{"WH_INGEST_9/wh-ingest-0"}})
	require.ErrorIs(t, err, ErrUnitsUnsupported)
}

// unitConsumer consumes one unit of e, sending each row's body to the
// returned channel and acking it unless hold is set.
func unitConsumer(t *testing.T, e *ExternalNATS, unit string, hold *atomic.Bool) (Consumer, func(), <-chan string) {
	t.Helper()
	c, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable, Units: []string{unit}})
	require.NoError(t, err)
	got := make(chan string, 64)
	stop, _, err := c.Consume(func(m *Message) {
		if hold == nil || !hold.Load() {
			assert.NoError(t, m.Ack())
		}
		got <- string(m.Data)
	}, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	return c, stop, got
}

// Two processes pull one unit and only the pinned one receives. Release by
// the one that never received leaves the owner's pin alone; the owner's own
// release, after it stopped, hands the unit over at once.
func TestExternalNATS_ReleaseUnpinsOnlyItsOwnPin(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	a, b := f.broker(t, nil), f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	unit := shippedPartition(p) + "/" + natsShardDurable("wh-ingest", s)

	ca, stopA, gotA := unitConsumer(t, a, unit, nil)
	require.NoError(t, a.Publish(t.Context(), topic, []byte("1")))
	require.Equal(t, "1", receive(t, gotA))
	cb, _, gotB := unitConsumer(t, b, unit, nil)
	require.NoError(t, a.Publish(t.Context(), topic, []byte("2")))
	require.Equal(t, "2", receive(t, gotA), "A holds the pin")

	pinnedTo := func() string {
		c, err := f.admin.Consumer(t.Context(), shippedPartition(p), natsShardDurable("wh-ingest", s))
		require.NoError(t, err)
		return pinnedClient(c.CachedInfo())
	}
	pin := pinnedTo()
	require.NotEmpty(t, pin)
	require.NoError(t, cb.(Releaser).Release(t.Context()))
	assert.Equal(t, pin, pinnedTo(), "B never held the unit, so its release leaves A's pin")

	stopA()
	released := time.Now()
	require.NoError(t, ca.(Releaser).Release(t.Context()))
	assert.Empty(t, pinnedTo())
	require.NoError(t, a.Publish(t.Context(), topic, []byte("3")))
	require.Equal(t, "3", receive(t, gotB))
	assert.Less(t, time.Since(released), 5*time.Second, "at once, not after the pinned ttl")
}

// quietAfter shortens how recently a unit must have been active to count
// as someone's, the durable's pinned_ttl otherwise.
func quietAfter(d time.Duration) func(*NATSConfig) {
	return func(cfg *NATSConfig) { cfg.activeWithin = d }
}

// A unit whose holder is gone with rows unacked: ResetOrphaned leaves it
// alone while a client still holds the pin, or was active on it within the
// window, and once neither, redelivers the held rows to the next owner at
// once, not after ack_wait.
func TestExternalNATS_ResetOrphaned(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	const quiet = time.Second
	a, b := f.broker(t, nil), f.broker(t, quietAfter(quiet))
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	unit := shippedPartition(p) + "/" + natsShardDurable("wh-ingest", s)

	var hold atomic.Bool
	hold.Store(true)
	unitConsumer(t, a, unit, &hold)
	for _, row := range []string{"x", "y"} {
		require.NoError(t, a.Publish(t.Context(), topic, []byte(row)))
	}
	reset, err := b.ResetOrphaned(t.Context(), unit)
	require.ErrorIs(t, err, ErrUnitHeld, "a live holder's rows are its own")
	assert.False(t, reset)

	a.nc.Close() // A dies holding x and y
	st, err := f.admin.Stream(t.Context(), shippedPartition(p))
	require.NoError(t, err)
	require.NoError(t, st.UnpinConsumer(t.Context(), natsShardDurable("wh-ingest", s), natsPriorityGroup), "stands in for the pin lapsing")
	reset, err = b.ResetOrphaned(t.Context(), unit)
	require.ErrorIs(t, err, ErrUnitHeld, "no pin alone is no proof: it delivered just now")
	assert.False(t, reset)
	time.Sleep(quiet)
	reset, err = b.ResetOrphaned(t.Context(), unit)
	require.NoError(t, err)
	require.True(t, reset)
	_, _, gotB := unitConsumer(t, b, unit, nil)
	assert.ElementsMatch(t, []string{"x", "y"}, []string{receive(t, gotB), receive(t, gotB)})

	reset, err = b.ResetOrphaned(t.Context(), unit)
	require.ErrorIs(t, err, ErrUnitHeld, "b holds it now")
	assert.False(t, reset)
}

// unitPin is the pin the server holds on topic's unit in the shipped topology.
func unitPin(t *testing.T, f *natsFixture, topic Topic) string {
	t.Helper()
	p, s := natsRoute(topic, 4, 8)
	c, err := f.admin.Consumer(t.Context(), shippedPartition(p), natsShardDurable("wh-ingest", s))
	require.NoError(t, err)
	return pinnedClient(c.CachedInfo())
}

// A consumer of every unit releases each unit it held once its stop has
// drained, so the next process receives at once.
func TestExternalNATS_WholeConsumerReleasesOnStop(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	c, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.NoError(t, err)
	got := make(chan string, 4)
	stop, _, err := c.Consume(func(m *Message) { assert.NoError(t, m.Ack()); got <- string(m.Data) }, 16)
	require.NoError(t, err)
	require.NoError(t, e.Publish(t.Context(), topic, []byte("x")))
	require.Equal(t, "x", receive(t, got))
	require.NotEmpty(t, unitPin(t, f, topic))
	stop()
	require.Eventually(t, func() bool { return unitPin(t, f, topic) == "" }, 3*time.Second, 20*time.Millisecond,
		"released after the drain, well before the 10s pinned ttl")
}

// A consumer of every unit takes over a unit whose holder is gone with rows
// unacked: it receives them at once, not after ack_wait.
func TestExternalNATS_WholeConsumerTakesOrphansOver(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	a := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	var hold atomic.Bool
	hold.Store(true)
	_, _, gotA := unitConsumer(t, a, shippedPartition(p)+"/"+natsShardDurable("wh-ingest", s), &hold)
	require.NoError(t, a.Publish(t.Context(), topic, []byte("held")))
	require.Equal(t, "held", receive(t, gotA))
	a.nc.Close()
	st, err := f.admin.Stream(t.Context(), shippedPartition(p))
	require.NoError(t, err)
	require.NoError(t, st.UnpinConsumer(t.Context(), natsShardDurable("wh-ingest", s), natsPriorityGroup), "stands in for the pin lapsing")
	const quiet = time.Second
	time.Sleep(quiet) // and for its owner's silence since

	b := f.broker(t, quietAfter(quiet))
	c, err := b.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.NoError(t, err)
	got := make(chan string, 4)
	began := time.Now()
	stop, _, err := c.Consume(func(m *Message) { assert.NoError(t, m.Ack()); got <- string(m.Data) }, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	require.Equal(t, "held", receive(t, got))
	assert.Less(t, time.Since(began), 5*time.Second, "at once, not after the minute's ack_wait")
}

// A consumer whose pin was taken over since it last received leaves the new
// holder's pin alone when it releases.
func TestExternalNATS_ReleaseChecksThePinIsStillItsOwn(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	a, b := f.broker(t, nil), f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	unit := shippedPartition(p) + "/" + natsShardDurable("wh-ingest", s)
	ca, stopA, gotA := unitConsumer(t, a, unit, nil)
	require.NoError(t, a.Publish(t.Context(), topic, []byte("1")))
	require.Equal(t, "1", receive(t, gotA))
	stopA()

	st, err := f.admin.Stream(t.Context(), shippedPartition(p))
	require.NoError(t, err)
	require.NoError(t, st.UnpinConsumer(t.Context(), natsShardDurable("wh-ingest", s), natsPriorityGroup))
	_, _, gotB := unitConsumer(t, b, unit, nil)
	require.NoError(t, b.Publish(t.Context(), topic, []byte("2")))
	require.Equal(t, "2", receive(t, gotB))
	pin := unitPin(t, f, topic)
	require.NotEmpty(t, pin)

	require.NoError(t, ca.(Releaser).Release(t.Context()))
	assert.Equal(t, pin, unitPin(t, f, topic), "A's release leaves B's pin")
}

// Unowned counts the units with rows waiting that no consumer holds.
func TestExternalNATS_Unowned(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	n, err := e.Unowned(t.Context())
	require.NoError(t, err)
	assert.Zero(t, n, "no rows, nothing to own")

	topic := Topic{Tenant: "acme", Table: "events"}
	require.NoError(t, e.Publish(t.Context(), topic, []byte("x")))
	n, err = e.Unowned(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a row nobody pulls")

	var hold atomic.Bool
	hold.Store(true)
	p, s := natsRoute(topic, 4, 8)
	_, _, got := unitConsumer(t, e, shippedPartition(p)+"/"+natsShardDurable("wh-ingest", s), &hold)
	require.Equal(t, "x", receive(t, got))
	n, err = e.Unowned(t.Context())
	require.NoError(t, err)
	assert.Zero(t, n, "held by the consumer that received it")
}

// One unit's consume throughput: a backlog of rows on one shard, drained by
// one consumer that acks each row as it arrives.
func TestExternalNATS_MeasureUnitConsumeThroughput(t *testing.T) {
	measure(t)
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "t"}
	payload := make([]byte, 256)
	const n = 20_000
	var wg sync.WaitGroup
	for w := range 32 {
		wg.Go(func() {
			for i := w; i < n; i += 32 {
				assert.NoError(t, e.Publish(t.Context(), topic, payload))
			}
		})
	}
	wg.Wait()
	p, s := natsRoute(topic, 4, 8)
	c, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable, Units: []string{shippedPartition(p) + "/" + natsShardDurable("wh-ingest", s)}})
	require.NoError(t, err)
	var got atomic.Int64
	done := make(chan struct{})
	start := time.Now()
	stop, _, err := c.Consume(func(m *Message) {
		_ = m.Ack()
		if got.Add(1) == n {
			close(done)
		}
	}, 500)
	require.NoError(t, err)
	t.Cleanup(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		t.Fatalf("received %d of %d", got.Load(), n)
	}
	elapsed := time.Since(start)
	t.Logf("one unit drained %d rows of %d bytes in %s: %.0f rows/s", n, len(payload), elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
}

// A shard durable deleted while its consumer's handler is busy and no pull
// of it is waiting ends delivery with failed, not a silent stall: the next
// pin-keeping pull gets no responders, and the durable is looked up.
func TestExternalNATS_DurableDeletedWhileBusyEndsDelivery(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	stream, durable := shippedPartition(p), natsShardDurable("wh-ingest", s)
	c, err := e.CreateConsumer(t.Context(), ConsumerConfig{
		Durable: workerDurable, Units: []string{stream + "/" + durable},
		MaxHeld: func() int { return 2 },
	})
	require.NoError(t, err)
	busy := make(chan struct{})
	t.Cleanup(func() { close(busy) })
	entered := make(chan struct{}, 8)
	stop, failed, err := c.Consume(func(*Message) { entered <- struct{}{}; <-busy }, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	for i := range 5 {
		require.NoError(t, e.Publish(t.Context(), topic, []byte(strconv.Itoa(i))))
	}
	<-entered // the handler holds the first row; the unit is at its cap

	require.Eventually(t, func() bool {
		cons, err := f.admin.Consumer(t.Context(), stream, durable)
		return err == nil && cons.CachedInfo().NumAckPending == 2 && cons.CachedInfo().NumWaiting == 0
	}, 10*time.Second, 5*time.Millisecond, "a moment with no pull waiting")
	require.NoError(t, f.admin.DeleteConsumer(t.Context(), stream, durable))
	deleted := time.Now()
	select {
	case err := <-failed:
		assert.ErrorIs(t, err, ErrDeliveryEnded)
		assert.ErrorIs(t, err, ErrConsumerNotFound)
		t.Logf("failed %s after the delete: %v", time.Since(deleted).Round(10*time.Millisecond), err)
	case <-time.After(20 * time.Second):
		t.Fatal("a durable deleted under a busy handler stalled silently")
	}
}

// Rows NAKed with a delay that come due while their unit is at its cap come
// back in order. A pin renewal that held a due row back would have the server
// requeue it behind the others (measured with max_bytes-1 renewals: 2 3 0 1),
// so at the cap the renewal takes the row instead.
func TestExternalNATS_RedeliveryOrderKeptAtTheCap(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	c, err := e.CreateConsumer(t.Context(), ConsumerConfig{
		Durable: workerDurable,
		Units:   []string{shippedPartition(p) + "/" + natsShardDurable("wh-ingest", s)}, MaxHeld: func() int { return 4 },
	})
	require.NoError(t, err)
	var mu sync.Mutex
	var got []string
	var held []*Message
	stop, _, err := c.Consume(func(m *Message) {
		mu.Lock()
		got = append(got, string(m.Data))
		held = append(held, m)
		mu.Unlock()
	}, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	for i := range 8 {
		require.NoError(t, e.Publish(t.Context(), topic, []byte(strconv.Itoa(i))))
	}
	take := func(n int) []*Message {
		require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(held) >= n }, 20*time.Second, 10*time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		out := held[:n]
		held = held[n:]
		return out
	}
	for _, m := range take(4) { // 0..3 come due in 3s, while 4..7 fill the cap
		require.NoError(t, m.NakWithDelay(3*time.Second))
	}
	later := take(4)
	time.Sleep(14 * time.Second) // two renewals at least
	for _, m := range later {
		require.NoError(t, m.Ack())
	}
	redelivered := take(4)
	var order []string
	for _, m := range redelivered {
		order = append(order, string(m.Data))
		_ = m.Ack()
	}
	mu.Lock()
	first := slices.Clone(got[:8])
	mu.Unlock()
	assert.Equal(t, []string{"0", "1", "2", "3", "4", "5", "6", "7"}, first)
	assert.Equal(t, []string{"0", "1", "2", "3"}, order, "redelivered in order across the renewals")
}

// A unit whose handler is blocked, at its cap, keeps its pin, and takes no
// more rows to renew it than one ack_wait's worth of renewals (shortened to
// 10s, so 2 here); past that it renews without taking any.
func TestExternalNATS_BlockedHandlerTakesABoundedOvershoot(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	topic := Topic{Tenant: "acme", Table: "events"}
	p, s := natsRoute(topic, 4, 8)
	stream, durable := shippedPartition(p), natsShardDurable("wh-ingest", s)
	c, err := e.CreateConsumer(t.Context(), ConsumerConfig{
		Durable: workerDurable, Units: []string{stream + "/" + durable},
		MaxHeld: func() int { return 2 },
	})
	require.NoError(t, err)
	c.(*externalConsumer).parts[0].ackWait = 2 * renewEvery
	busy := make(chan struct{})
	entered := make(chan struct{}, 16)
	stop, _, err := c.Consume(func(*Message) { entered <- struct{}{}; <-busy }, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	t.Cleanup(func() { close(busy) }) // first, so stop's cleanup is not left waiting
	for i := range 10 {
		require.NoError(t, e.Publish(t.Context(), topic, []byte(strconv.Itoa(i))))
	}
	<-entered
	state := func() (string, uint64) {
		cons, err := f.admin.Consumer(t.Context(), stream, durable)
		require.NoError(t, err)
		return pinnedClient(cons.CachedInfo()), cons.CachedInfo().Delivered.Consumer
	}
	require.Eventually(t, func() bool { _, d := state(); return d >= 2 }, 5*time.Second, 10*time.Millisecond)
	pin, _ := state()
	require.NotEmpty(t, pin)
	// The row in the handler stops counting at its ack_wait (10s here), as
	// the server would redeliver it then, so one more is taken; after that
	// nothing, however many renewals follow.
	time.Sleep(16 * time.Second)
	_, settled := state()
	time.Sleep(11 * time.Second) // two more renewals
	now, delivered := state()
	assert.Equal(t, pin, now, "the pin was kept")
	assert.Equal(t, settled, delivered, "renewals past the bound take no rows")
	assert.LessOrEqual(t, delivered, uint64(5), "the cap of 2, one ack_wait (10s) of renewals at 5s, and the expired row's slot")
}
