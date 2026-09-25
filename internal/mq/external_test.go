//go:build integration

package mq

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
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
	stream := shippedPartition(partitionOf(topic.Tenant, 4))

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
	stream := shippedPartition(partitionOf(topic.Tenant, 4))
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
	stream := shippedPartition(partitionOf(topic.Tenant, 4))
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
		Topology: NATSTopology{Partitions: 4}, TopologyWait: 500 * time.Millisecond,
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
// repair; a history source re-attaching after a restart is not a fault but
// shows on the source gauges.
func TestExternalNATS_Recheck(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, func(c *NATSConfig) {
		c.recheckEvery = 50 * time.Millisecond
		c.sourcesEvery = 50 * time.Millisecond
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

	// After a NATS restart the history's sources take ~10s to re-attach, and
	// read as silent until then: the source gauges show it, and it is no
	// topology fault.
	f.restart(t)
	silent := func() bool {
		for _, s := range *e.sources.Load() {
			if s.active > time.Second {
				return true
			}
		}
		return false
	}
	require.Eventually(t, func() bool { return e.connected.Load() && silent() }, 10*time.Second, 10*time.Millisecond,
		"no source read as silent after the restart")
	for range 20 {
		assert.True(t, e.topologyOK.Load(), "a source re-attaching is not a topology fault")
		time.Sleep(50 * time.Millisecond)
	}
}

// The gauges report the connection, the topology and each history source.
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
	assert.Equal(t, []float64{0, 0, 0, 0}, got["wavehouse_mq_history_source_lag"])
	require.Len(t, got["wavehouse_mq_history_source_last_active_seconds"], 4)
	for _, v := range got["wavehouse_mq_history_source_last_active_seconds"] {
		assert.GreaterOrEqual(t, v, 0.0, "every source attached")
	}
}

// PurgeAcked removes nothing, and warns once for a tenant whose gap window
// the history cannot hold.
func TestExternalNATS_PurgeAckedWarnsOnAShortHistory(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	maxAge := time.Duration(e.historyMaxAge.Load())
	require.Positive(t, maxAge, "the shipped history has a max_age")

	purged, err := e.PurgeAcked(t.Context(), workerDurable, map[tenant.ID]time.Time{
		"acme":   time.Now().Add(-2 * maxAge),
		"globex": time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)
	assert.False(t, purged)
	_, acme := e.warnedGap.Load(tenant.ID("acme"))
	_, globex := e.warnedGap.Load(tenant.ID("globex"))
	assert.True(t, acme)
	assert.False(t, globex)
}

// CreateConsumer finds the operator's durable and holds it to the worker's
// ask; it never creates one.
func TestExternalNATS_CreateConsumerChecksTheDurable(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)

	_, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable, AckWait: time.Hour})
	require.ErrorContains(t, err, "ack_wait")
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: "someone-else"})
	require.ErrorIs(t, err, ErrConsumerNotFound)
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: DefaultNATSIngestConsumer, AckWait: time.Minute})
	require.NoError(t, err)

	require.NoError(t, f.admin.DeleteConsumer(t.Context(), shippedPartition(0), DefaultNATSIngestConsumer))
	_, err = e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.ErrorIs(t, err, ErrConsumerNotFound)
	_, err = f.admin.Consumer(t.Context(), shippedPartition(0), DefaultNATSIngestConsumer)
	require.ErrorIs(t, err, jetstream.ErrConsumerNotFound, "the broker must not recreate the durable")
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
	denied("delete the ingest durable", call(ctx, func(ctx context.Context) error {
		return js.DeleteConsumer(ctx, partition, DefaultNATSIngestConsumer)
	}))

	_, err = f.admin.Stream(ctx, "ROGUE")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
	_, err = f.admin.Consumer(ctx, partition, DefaultNATSIngestConsumer)
	require.NoError(t, err)
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
// partition and the history's source. Design risk 3 moves the hub off the
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

// A source that never attached reads -1 on its gauge: the server reports it
// as -1ns, which Seconds() would pass on as -1e-9.
func TestExternalNATS_ActiveSeconds(t *testing.T) {
	t.Parallel()
	assert.InDelta(t, -1.0, activeSeconds(-1), 0)
	assert.InDelta(t, 0.5, activeSeconds(500*time.Millisecond), 1e-9)
}

// The duplicate window must cover every attempt of a retried publish, not
// just two publish timeouts: a window of exactly 2 × PublishTimeout is too
// short for the last retry.
func TestExternalNATS_DuplicateWindowCoversEveryRetry(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	topo := NATSTopology{Partitions: 4, PublishTimeout: 30 * time.Second}
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
