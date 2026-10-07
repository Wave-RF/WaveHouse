//go:build integration

package tests

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The shipped topology's partitions and shards: 32 units.
const (
	claimPartitions = 4
	claimShards     = 8
)

// shardLog records, per topic, the processes that received its rows.
type shardLog struct {
	mu   sync.Mutex
	rows map[mq.Topic]map[string]int
	at   map[mq.Topic]time.Time // the last row's arrival
}

func (l *shardLog) add(topic mq.Topic, proc string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rows == nil {
		l.rows, l.at = map[mq.Topic]map[string]int{}, map[mq.Topic]time.Time{}
	}
	if l.rows[topic] == nil {
		l.rows[topic] = map[string]int{}
	}
	l.rows[topic][proc]++
	l.at[topic] = time.Now()
}

func (l *shardLog) count(topic mq.Topic) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.rows[topic] {
		n += c
	}
	return n
}

func (l *shardLog) countBy(topic mq.Topic, proc string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rows[topic][proc]
}

func (l *shardLog) writers(topic mq.Topic) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for p := range l.rows[topic] {
		out = append(out, p)
	}
	return out
}

func (l *shardLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows, l.at = nil, nil
}

// shardProc is one ingest process's claims over a shared NATS, as the
// restricted wavehouse user, with a lease of its own timings.
type shardProc struct {
	id     string
	broker *mq.ExternalNATS
	stop   func()
	failed <-chan error
	hold   atomic.Bool // leave rows unacked, as a stuck insert would
	held   atomic.Int64
}

func shardBroker(t *testing.T, url string) *mq.ExternalNATS {
	t.Helper()
	pw := filepath.Join(t.TempDir(), "nats-password")
	require.NoError(t, os.WriteFile(pw, []byte(natstest.Password(natstest.WaveHouseUser)), 0o600))
	broker, err := mq.NewNATS(t.Context(), mq.NATSConfig{
		URLs: []string{url}, User: natstest.WaveHouseUser, PasswordFile: pw,
		Topology: mq.NATSTopology{
			Prefix: "wh", Partitions: claimPartitions, Shards: claimShards, IngestConsumer: "wh-ingest",
			PublishTimeout: 5 * time.Second, CoordBucket: natstest.CoordBucket,
		},
		TopologyWait: 10 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	return broker
}

func startShardProc(t *testing.T, url, id string, lease time.Duration, log *shardLog) *shardProc {
	t.Helper()
	ctx := t.Context()
	p := &shardProc{id: id, broker: shardBroker(t, url)}
	leases, err := p.broker.Leases(ctx, natstest.CoordBucket, id, mq.WithLeaseTimings(lease, lease*7/10, 100*time.Millisecond))
	require.NoError(t, err)
	q, err := ingest.ClaimShards(p.broker, leases, ingest.ClaimConfig{Every: 100 * time.Millisecond, Handover: 2 * time.Second})
	require.NoError(t, err)
	cons, err := q.CreateConsumer(ctx, mq.ConsumerConfig{Durable: ingest.BufferConsumerName, AckWait: 60 * time.Second, MaxAckPending: 10_000})
	require.NoError(t, err)
	stop, failed, err := cons.Consume(func(m *mq.Message) {
		log.add(m.Topic(), id)
		if p.hold.Load() {
			p.held.Add(1)
			return
		}
		_ = m.DoubleAck(context.Background())
	}, 100)
	require.NoError(t, err)
	t.Cleanup(stop)
	p.stop, p.failed = stop, failed
	return p
}

func shardTopics(id tenant.ID, n int) []mq.Topic {
	out := make([]mq.Topic, n)
	for i := range out {
		out[i] = mq.Topic{Tenant: id, Table: "t" + strconv.Itoa(i)}
	}
	return out
}

func publishRows(t *testing.T, b mq.Broker, topics []mq.Topic, rows int) {
	t.Helper()
	for _, topic := range topics {
		for i := range rows {
			require.NoError(t, b.Publish(t.Context(), topic, []byte(strconv.Itoa(i))))
		}
	}
}

// everyRowOnce publishes rows to topics and reports, once every row arrived,
// whether each reached one of procs and none arrived twice.
func everyRowOnce(t *testing.T, b mq.Broker, log *shardLog, topics []mq.Topic, procs ...string) {
	t.Helper()
	log.reset()
	publishRows(t, b, topics, 3)
	require.Eventually(t, func() bool {
		for _, topic := range topics {
			if log.count(topic) < 3 {
				return false
			}
		}
		return true
	}, 30*time.Second, 20*time.Millisecond, "every row arrives")
	for _, topic := range topics {
		assert.Subset(t, procs, log.writers(topic), "%v", topic)
		assert.Equal(t, 3, log.count(topic), "%v: no row twice", topic)
	}
}

// oneWriterEach is everyRowOnce, and also reports whether each topic's rows
// reached exactly one of procs.
func oneWriterEach(t *testing.T, b mq.Broker, log *shardLog, topics []mq.Topic, procs ...string) {
	t.Helper()
	everyRowOnce(t, b, log, topics, procs...)
	per := map[string]int{}
	for _, topic := range topics {
		w := log.writers(topic)
		require.Len(t, w, 1, "%v written by %v", topic, w)
		per[w[0]]++
	}
	t.Logf("tables per process: %v", per)
}

func pinnedUnits(t *testing.T, srv *natstest.Server) int {
	t.Helper()
	n := 0
	for p := range claimPartitions {
		for s := range claimShards {
			c, err := srv.Operator.JetStream().Consumer(t.Context(), "WH_INGEST_"+strconv.Itoa(p), "wh-ingest-"+strconv.Itoa(s))
			require.NoError(t, err)
			for _, g := range c.CachedInfo().PriorityGroups {
				if g.PinnedClientID != "" {
					n++
				}
			}
		}
	}
	return n
}

// Scaling 1 → 3 → 2 processes: every table is written by one process once
// each step has settled, a clean stop hands its shards on well inside the
// lease, and no row is received twice.
func TestShardClaims_ScaleOneThreeTwo(t *testing.T) {
	srv := natstest.Start(t)
	log := &shardLog{}
	const lease = 3 * time.Second
	topics := append(shardTopics("acme", 30), shardTopics("globex", 30)...)

	a := startShardProc(t, srv.URL(), "proc-a", lease, log)
	oneWriterEach(t, a.broker, log, topics, "proc-a")
	assert.Positive(t, pinnedUnits(t, srv))

	startShardProc(t, srv.URL(), "proc-b", lease, log)
	c := startShardProc(t, srv.URL(), "proc-c", lease, log)
	time.Sleep(3 * time.Second) // membership and handovers settle
	oneWriterEach(t, a.broker, log, topics, "proc-a", "proc-b", "proc-c")

	stopped := time.Now()
	c.stop()
	// The first rows after the stop time it, and are not held to one writer
	// each: as the survivors take proc-c's shards their even share also
	// moves one of proc-a's to proc-b, and a table whose rows arrive across
	// that handover is written by one and then the other.
	everyRowOnce(t, a.broker, log, topics, "proc-a", "proc-b")
	took := time.Since(stopped)
	t.Logf("after a clean stop every table was written again within %s (lease %s)", took.Round(10*time.Millisecond), lease)
	assert.Less(t, took, 3*lease, "a clean stop releases at once")
	time.Sleep(3 * time.Second) // membership and handovers settle
	oneWriterEach(t, a.broker, log, topics, "proc-a", "proc-b")
	noShardFailure(t, a)
}

// A process whose connection dies holding rows unacked: once its lease runs
// out and its pins lapse, a survivor takes its shards over, resets them, and
// receives the held rows at once, not after ack_wait (a minute here), and
// none of the rows it acked before.
func TestShardClaims_CrashWithRowsInFlight(t *testing.T) {
	srv := natstest.Start(t)
	log := &shardLog{}
	const lease = 3 * time.Second
	proxy := startTCPProxy(t, srv.URL())

	victim := startShardProc(t, proxy.url, "proc-a", lease, log)
	topics := shardTopics("acme", 40)
	oneWriterEach(t, victim.broker, log, topics, "proc-a")

	survivor := startShardProc(t, srv.URL(), "proc-b", lease, log)
	time.Sleep(3 * time.Second)
	oneWriterEach(t, survivor.broker, log, topics, "proc-a", "proc-b")

	// The victim holds what it receives now, then dies.
	victim.hold.Store(true)
	log.reset()
	publishRows(t, survivor.broker, topics, 1)
	require.Eventually(t, func() bool {
		n := 0
		for _, topic := range topics {
			n += log.count(topic)
		}
		return n == len(topics)
	}, 10*time.Second, 20*time.Millisecond)
	held := victim.held.Load()
	require.Positive(t, held, "the victim holds some rows")
	killed := time.Now()
	proxy.kill()

	require.Eventually(t, func() bool {
		for _, topic := range topics {
			if !slices.Contains(log.writers(topic), "proc-b") {
				return false
			}
		}
		return true
	}, 45*time.Second, 50*time.Millisecond, "the survivor receives every table's row")
	took := time.Since(killed)
	t.Logf("the victim held %d rows; the survivor had all of them %s after the kill (lease %s, pinned ttl 10s, ack_wait 60s)", held, took.Round(100*time.Millisecond), lease)
	assert.Less(t, took, 30*time.Second, "at takeover, not after ack_wait")
	for _, topic := range topics {
		assert.LessOrEqual(t, log.countBy(topic, "proc-b"), 1, "%v: an acked row replayed", topic)
	}
	noShardFailure(t, survivor)
}

func noShardFailure(t *testing.T, procs ...*shardProc) {
	t.Helper()
	for _, p := range procs {
		select {
		case err := <-p.failed:
			t.Fatalf("%s failed: %v", p.id, err)
		default:
		}
	}
}

// tcpProxy forwards a client to the NATS server until kill, which ends every
// connection and refuses new ones: to the client, the server is gone.
type tcpProxy struct {
	url   string
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
	dead  atomic.Bool
}

func startTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	u := target[len("nats://"):]
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &tcpProxy{url: "nats://" + ln.Addr().String(), ln: ln}
	t.Cleanup(p.kill)
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			var d net.Dialer
			out, err := d.DialContext(context.Background(), "tcp", u)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.mu.Lock()
			if p.dead.Load() {
				p.mu.Unlock()
				_ = in.Close()
				_ = out.Close()
				return
			}
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return p
}

func (p *tcpProxy) kill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dead.Swap(true) {
		return
	}
	_ = p.ln.Close()
	for _, c := range p.conns {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			continue
		}
	}
}
