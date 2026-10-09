//go:build integration

package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// insertLog is a stub ClickHouse shared by every process: the rows each
// table received, in the order their inserts completed, and who sent them.
type insertLog struct {
	mu      sync.Mutex
	rows    map[string][]int
	writers map[string][]string
	// first and last are when each process's first and last insert of each
	// table completed: proc → table → time.
	first, last map[string]map[string]time.Time
	total       int
}

// server is proc's ClickHouse. gate, when set, runs before an insert is
// recorded, as a slow server would take its time.
func (l *insertLog) server(t *testing.T, proc string, gate func(table string)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		table := r.URL.Query().Get("param_target_table")
		var seqs []int
		sc := bufio.NewScanner(r.Body)
		for sc.Scan() {
			var row []int
			if err := json.Unmarshal(sc.Bytes(), &row); err != nil || len(row) != 1 {
				http.Error(w, "bad row", http.StatusBadRequest)
				return
			}
			seqs = append(seqs, row[0])
		}
		if gate != nil {
			gate(table)
		}
		l.mu.Lock()
		if l.rows == nil {
			l.rows, l.writers = map[string][]int{}, map[string][]string{}
			l.first, l.last = map[string]map[string]time.Time{}, map[string]map[string]time.Time{}
		}
		if l.first[proc] == nil {
			l.first[proc], l.last[proc] = map[string]time.Time{}, map[string]time.Time{}
		}
		now := time.Now()
		if _, ok := l.first[proc][table]; !ok {
			l.first[proc][table] = now
		}
		l.last[proc][table] = now
		l.rows[table] = append(l.rows[table], seqs...)
		if w := l.writers[table]; len(w) == 0 || w[len(w)-1] != proc {
			l.writers[table] = append(w, proc)
		}
		l.total += len(seqs)
		l.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (l *insertLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.total
}

func (l *insertLog) table(name string) []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.rows[name])
}

// inOrder reports whether seqs is exactly 0..n-1: every row once, in order.
func inOrder(seqs []int, n int) bool {
	if len(seqs) != n {
		return false
	}
	for i, s := range seqs {
		if s != i {
			return false
		}
	}
	return true
}

func seqEnvelope(t *testing.T, table string, seq int) []byte {
	t.Helper()
	out, err := json.Marshal(ingest.EventMessage{
		TableName: table, Format: ingest.FormatJSONCompactEachRow,
		Columns: []string{"seq"}, Row: json.RawMessage("[" + strconv.Itoa(seq) + "]"),
	})
	require.NoError(t, err)
	return out
}

// workerProc is one ingest process: the real worker over shard claims, as
// the restricted wavehouse user, writing to its own stub ClickHouse.
type workerProc struct {
	id     string
	broker *mq.ExternalNATS
	stop   func(context.Context) error
	failed <-chan error
}

func startWorkerProc(t *testing.T, url, id string, lease time.Duration, cfg ingest.ClaimConfig, chURL string) *workerProc {
	t.Helper()
	ctx := t.Context()
	p := &workerProc{id: id, broker: shardBroker(t, url)}
	leases, err := p.broker.Leases(ctx, natstest.CoordBucket, id, mq.WithLeaseTimings(lease, lease*7/10, 100*time.Millisecond))
	require.NoError(t, err)
	q, err := ingest.ClaimShards(p.broker, leases, cfg)
	require.NoError(t, err)
	p.stop, p.failed, err = ingest.StartIngestWorker(context.WithoutCancel(ctx), q, &testutil.MockCache{},
		func(tenant.ID) chconn.Target { return chconn.Target{URL: chURL, Database: "db"} }, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.stop(context.Background()) })
	return p
}

func noWorkerFailure(t *testing.T, procs ...*workerProc) {
	t.Helper()
	for _, p := range procs {
		select {
		case err := <-p.failed:
			t.Fatalf("%s failed: %v", p.id, err)
		default:
		}
	}
}

// unitOf publishes data to topic before anything consumes it, and returns
// the unit whose durable the row waits on. The server counts a stored row
// toward its durables on a goroutine of its own, after acking the publish,
// so the counts are read until one has risen.
func unitOf(t *testing.T, srv *natstest.Server, b mq.Broker, topic mq.Topic, data []byte) string {
	t.Helper()
	before := pendingByUnit(t, srv)
	require.NoError(t, b.Publish(t.Context(), topic, data))
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for u, n := range pendingByUnit(t, srv) {
			if n > before[u] {
				return u
			}
		}
	}
	t.Fatalf("no unit took %v's row", topic)
	return ""
}

func pendingByUnit(t *testing.T, srv *natstest.Server) map[string]uint64 {
	t.Helper()
	out := map[string]uint64{}
	for p := range claimPartitions {
		for s := range claimShards {
			stream, durable := "WH_INGEST_"+strconv.Itoa(p), "wh-ingest-"+strconv.Itoa(s)
			c, err := srv.Operator.JetStream().Consumer(t.Context(), stream, durable)
			require.NoError(t, err)
			out[stream+"/"+durable] = c.CachedInfo().NumPending
		}
	}
	return out
}

// unitState is what the server says of a unit: its pin, and its rows
// delivered and awaiting an ack.
func unitState(t *testing.T, srv *natstest.Server, unit string) (pin string, delivered uint64, ackPending int) {
	t.Helper()
	stream, durable, _ := strings.Cut(unit, "/")
	c, err := srv.Operator.JetStream().Consumer(t.Context(), stream, durable)
	require.NoError(t, err)
	info := c.CachedInfo()
	for _, g := range info.PriorityGroups {
		pin = g.PinnedClientID
	}
	return pin, info.Delivered.Consumer, info.NumAckPending
}

// An owner whose ClickHouse takes longer than the durable's pinned_ttl (10s)
// to answer, with its unit's share of held rows full, keeps its pin all the
// while: it takes at most one row past its share per 5s renewal, the unit
// never reads as unowned, and no other process may reset it. Once the insert returns, the rest of
// the table's rows follow, in order.
func TestShardClaims_BlockedOwnerKeepsItsShard(t *testing.T) {
	t.Parallel()
	srv := natstest.Start(t)
	pub := shardBroker(t, srv.URL())
	const rows, share = 20, 8
	unit := unitOf(t, srv, pub, mq.Topic{Tenant: "acme", Table: "stuck"}, seqEnvelope(t, "stuck", 0))
	for i := 1; i < rows; i++ {
		require.NoError(t, pub.Publish(t.Context(), mq.Topic{Tenant: "acme", Table: "stuck"}, seqEnvelope(t, "stuck", i)))
	}

	entered, unblock := make(chan struct{}), make(chan struct{})
	var first sync.Once
	log := &insertLog{}
	ch := log.server(t, "proc-a", func(string) {
		first.Do(func() { close(entered); <-unblock })
	})
	a := startWorkerProc(t, srv.URL(), "proc-a", 3*time.Second, ingest.ClaimConfig{Every: 100 * time.Millisecond, MaxHeld: share}, ch.URL)
	t.Cleanup(func() { closeOnce(unblock) }) // first, should the test end early
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the first insert never started")
	}
	pin, delivered, ackPending := unitState(t, srv, unit)
	require.NotEmpty(t, pin)
	require.GreaterOrEqual(t, ackPending, share, "the unit's share is full")

	other := shardBroker(t, srv.URL())
	blocked := time.Now()
	for time.Since(blocked) < 13*time.Second {
		time.Sleep(500 * time.Millisecond)
		now, d, _ := unitState(t, srv, unit)
		require.Equal(t, pin, now, "the pin moved %s into the block", time.Since(blocked).Round(time.Millisecond))
		require.LessOrEqual(t, d-delivered, uint64(time.Since(blocked)/(5*time.Second))+1, "at most one row past the share per renewal")
		if time.Since(blocked) > 11*time.Second {
			n, err := a.broker.Unowned(t.Context())
			require.NoError(t, err)
			assert.Zero(t, n, "a blocked owner's unit is not unowned")
			_, err = other.ResetOrphaned(t.Context(), unit)
			assert.ErrorIs(t, err, mq.ErrUnitHeld, "nor may another process reset it")
		}
	}
	_, d, ackPending := unitState(t, srv, unit)
	assert.Equal(t, int(d), ackPending, "every row delivered is still the owner's, none reset")
	t.Logf("%d rows past the share delivered over %s of renewals", d-delivered, time.Since(blocked).Round(time.Second))

	blockedFor := time.Since(blocked)
	closeOnce(unblock)
	resumed := time.Now()
	require.Eventually(t, func() bool { return len(log.table("stuck")) == rows }, 30*time.Second, 50*time.Millisecond)
	t.Logf("blocked %s with its pin kept throughout; the other %d rows followed %s after the insert returned",
		blockedFor.Round(100*time.Millisecond), rows-share, time.Since(resumed).Round(10*time.Millisecond))
	assert.True(t, inOrder(log.table("stuck"), rows), "%v", log.table("stuck"))
	noWorkerFailure(t, a)
}

// A table whose ClickHouse hangs fills only its own unit's share of held
// rows: a table on another unit of the same process keeps flowing.
func TestShardClaims_StuckShardDoesNotStallTheOthers(t *testing.T) {
	t.Parallel()
	const (
		share = 8
		// more rows take more/share batch windows: the healthy unit holds at
		// most share rows, too few to fill a batch, so each batch is written
		// as its window (the worker's 5s) closes.
		more   = 3 * share
		window = 5 * time.Second
	)
	srv := natstest.Start(t)
	pub := shardBroker(t, srv.URL())
	stuck := mq.Topic{Tenant: "acme", Table: "stuck"}
	stuckUnit := unitOf(t, srv, pub, stuck, seqEnvelope(t, "stuck", 0))
	for i := 1; i < 20; i++ {
		require.NoError(t, pub.Publish(t.Context(), stuck, seqEnvelope(t, "stuck", i)))
	}
	var healthy mq.Topic
	for i := 0; healthy.Table == ""; i++ {
		topic := mq.Topic{Tenant: "acme", Table: "healthy" + strconv.Itoa(i)}
		if unitOf(t, srv, pub, topic, seqEnvelope(t, topic.Table, 0)) != stuckUnit {
			healthy = topic
		}
	}

	entered, unblock := make(chan struct{}), make(chan struct{})
	var first sync.Once
	log := &insertLog{}
	ch := log.server(t, "proc-a", func(table string) {
		if table == "stuck" {
			first.Do(func() { close(entered) })
			<-unblock
		}
	})
	a := startWorkerProc(t, srv.URL(), "proc-a", 3*time.Second, ingest.ClaimConfig{Every: 100 * time.Millisecond, MaxHeld: share}, ch.URL)
	t.Cleanup(func() { closeOnce(unblock) }) // first, should the test end early
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the stuck table's insert never started")
	}
	_, _, held := unitState(t, srv, stuckUnit)
	require.GreaterOrEqual(t, held, share, "the stuck unit's share is full")
	// Row 0 opened the healthy table's first window when the worker started,
	// as the stuck table's rows did; the rows start a window of their own
	// once it has closed.
	require.Eventually(t, func() bool { return len(log.table(healthy.Table)) == 1 }, 30*time.Second, 50*time.Millisecond,
		"the healthy table's first row was never written")

	for i := 1; i <= more; i++ {
		require.NoError(t, pub.Publish(t.Context(), healthy, seqEnvelope(t, healthy.Table, i)))
	}
	began := time.Now()
	// One window more than the rows take, for the inserts, acks and fetches
	// between windows.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, log.table(healthy.Table), more+1, "the healthy table stalled behind the stuck one")
	}, (more/share+1)*window, 50*time.Millisecond)
	t.Logf("with %s's share full, %s's %d rows were written %s after they were published",
		stuck.Table, healthy.Table, more, time.Since(began).Round(10*time.Millisecond))
	assert.True(t, inOrder(log.table(healthy.Table), more+1))
	assert.Empty(t, log.table("stuck"), "the stuck insert has not returned")
	closeOnce(unblock)
	noWorkerFailure(t, a)
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// Under continuous publishing, every table's rows reach ClickHouse exactly
// once and in order while a second process joins (half the units hand over)
// and the first then stops cleanly (the rest move): the process giving a
// unit up writes what it holds before the next owner receives anything.
// The first process's ClickHouse takes longer than a batch window, so a
// unit released before its rows were written would have the next owner's
// rows land first. And a clean stop leaves nothing unsettled behind, so the
// next owner binds its units at once rather than waiting to judge them
// orphaned.
func TestShardClaims_TableOrderAcrossHandoverAndStop(t *testing.T) {
	t.Parallel()
	srv := natstest.Start(t)
	pub := shardBroker(t, srv.URL())
	log := &insertLog{}
	chA := log.server(t, "proc-a", func(string) { time.Sleep(6 * time.Second) })
	chB := log.server(t, "proc-b", nil)
	const lease, tables = 3 * time.Second, 16
	cfg := ingest.ClaimConfig{Every: 100 * time.Millisecond}

	a := startWorkerProc(t, srv.URL(), "proc-a", lease, cfg, chA.URL)
	var published atomic.Int64
	quit := make(chan struct{})
	var publisher sync.WaitGroup
	publisher.Go(func() {
		for seq := 0; ; seq++ {
			select {
			case <-quit:
				return
			default:
			}
			for i := range tables {
				table := "t" + strconv.Itoa(i)
				if err := pub.Publish(context.Background(), mq.Topic{Tenant: "acme", Table: table}, seqEnvelope(t, table, seq)); err != nil {
					t.Errorf("publish %s/%d: %v", table, seq, err)
					return
				}
			}
			published.Store(int64(seq + 1))
			time.Sleep(20 * time.Millisecond)
		}
	})

	time.Sleep(3 * time.Second)
	b := startWorkerProc(t, srv.URL(), "proc-b", lease, cfg, chB.URL)
	time.Sleep(15 * time.Second) // membership moves, and each moved unit's rows are written first
	stopping := time.Now()
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, a.stop(stopCtx))
	stopped := time.Now()
	t.Logf("proc-a stopped cleanly in %s", stopped.Sub(stopping).Round(10*time.Millisecond))
	time.Sleep(6 * time.Second)
	close(quit)
	publisher.Wait()

	rows := int(published.Load())
	require.Eventually(t, func() bool { return log.count() >= rows*tables }, 45*time.Second, 100*time.Millisecond,
		"inserted %d of %d rows", log.count(), rows*tables)
	time.Sleep(time.Second) // anything written twice would show now
	moved := 0
	for i := range tables {
		table := "t" + strconv.Itoa(i)
		assert.True(t, inOrder(log.table(table), rows), "%s: %d rows, want 0..%d in order", table, len(log.table(table)), rows-1)
		log.mu.Lock()
		if len(log.writers[table]) > 1 {
			moved++
		}
		assert.LessOrEqual(t, len(log.writers[table]), 3, "%s changed writer more than a handover and a stop explain: %v", table, log.writers[table])
		log.mu.Unlock()
	}
	t.Logf("%d rows in each of %d tables; %d tables changed writer", rows, tables, moved)
	assert.Positive(t, moved, "some table moved between the processes")

	// The tables proc-a still wrote while stopping moved at the stop: proc-b's
	// first write of each follows within its batch window (5s), not after a
	// wait for the unit to count as orphaned (pinned_ttl, 10s) on top.
	log.mu.Lock()
	defer log.mu.Unlock()
	var gaps []time.Duration
	for i := range tables {
		table := "t" + strconv.Itoa(i)
		if last, ok := log.last["proc-a"][table]; ok && !last.Before(stopping) {
			gaps = append(gaps, log.first["proc-b"][table].Sub(stopped))
		}
	}
	require.NotEmpty(t, gaps, "some tables moved at the stop")
	slices.Sort(gaps)
	t.Logf("%d tables moved at the stop; proc-b's first write of each came %s to %s after it", len(gaps), gaps[0].Round(10*time.Millisecond), gaps[len(gaps)-1].Round(10*time.Millisecond))
	assert.Less(t, gaps[len(gaps)-1], 7*time.Second, "a unit the stop gave up waited to be judged orphaned")
	noWorkerFailure(t, b)
}
