package ingest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// fakeShards is a sharded queue every test process shares: it records, per
// unit, which process consumes it and what each process did with it.
type fakeShards struct {
	units, extra []string

	mu        sync.Mutex
	consumers map[string]map[string]*fakeUnitConsumer // unit → process → consumer
	events    []string                                // "<process> <verb> <unit>"
	unowned   []string                                // the processes that asked
	pinned    map[string]bool                         // units a gone owner's pin still holds
}

func newFakeShards(n int, extra ...string) *fakeShards {
	f := &fakeShards{consumers: map[string]map[string]*fakeUnitConsumer{}, extra: extra}
	for i := range n {
		f.units = append(f.units, fmt.Sprintf("S/u-%02d", i))
	}
	return f
}

func (f *fakeShards) record(proc, verb, unit string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, proc+" "+verb+" "+unit)
}

func (f *fakeShards) eventLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

// owners maps each unit to the processes consuming it now.
func (f *fakeShards) owners() map[string][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]string{}
	for u, by := range f.consumers {
		for proc, c := range by {
			if c.running.Load() {
				out[u] = append(out[u], proc)
			}
		}
	}
	return out
}

// split reports whether every unit has exactly one consumer, among procs,
// none of them above an even share.
func (f *fakeShards) split(procs ...string) bool {
	owners := f.owners()
	units := slices.Concat(f.units, f.extra)
	limit := (len(units) + len(procs) - 1) / len(procs)
	load := map[string]int{}
	for _, u := range units {
		if len(owners[u]) != 1 || !slices.Contains(procs, owners[u][0]) {
			return false
		}
		load[owners[u][0]]++
	}
	for _, n := range load {
		if n > limit {
			return false
		}
	}
	return true
}

// deliver hands one row to unit u's running consumer, if any, and returns
// whether one took it. It runs the handler on a goroutine of its own, as a
// delivery goroutine would; settled fires when the row is settled.
func (f *fakeShards) deliver(u string, settled func()) bool {
	f.mu.Lock()
	var c *fakeUnitConsumer
	for _, cand := range f.consumers[u] {
		if cand.running.Load() {
			c = cand
		}
	}
	f.mu.Unlock()
	if c == nil {
		return false
	}
	ack := func() error {
		if settled != nil {
			settled()
		}
		return nil
	}
	m := mq.NewMessage(context.Background(), mq.Topic{Tenant: "acme", Table: u}, []byte(u), time.Now(),
		func(context.Context) error { return ack() }, ack, ack)
	go c.handler(m)
	return true
}

// deliverFailing is deliver with every settle answered by err.
func (f *fakeShards) deliverFailing(u string, err error) bool {
	f.mu.Lock()
	var c *fakeUnitConsumer
	for _, cand := range f.consumers[u] {
		if cand.running.Load() {
			c = cand
		}
	}
	f.mu.Unlock()
	if c == nil {
		return false
	}
	fail := func() error { return err }
	m := mq.NewMessage(context.Background(), mq.Topic{Tenant: "acme", Table: u}, []byte(u), time.Now(),
		func(context.Context) error { return fail() }, fail, fail)
	go c.handler(m)
	return true
}

// fakeProc is one process's view of the shared fake.
type fakeProc struct {
	f    *fakeShards
	name string
	// failCreate, when set, fails binding that unit.
	failCreate atomic.Pointer[string]
}

func (p *fakeProc) IngestUnits() ([]string, []string) { return p.f.units, p.f.extra }

func (p *fakeProc) ResetOrphaned(_ context.Context, unit string) (bool, error) {
	p.f.mu.Lock()
	held := p.f.pinned[unit]
	p.f.mu.Unlock()
	if held {
		return false, mq.ErrUnitHeld
	}
	p.f.record(p.name, "reset", unit)
	return true, nil
}

func (p *fakeProc) Unowned(context.Context) (int, error) {
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	p.f.unowned = append(p.f.unowned, p.name)
	return 0, nil
}

func (p *fakeProc) DeadLetter(context.Context, *mq.Message, ...mq.PublishOpt) error { return nil }

func (p *fakeProc) CreateConsumer(_ context.Context, cfg mq.ConsumerConfig) (mq.Consumer, error) {
	if len(cfg.Units) != 1 {
		return &fakeUnitConsumer{}, nil // the boot check of every unit
	}
	u := cfg.Units[0]
	if bad := p.failCreate.Load(); bad != nil && *bad == u {
		return nil, errors.New("no such durable")
	}
	p.f.record(p.name, "create", u)
	return &fakeUnitConsumer{p: p, unit: u, failed: make(chan error, 1)}, nil
}

type fakeUnitConsumer struct {
	p       *fakeProc
	unit    string
	handler func(*mq.Message)
	failed  chan error
	running atomic.Bool
}

func (c *fakeUnitConsumer) Consume(handler func(*mq.Message), _ int) (func(), <-chan error, error) {
	c.handler = handler
	c.running.Store(true)
	c.p.f.mu.Lock()
	if c.p.f.consumers[c.unit] == nil {
		c.p.f.consumers[c.unit] = map[string]*fakeUnitConsumer{}
	}
	c.p.f.consumers[c.unit][c.p.name] = c
	c.p.f.mu.Unlock()
	return func() {
		if c.running.CompareAndSwap(true, false) {
			c.p.f.record(c.p.name, "stop", c.unit)
		}
	}, c.failed, nil
}

func (c *fakeUnitConsumer) Release(context.Context) error {
	c.p.f.record(c.p.name, "release", c.unit)
	return nil
}

// claimProc is one process's claims over the fake.
type claimProc struct {
	*fakeProc
	stop   func()
	failed <-chan error
	loop   *claimLoop
}

func fastClaims(cfg ClaimConfig) ClaimConfig {
	if cfg.Every == 0 {
		cfg.Every = 20 * time.Millisecond
	}
	if cfg.Handover == 0 {
		cfg.Handover = time.Second
	}
	if cfg.UnownedEvery == 0 {
		cfg.UnownedEvery = time.Millisecond
	}
	return cfg
}

func startClaims(t *testing.T, f *fakeShards, c *coord.Local, name string, cfg ClaimConfig, handler func(*mq.Message)) *claimProc {
	t.Helper()
	p := &fakeProc{f: f, name: name}
	q, err := ClaimShards(p, c, fastClaims(cfg))
	require.NoError(t, err)
	cons, err := q.CreateConsumer(t.Context(), mq.ConsumerConfig{Durable: BufferConsumerName})
	require.NoError(t, err)
	if handler == nil {
		handler = func(m *mq.Message) { _ = m.Ack() }
	}
	stop, failed, err := cons.Consume(handler, 100)
	require.NoError(t, err)
	t.Cleanup(stop)
	return &claimProc{fakeProc: p, stop: stop, failed: failed, loop: cons.(*claimingConsumer).loop.Load()}
}

func noFailure(t *testing.T, procs ...*claimProc) {
	t.Helper()
	for _, p := range procs {
		select {
		case err := <-p.failed:
			t.Fatalf("%s failed: %v", p.name, err)
		default:
		}
	}
}

func TestClaimShards_Refuses(t *testing.T) {
	t.Parallel()
	f := newFakeShards(2)
	_, err := ClaimShards(&fakeProc{f: f}, noObserver{coord.NewLocal()}, ClaimConfig{})
	require.ErrorContains(t, err, "observes leases")
	_, err = ClaimShards(plainQueue{}, coord.NewLocal(), ClaimConfig{})
	require.ErrorContains(t, err, "sharded queue")
	q, err := ClaimShards(&fakeProc{f: f}, coord.NewLocal(), ClaimConfig{})
	require.NoError(t, err)
	_, err = q.CreateConsumer(t.Context(), mq.ConsumerConfig{Units: []string{"S/u-00"}})
	require.ErrorContains(t, err, "chooses its own units")
}

type noObserver struct{ coord.Coordinator }

type plainQueue struct{}

func (plainQueue) CreateConsumer(context.Context, mq.ConsumerConfig) (mq.Consumer, error) {
	return nil, nil //nolint:nilnil // never called
}
func (plainQueue) DeadLetter(context.Context, *mq.Message, ...mq.PublishOpt) error { return nil }

// Three processes split the units with none shared, none left over, and no
// process above an even share.
func TestClaims_SplitWithoutOverlap(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(12), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	b := startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	d := startClaims(t, f, c.Peer(), "c", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a", "b", "c") }, 5*time.Second, 10*time.Millisecond, "%v", f.owners())
	noFailure(t, a, b, d)
}

// A process joining takes its share, handed over by the others; one that
// stops releases its units at once, and the others take them.
func TestClaims_JoinAndStopHandOver(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(12), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	b := startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a", "b") }, 5*time.Second, 10*time.Millisecond, "%v", f.owners())
	for _, e := range f.eventLog() {
		if strings.HasPrefix(e, "b create ") {
			unit := strings.TrimPrefix(e, "b create ")
			assert.Contains(t, f.eventLog(), "a release "+unit, "a released %s before b took it", unit)
		}
	}
	b.stop()
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for _, e := range f.eventLog() {
		if strings.HasPrefix(e, "b create ") {
			assert.Contains(t, f.eventLog(), "b release "+strings.TrimPrefix(e, "b create "))
		}
	}
	noFailure(t, a)
}

// A unit given up waits for the rows it delivered to settle before it is
// released, bounded by Handover.
func TestClaims_HandoverWaitsForDeliveredRows(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(4), coord.NewLocal()
	var pending sync.WaitGroup
	release := make(chan struct{})
	a := startClaims(t, f, c, "a", ClaimConfig{Handover: 5 * time.Second}, func(m *mq.Message) {
		go func() { <-release; _ = m.Ack(); pending.Done() }()
	})
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for _, u := range f.units {
		pending.Add(1)
		require.True(t, f.deliver(u, nil))
	}
	require.Eventually(t, func() bool { return a.loop.held.count() == 4 }, 5*time.Second, 10*time.Millisecond)
	startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	require.Eventually(t, func() bool {
		return slices.ContainsFunc(f.eventLog(), func(e string) bool { return strings.HasPrefix(e, "a stop ") })
	},
		5*time.Second, 10*time.Millisecond, "a gives some units up")
	time.Sleep(300 * time.Millisecond)
	assert.False(t, slices.ContainsFunc(f.eventLog(), func(e string) bool { return strings.HasPrefix(e, "a release ") }),
		"no release while its rows are unsettled: %v", f.eventLog())
	close(release)
	pending.Wait()
	require.Eventually(t, func() bool { return f.split("a", "b") }, 5*time.Second, 10*time.Millisecond)
	assert.Zero(t, a.loop.held.count(), "every settled row gave its slot back")
}

// A process that loses its membership lease gives every unit up.
func TestClaims_LostMembershipGivesEveryUnitUp(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(4), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	// The lease ends under a (the in-process backend never expires one).
	require.NoError(t, c.Close(t.Context()))
	require.Eventually(t, func() bool { return len(f.owners()) == 0 }, 5*time.Second, 10*time.Millisecond, "%v", f.owners())
	noFailure(t, a)
}

// More processes than units: the extra ones hold nothing, and fail nothing.
func TestClaims_MoreProcessesThanUnits(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(2), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	b := startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	d := startClaims(t, f, c.Peer(), "c", ClaimConfig{}, nil)
	require.Eventually(t, func() bool {
		o := f.owners()
		return len(o) == 2 && len(o["S/u-00"]) == 1 && len(o["S/u-01"]) == 1
	}, 5*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	held := 0
	for _, p := range []*claimProc{a, b, d} {
		held += int(p.loop.ownedGauge.Load())
	}
	assert.Equal(t, 2, held)
	noFailure(t, a, b, d)
}

// crash stops p's claims the way a killed process ends: no release, and its
// membership lease gone (the in-process backend never expires one).
func crash(t *testing.T, p *claimProc) {
	t.Helper()
	l := p.loop
	l.stopOnce.Do(func() { // stop is then a no-op
		l.stopped.Store(true)
		l.cancel()
		close(l.quit)
		<-l.done
		require.NoError(t, l.member.Resign(context.Background()))
		for _, cl := range l.owned {
			cl.stop() // its fake consumers stop being chosen; nothing is released
		}
	})
}

// A unit whose owner died is reset before its new owner binds it, so the
// dead owner's unsettled rows come back at once; a handover to a joining
// process resets nothing.
func TestClaims_TakeoverResetsBeforeBinding(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(8), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	b := startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a", "b") }, 5*time.Second, 10*time.Millisecond)
	assert.False(t, slices.ContainsFunc(f.eventLog(), func(e string) bool { return strings.HasPrefix(e, "b reset ") }),
		"a handover is no takeover: %v", f.eventLog())

	var aUnits []string
	for u, owners := range f.owners() {
		if owners[0] == "a" {
			aUnits = append(aUnits, u)
		}
	}
	crash(t, a)
	require.Eventually(t, func() bool {
		for _, u := range aUnits {
			if !slices.Contains(f.eventLog(), "b create "+u) || !slices.Contains(f.eventLog(), "b reset "+u) {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond, "%v", f.eventLog())
	log := f.eventLog()
	for _, u := range aUnits {
		reset := slices.Index(log, "b reset "+u)
		bound := -1
		for i, e := range log {
			if e == "b create "+u {
				bound = i
			}
		}
		assert.Less(t, reset, bound, "%s reset before bound", u)
	}
	noFailure(t, b)
}

// A configured unit that cannot be bound, or whose delivery ends, fails the
// claims; an extra one's only ends that unit.
func TestClaims_EndedDelivery(t *testing.T) {
	t.Parallel()
	t.Run("extra", func(t *testing.T) {
		t.Parallel()
		f := newFakeShards(2, "X/u-09")
		p := startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{}, nil)
		require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
		f.mu.Lock()
		f.consumers["X/u-09"]["a"].failed <- errors.New("deleted")
		f.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		noFailure(t, p)
	})
	t.Run("configured", func(t *testing.T) {
		t.Parallel()
		f := newFakeShards(2)
		p := startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{}, nil)
		require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
		f.mu.Lock()
		f.consumers["S/u-01"]["a"].failed <- errors.New("deleted")
		f.mu.Unlock()
		select {
		case err := <-p.failed:
			assert.ErrorContains(t, err, "S/u-01")
		case <-time.After(5 * time.Second):
			t.Fatal("a configured unit's delivery ending did not fail the claims")
		}
	})
	t.Run("unbound", func(t *testing.T) {
		t.Parallel()
		f := newFakeShards(2)
		bad := "S/u-01"
		p := &fakeProc{f: f, name: "a"}
		p.failCreate.Store(&bad)
		q, err := ClaimShards(p, coord.NewLocal(), fastClaims(ClaimConfig{}))
		require.NoError(t, err)
		cons, err := q.CreateConsumer(t.Context(), mq.ConsumerConfig{})
		require.NoError(t, err)
		stop, failed, err := cons.Consume(func(*mq.Message) {}, 10)
		require.NoError(t, err)
		t.Cleanup(stop)
		select {
		case err := <-failed:
			assert.ErrorContains(t, err, bad)
		case <-time.After(5 * time.Second):
			t.Fatal("a unit that cannot be bound did not fail the claims")
		}
	})
}

// Rows delivered and unsettled across every unit stop at MaxHeld: a unit at
// the cap waits, and resumes as rows settle.
func TestClaims_HeldRowsCap(t *testing.T) {
	t.Parallel()
	f := newFakeShards(8)
	var received atomic.Int64
	var mu sync.Mutex
	var held []*mq.Message
	p := startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{MaxHeld: 5}, func(m *mq.Message) {
		received.Add(1)
		mu.Lock()
		held = append(held, m)
		mu.Unlock()
	})
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for i := range 20 {
		require.True(t, f.deliver(f.units[i%8], nil))
	}
	require.Eventually(t, func() bool { return received.Load() == 5 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(5), received.Load(), "never above the cap")
	assert.Equal(t, 5, p.loop.held.count())

	mu.Lock()
	for _, m := range held[:3] {
		require.NoError(t, m.Ack())
	}
	mu.Unlock()
	require.Eventually(t, func() bool { return received.Load() == 8 }, 5*time.Second, 10*time.Millisecond, "settling three admits three")
}

// A batcher that holds rows until a timer flushes them never deadlocks
// against the cap: each flush settles rows, which admits more.
func TestClaims_HeldRowsCapDrainsThroughFlushes(t *testing.T) {
	t.Parallel()
	f := newFakeShards(4)
	var mu sync.Mutex
	var batch []*mq.Message
	var acked atomic.Int64
	startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{MaxHeld: 10}, func(m *mq.Message) {
		mu.Lock()
		batch = append(batch, m)
		mu.Unlock()
	})
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	stop := make(chan struct{})
	defer close(stop)
	go func() { // the batch timer: flush whatever is held every 20ms
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				mu.Lock()
				flush := batch
				batch = nil
				mu.Unlock()
				for _, m := range flush {
					_ = m.Ack()
				}
			}
		}
	}()
	for i := range 200 {
		require.True(t, f.deliver(f.units[i%4], func() { acked.Add(1) }))
	}
	require.Eventually(t, func() bool { return acked.Load() == 200 }, 10*time.Second, 10*time.Millisecond, "acked %d", acked.Load())
}

// Only the lowest live member counts the units without an owner.
func TestClaims_UnownedCountedByTheLowestMember(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(4), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a", "b") }, 5*time.Second, 10*time.Millisecond)
	f.mu.Lock()
	f.unowned = nil
	f.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	assert.NotEmpty(t, f.unowned)
	for _, who := range f.unowned {
		assert.Equal(t, "a", who)
	}
	assert.True(t, a.loop.rankZero.Load())
}

// A unit taken over while the dead owner's pin still holds waits, unbound,
// until the pin lapses; then it is reset and bound.
func TestClaims_TakeoverWaitsForTheDeadOwnersPin(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(2), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	b := startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a", "b") }, 5*time.Second, 10*time.Millisecond)
	var aUnit string
	for u, owners := range f.owners() {
		if owners[0] == "a" {
			aUnit = u
		}
	}
	f.mu.Lock()
	f.pinned = map[string]bool{aUnit: true}
	f.mu.Unlock()
	crash(t, a)
	time.Sleep(300 * time.Millisecond)
	assert.NotContains(t, f.eventLog(), "b create "+aUnit, "not bound while the pin holds")
	f.mu.Lock()
	f.pinned = nil
	f.mu.Unlock()
	require.Eventually(t, func() bool { return slices.Contains(f.eventLog(), "b create "+aUnit) }, 5*time.Second, 10*time.Millisecond)
	assert.Contains(t, f.eventLog(), "b reset "+aUnit)
	noFailure(t, b)
}

// The gauges register on the meter provider in place when the claims start,
// which the process sets after this package's instruments are made.
func TestClaims_GaugesReport(t *testing.T) { //nolint:paralleltest // substitutes the package's claim meter
	reader := sdkmetric.NewManualReader()
	prev := claimMeter
	claimMeter = func() metric.Meter { return sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test") }
	t.Cleanup(func() { claimMeter = prev })

	f := newFakeShards(4)
	startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	got := map[string]int64{}
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &rm))
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) > 0 {
					got[m.Name] = g.DataPoints[0].Value
				}
			}
		}
		return got["wavehouse_ingest_shards_owned"] == 4
	}, 5*time.Second, 20*time.Millisecond, "%v", got)
	assert.Equal(t, int64(1), got["wavehouse_ingest_shard_members"])
	assert.Contains(t, got, "wavehouse_ingest_shards_unowned", "the lowest member reports it")
	assert.Contains(t, got, "wavehouse_ingest_rows_held")
}

// A row the worker lets go of without the broker confirming it (an ack that
// failed) frees its slot all the same: the broker redelivers it as a new row,
// which takes a slot of its own.
func TestClaims_HeldRowsFreedOnAFailedSettle(t *testing.T) {
	t.Parallel()
	f := newFakeShards(2)
	fail := errors.New("no answer")
	var received atomic.Int64
	p := startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{MaxHeld: 3}, func(m *mq.Message) {
		received.Add(1)
		_ = m.Ack()
	})
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for i := range 10 {
		require.True(t, f.deliverFailing(f.units[i%2], fail))
	}
	require.Eventually(t, func() bool { return received.Load() == 10 }, 5*time.Second, 10*time.Millisecond, "no slot leaks: got %d", received.Load())
	assert.Zero(t, p.loop.held.count())
}

// A row the worker never settles (it leaves some for the broker to
// redeliver) frees its slot once the broker would redeliver it anyway.
func TestClaims_HeldRowsExpire(t *testing.T) {
	t.Parallel()
	f := newFakeShards(1)
	var received atomic.Int64
	p := startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{MaxHeld: 2, SlotExpiry: 200 * time.Millisecond}, func(*mq.Message) {
		received.Add(1) // never settled
	})
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for range 4 {
		require.True(t, f.deliver(f.units[0], nil))
	}
	require.Eventually(t, func() bool { return received.Load() == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return received.Load() == 4 }, 5*time.Second, 10*time.Millisecond, "the expired slots admit the rest")
	require.Eventually(t, func() bool { return p.loop.held.count() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// A process whose membership lease ends under it takes a fresh one on its
// next tick and keeps consuming.
func TestClaims_RejoinsAfterLosingItsLease(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(4), coord.NewLocal()
	a := startClaims(t, f, c, "a", ClaimConfig{}, nil)
	var first coord.Term
	require.Eventually(t, func() bool {
		first = a.loop.memberTerm()
		return f.split("a") && first != nil
	}, 5*time.Second, 10*time.Millisecond)
	// The lease ends under a, as a backend reporting it lost would.
	require.NoError(t, first.Resign(t.Context()))
	require.Eventually(t, func() bool { m := a.loop.memberTerm(); return m != nil && m != first }, 5*time.Second, 10*time.Millisecond, "a joined again")
	assert.Greater(t, a.loop.memberTerm().Token(), first.Token())
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	noFailure(t, a)
}

// The only live process, on its first tick, resets the units nobody holds:
// a restart of the only ingest process gets its predecessor's unsettled
// rows back at once. With another member live, it does not.
func TestClaims_FirstTickResetsOnlyWhenAlone(t *testing.T) {
	t.Parallel()
	f := newFakeShards(3)
	startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for _, u := range f.units {
		assert.Contains(t, f.eventLog(), "a reset "+u)
	}

	g, c := newFakeShards(3), coord.NewLocal()
	startClaims(t, g, c, "b", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return g.split("b") }, 5*time.Second, 10*time.Millisecond)
	d := startClaims(t, g, c.Peer(), "d", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return g.split("b", "d") }, 5*time.Second, 10*time.Millisecond)
	assert.False(t, slices.ContainsFunc(g.eventLog(), func(e string) bool { return strings.HasPrefix(e, "d reset ") }),
		"a process joining a live member resets nothing: %v", g.eventLog())
	noFailure(t, d)
}

// stop cuts a handover short: it does not wait out Handover for rows the
// worker will never settle, and a delivery waiting at the cap returns
// without reaching the handler.
func TestClaims_StopCutsHandoversShort(t *testing.T) {
	t.Parallel()
	f, c := newFakeShards(4), coord.NewLocal()
	var received atomic.Int64
	a := startClaims(t, f, c, "a", ClaimConfig{Handover: time.Minute, MaxHeld: 4}, func(*mq.Message) { received.Add(1) })
	require.Eventually(t, func() bool { return f.split("a") }, 5*time.Second, 10*time.Millisecond)
	for i := range 6 { // four fill the cap, two wait at it
		require.True(t, f.deliver(f.units[i%4], nil))
	}
	require.Eventually(t, func() bool { return received.Load() == 4 }, 5*time.Second, 10*time.Millisecond)
	startClaims(t, f, c.Peer(), "b", ClaimConfig{}, nil) // a gives half its units up, unsettled
	require.Eventually(t, func() bool {
		return slices.ContainsFunc(f.eventLog(), func(e string) bool { return strings.HasPrefix(e, "a stop ") })
	},
		5*time.Second, 10*time.Millisecond)
	stopped := make(chan struct{})
	go func() { a.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("stop waited out the handover")
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(4), received.Load(), "the waiting deliveries never reached the handler")
}

// Membership slots are capped: a queue of many units still reads at most
// maxMembers leases a tick.
func TestClaims_MembershipSlotsCapped(t *testing.T) {
	t.Parallel()
	f := newFakeShards(3 * maxMembers)
	p := startClaims(t, f, coord.NewLocal(), "a", ClaimConfig{}, nil)
	require.Eventually(t, func() bool { return f.split("a") }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, maxMembers, p.loop.slots)
}
