package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// Shard claims: over a sharded queue (mq.Sharded), each unit — one shard of
// one partition — is consumed by one ingest process, so every table, which
// lives in one unit, has one process batching its inserts. The broker makes
// that exclusive (one pinned puller receives at a time); the claims make it
// balanced.
//
// Membership. Each process holds one lease, "ingest.m<j>" for the lowest free
// j below the number of configured units, and every tick reads which slots
// are live (coord.Observer). A process without a slot idles.
//
// Assignment. Every process computes the same owner for every unit from the
// live slots (assignUnits: rendezvous hashing capped at an even share), and
// consumes the units assigned to its own slot. Two processes whose views of
// the members differ may both pull a unit, and the broker delivers it to one
// of them. Views can differ for up to a lease: each process starts a
// silent member's expiry clock when it first sees it.
//
// Handover. A unit no longer assigned here stops fetching, waits until the
// rows it delivered are settled (bounded by Handover), then releases the
// broker's pin, so the next owner receives at once. A clean stop releases
// every unit after the worker has flushed.
//
// Takeover. A unit whose owner in the previous tick's view is no longer a
// member is reset to its ack floor before it is consumed (ResetOrphaned), so
// the dead owner's unacked rows come back at once instead of after ack_wait.
// While the dead owner's pin has not lapsed yet the unit waits, unbound, and
// is tried again next tick, up to orphanWait. On a process's first tick,
// which has no previous view, only when it is the one live member: then
// whatever holds rows unpinned is gone (a crash or a restart of the only
// process). Never after pulling: a reset also redelivers the caller's own
// unacked rows.
//
// Memory. Rows delivered and not yet settled, across every unit this process
// holds, are capped at MaxHeld: at the cap a unit's delivery waits, and the
// broker keeps the rest.
const (
	memberLeasePrefix = "ingest.m"

	defaultHandover     = 15 * time.Second
	defaultUnownedEvery = 15 * time.Second
	// settleQuiet is how long a unit given up must deliver nothing before its
	// handover counts it settled: the rows it fetched ahead reach the worker
	// in that time.
	settleQuiet = 250 * time.Millisecond
	// tickTimeout bounds one tick's requests.
	tickTimeout = 10 * time.Second
	// releaseWait bounds a lease resign or a unit's release.
	releaseWait = 5 * time.Second
	// orphanWait bounds how long a unit taken over waits for its dead
	// owner's pin to lapse before it is bound anyway: past it, whoever holds
	// the pin is alive, and the broker's pin decides.
	orphanWait = 30 * time.Second
)

func memberLease(j int) string { return memberLeasePrefix + strconv.Itoa(j) }

// ClaimConfig tunes ClaimShards.
type ClaimConfig struct {
	// Every is how often membership is read and units claimed or given up;
	// default coord.RetryPeriod.
	Every time.Duration
	// Handover bounds how long a unit given up waits for its delivered rows
	// to settle before it is released; default 15s.
	Handover time.Duration
	// MaxHeld caps the rows delivered and not yet settled across every unit
	// this process holds; default the worker's maxAckPending.
	MaxHeld int
	// UnownedEvery is how often the lowest live member reads how many units
	// have rows and no owner; default 15s.
	UnownedEvery time.Duration
	// SlotExpiry frees a delivered row's MaxHeld slot if the worker never
	// settles it: by then the broker redelivers the row anyway. Default the
	// worker's ack_wait.
	SlotExpiry time.Duration
}

// shardedQueue is a queue ClaimShards can share out.
type shardedQueue interface {
	Queue
	mq.Sharded
}

// ClaimShards wraps q so that its consumer takes only the units this process
// is assigned through c, which every ingest process must share and which
// must implement coord.Observer. q must implement mq.Sharded.
func ClaimShards(q Queue, c coord.Coordinator, cfg ClaimConfig) (Queue, error) {
	sq, ok := q.(shardedQueue)
	if !ok {
		return nil, fmt.Errorf("shard claims need a sharded queue, got %T", q)
	}
	obs, ok := c.(coord.Observer)
	if !ok {
		return nil, fmt.Errorf("shard claims need a coordinator that observes leases, got %T", c)
	}
	if cfg.Every <= 0 {
		cfg.Every = coord.RetryPeriod
	}
	if cfg.Handover <= 0 {
		cfg.Handover = defaultHandover
	}
	if cfg.MaxHeld <= 0 {
		cfg.MaxHeld = maxAckPending
	}
	if cfg.UnownedEvery <= 0 {
		cfg.UnownedEvery = defaultUnownedEvery
	}
	if cfg.SlotExpiry <= 0 {
		cfg.SlotExpiry = ackWait
	}
	return &claimingQueue{Queue: q, sq: sq, c: c, obs: obs, cfg: cfg}, nil
}

type claimingQueue struct {
	Queue
	sq  shardedQueue
	c   coord.Coordinator
	obs coord.Observer
	cfg ClaimConfig
}

// CreateConsumer checks every configured unit's durable now, as a consumer
// of all of them would, so a missing one still refuses boot; units are bound
// one by one as they are claimed.
func (q *claimingQueue) CreateConsumer(ctx context.Context, cfg mq.ConsumerConfig) (mq.Consumer, error) {
	if cfg.Units != nil {
		return nil, errors.New("a claiming consumer chooses its own units")
	}
	configured, _ := q.sq.IngestUnits()
	if len(configured) == 0 {
		return nil, errors.New("shard claims need at least one unit")
	}
	check := cfg
	check.Units = configured
	if _, err := q.sq.CreateConsumer(ctx, check); err != nil {
		return nil, err
	}
	return &claimingConsumer{q: q, ctx: ctx, cfg: cfg}, nil
}

type claimingConsumer struct {
	q   *claimingQueue
	ctx context.Context
	cfg mq.ConsumerConfig
	// loop is the running claims, once Consume has started them.
	loop atomic.Pointer[claimLoop]
}

// Consume starts claiming. stop ends it, releasing every unit held and
// resigning the membership lease; failed reports, exactly once, a configured
// unit's delivery ending on its own or a unit that could not be bound.
func (c *claimingConsumer) Consume(handler func(*mq.Message), prefetch int) (func(), <-chan error, error) {
	configured, _ := c.q.sq.IngestUnits()
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(c.ctx))
	l := &claimLoop{
		q: c.q, ctx: loopCtx, cancel: cancel, cfg: c.cfg, handler: handler, prefetch: prefetch,
		slots: len(configured), configured: map[string]bool{},
		owned: map[string]*claim{}, releasing: map[string]struct{}{}, orphaned: map[string]time.Time{},
		held:   newHeldRows(c.q.cfg.MaxHeld),
		failed: make(chan error, 1), quit: make(chan struct{}), done: make(chan struct{}),
	}
	for _, u := range configured {
		l.configured[u] = true
	}
	var err error
	if l.gauges, err = registerClaimGauges(l); err != nil {
		slog.Warn("ingest: shard claim gauges not registered", "error", err)
	}
	c.loop.Store(l)
	go l.run()
	return l.stop, l.failed, nil
}

// claimLoop is one process's claims. member, owned, prev and lastUnowned
// belong to the run goroutine (and to stop once it has exited); releasing
// is shared with the handovers.
type claimLoop struct {
	q          *claimingQueue
	ctx        context.Context
	cancel     context.CancelFunc
	cfg        mq.ConsumerConfig
	handler    func(*mq.Message)
	prefetch   int
	slots      int
	configured map[string]bool

	member     coord.Term
	memberSnap atomic.Pointer[coord.Term] // member, for readers off the run goroutine
	slot       int
	owned      map[string]*claim
	prev       map[string]int // the previous tick's owners, nil before the first
	// orphaned holds the units taken over from a dead owner that still wait
	// for a reset, and since when.
	orphaned    map[string]time.Time
	lastUnowned time.Time

	mu        sync.Mutex
	releasing map[string]struct{}
	handovers sync.WaitGroup

	held *heldRows

	ownedGauge, membersGauge, unownedGauge atomic.Int64
	rankZero                               atomic.Bool
	gauges                                 metric.Registration

	failed            chan error
	reported, stopped atomic.Bool
	quit, done        chan struct{}
	stopOnce          sync.Once
}

// claim is one unit this process holds.
type claim struct {
	unit     string
	cons     mq.Consumer
	stopOnce sync.Once
	stopFn   func()
	// inflight counts rows delivered and not yet settled; last is when the
	// latest was delivered (unix nanoseconds).
	inflight atomic.Int64
	last     atomic.Int64
	quit     chan struct{}
}

func (cl *claim) stop() {
	cl.stopOnce.Do(func() {
		close(cl.quit)
		if cl.stopFn != nil {
			cl.stopFn()
		}
	})
}

func (l *claimLoop) run() {
	defer close(l.done)
	t := time.NewTicker(l.q.cfg.Every)
	defer t.Stop()
	for {
		l.tick()
		select {
		case <-l.quit:
			return
		case <-t.C:
		}
	}
}

// tick reads membership, then gives up or takes units to match this
// process's assignment. A failed read skips the rest of the tick: acting on
// a partial view of the members would move units for nothing.
func (l *claimLoop) tick() {
	ctx, cancel := context.WithTimeout(l.ctx, tickTimeout)
	defer cancel()
	defer l.ownedGauge.Store(int64(len(l.owned)))
	defer func() { m := l.member; l.memberSnap.Store(&m) }()
	l.reap()
	if l.member == nil {
		l.joinMembers(ctx)
	}
	var live []int
	for j := range l.slots {
		held, err := l.q.obs.Held(ctx, memberLease(j))
		if err != nil {
			l.warn("ingest: could not read the shard claim membership", err)
			return
		}
		if held {
			live = append(live, j)
		}
	}
	l.membersGauge.Store(int64(len(live)))
	configured, extra := l.q.sq.IngestUnits()
	units := slices.Concat(configured, extra)
	assignment := assignUnits(units, live)
	mine := l.member != nil && slices.Contains(live, l.slot)
	targets := map[string]bool{}
	if mine {
		for u, s := range assignment {
			if s == l.slot {
				targets[u] = true
			}
		}
	}
	l.giveUp(targets)
	l.take(ctx, targets, len(targets), live)
	l.prev = assignment
	l.rankZero.Store(mine && len(live) > 0 && live[0] == l.slot)
	l.readUnowned(ctx)
}

// memberTerm is the membership term as of the last tick, nil for none.
func (l *claimLoop) memberTerm() coord.Term {
	if t := l.memberSnap.Load(); t != nil {
		return *t
	}
	return nil
}

// reap drops the membership term if it ended under this process; the next
// tick then gives up every unit and joins again.
func (l *claimLoop) reap() {
	if l.member == nil {
		return
	}
	select {
	case <-l.member.Done():
		slog.Warn("ingest: lost the shard claim membership lease; giving up every shard", "slot", l.slot, "error", l.member.Err())
		claimEvents.Add(context.Background(), 1, eventAttr("lost"))
		l.member = nil
	default:
	}
}

// joinMembers takes the lowest free membership slot. With every slot taken,
// or on an error, the process stays a member of none this tick.
func (l *claimLoop) joinMembers(ctx context.Context) {
	for j := range l.slots {
		term, err := l.q.c.TryAcquire(ctx, memberLease(j))
		switch {
		case err == nil:
			l.member, l.slot = term, j
			slog.Info("ingest: joined the shard claim membership", "slot", j)
			return
		case errors.Is(err, coord.ErrHeld):
		case errors.Is(err, coord.ErrClosed):
			return
		default:
			l.warn("ingest: could not join the shard claim membership", err)
			return
		}
	}
}

// giveUp hands on every unit held that is not a target, and forgets the
// takeovers of units that are no longer targets.
func (l *claimLoop) giveUp(targets map[string]bool) {
	for u := range l.orphaned {
		if !targets[u] {
			delete(l.orphaned, u)
		}
	}
	for _, u := range slices.Sorted(maps.Keys(l.owned)) {
		if targets[u] {
			continue
		}
		cl := l.owned[u]
		delete(l.owned, u)
		l.mu.Lock()
		l.releasing[u] = struct{}{}
		l.mu.Unlock()
		l.handovers.Go(func() { l.handOver(cl) })
	}
}

// take binds every target not held yet. A unit whose previous owner is no
// longer a member is reset first, so its unsettled rows come back at once.
func (l *claimLoop) take(ctx context.Context, targets map[string]bool, count int, live []int) {
	for _, u := range slices.Sorted(maps.Keys(targets)) {
		if _, ok := l.owned[u]; ok || l.isReleasing(u) {
			continue
		}
		owner, known := l.prev[u]
		dead := known && owner != l.slot && !slices.Contains(live, owner)
		alone := l.prev == nil && len(live) == 1 && live[0] == l.slot
		if dead || alone {
			if _, waiting := l.orphaned[u]; !waiting {
				l.orphaned[u] = time.Now()
			}
		}
		if since, ok := l.orphaned[u]; ok {
			switch reset, err := l.q.sq.ResetOrphaned(ctx, u); {
			case errors.Is(err, mq.ErrUnitHeld) && time.Since(since) < orphanWait:
				continue // the dead owner's pin has not lapsed yet
			case errors.Is(err, mq.ErrUnitHeld):
			case err != nil:
				l.warn("ingest: could not take over a shard's unsettled rows; they come back after ack_wait", err)
			case reset:
				claimEvents.Add(context.Background(), 1, eventAttr("reset"))
				slog.Info("ingest: took over a shard's unsettled rows", "unit", u, "waited", time.Since(since).Round(time.Millisecond))
			}
			delete(l.orphaned, u)
		}
		if err := l.open(u, count); err != nil {
			if !l.configured[u] {
				slog.Info("ingest: could not bind a shard durable outside the configured ones", "unit", u, "error", err)
				continue
			}
			l.fail(fmt.Errorf("shard %s: %w", u, err))
			return
		}
	}
}

func (l *claimLoop) isReleasing(u string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.releasing[u]
	return ok
}

// open binds unit u and starts delivering it, counting each row until it
// settles and holding it against the process-wide cap.
func (l *claimLoop) open(u string, count int) error {
	cfg := l.cfg
	cfg.Units = []string{u}
	cons, err := l.q.sq.CreateConsumer(l.ctx, cfg)
	if err != nil {
		return err
	}
	cl := &claim{unit: u, cons: cons, quit: make(chan struct{})}
	share := l.prefetch
	if share > 0 {
		share = max(1, share/max(1, count))
	}
	stop, failed, err := cons.Consume(func(m *mq.Message) {
		if !l.held.acquire(cl.quit, l.quit) {
			return // stopping: the row is unsettled and comes back
		}
		cl.inflight.Add(1)
		cl.last.Store(time.Now().UnixNano())
		// The row leaves this process's hands at its first settle attempt,
		// or, if the worker never makes one (it leaves some rows for the
		// broker to redeliver), once the broker would redeliver it anyway.
		letGo := sync.OnceFunc(func() {
			cl.inflight.Add(-1)
			l.held.release()
		})
		expire := time.AfterFunc(l.q.cfg.SlotExpiry, letGo)
		m.OnSettled(func() { expire.Stop(); letGo() })
		l.handler(m)
	}, share)
	if err != nil {
		return err
	}
	cl.stopFn = stop
	l.owned[u] = cl
	l.watch(cl, failed)
	claimEvents.Add(context.Background(), 1, eventAttr("taken"))
	slog.Info("ingest: took a shard", "unit", u, "slot", l.slot)
	return nil
}

// watch reports a configured unit's delivery ending on its own; an extra's
// ending is the operator deleting it once drained.
func (l *claimLoop) watch(cl *claim, failed <-chan error) {
	go func() {
		select {
		case err := <-failed:
			if !l.configured[cl.unit] {
				slog.Info("ingest: stopped draining a shard outside the configured ones", "unit", cl.unit, "reason", err)
				return
			}
			l.fail(fmt.Errorf("shard %s: %w", cl.unit, err))
		case <-cl.quit:
		case <-l.quit:
		}
	}()
}

// handOver stops fetching cl's unit, waits for what it delivered to settle,
// bounded by Handover (or cut short by stop), and releases it.
func (l *claimLoop) handOver(cl *claim) {
	began := time.Now()
	cl.stop()
	deadline := time.After(l.q.cfg.Handover)
	poll := time.NewTicker(settleQuiet / 5)
	defer poll.Stop()
wait:
	for cl.inflight.Load() > 0 || time.Since(time.Unix(0, cl.last.Load())) < settleQuiet {
		select {
		case <-poll.C:
		case <-deadline:
			claimEvents.Add(context.Background(), 1, eventAttr("handover_timeout"))
			slog.Warn("ingest: handing a shard on before its rows settled", "unit", cl.unit, "unsettled", cl.inflight.Load())
			break wait
		case <-l.quit:
			break wait
		}
	}
	l.release(cl)
	l.mu.Lock()
	delete(l.releasing, cl.unit)
	l.mu.Unlock()
	handoverSeconds.Record(context.Background(), time.Since(began).Seconds())
	claimEvents.Add(context.Background(), 1, eventAttr("released"))
	slog.Info("ingest: handed a shard on", "unit", cl.unit, "took", time.Since(began).Round(time.Millisecond))
}

// release gives cl's unit up at the broker, best effort: a pin not released
// lapses on its own.
func (l *claimLoop) release(cl *claim) {
	r, ok := cl.cons.(mq.Releaser)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseWait)
	defer cancel()
	if err := r.Release(ctx); err != nil {
		slog.Warn("ingest: could not release a shard; its pin lapses on its own", "unit", cl.unit, "error", err)
	}
}

// readUnowned has the lowest live member count the units with rows and no
// owner, every UnownedEvery; the others report nothing.
func (l *claimLoop) readUnowned(ctx context.Context) {
	if !l.rankZero.Load() || time.Since(l.lastUnowned) < l.q.cfg.UnownedEvery {
		return
	}
	n, err := l.q.sq.Unowned(ctx)
	if err != nil {
		l.warn("ingest: could not count the shards without an owner", err)
		return
	}
	l.lastUnowned = time.Now()
	l.unownedGauge.Store(int64(n))
}

func (l *claimLoop) warn(msg string, err error) {
	if l.ctx.Err() == nil {
		slog.Warn(msg, "error", err)
	}
}

func (l *claimLoop) fail(err error) {
	if l.stopped.Load() || !l.reported.CompareAndSwap(false, true) {
		return
	}
	l.failed <- err
}

// stop ends the claims: every unit stops, is released, and the membership
// lease is resigned, at once — the worker has flushed what it held before
// calling it.
func (l *claimLoop) stop() {
	l.stopOnce.Do(func() {
		l.stopped.Store(true)
		l.cancel()
		close(l.quit)
		<-l.done
		for _, cl := range l.owned {
			cl.stop()
		}
		l.handovers.Wait()
		// At once, so a broker that no longer answers costs one release's
		// wait, not one per unit.
		var releases sync.WaitGroup
		for _, cl := range l.owned {
			releases.Go(func() { l.release(cl) })
			claimEvents.Add(context.Background(), 1, eventAttr("released"))
		}
		releases.Wait()
		clear(l.owned)
		l.ownedGauge.Store(0)
		if l.member != nil {
			ctx, cancel := context.WithTimeout(context.Background(), releaseWait)
			if err := l.member.Resign(ctx); err != nil {
				slog.Warn("ingest: resign failed; the lease runs out on its own", "lease", l.member.Name(), "error", err)
			}
			cancel()
		}
		if l.gauges != nil {
			_ = l.gauges.Unregister()
		}
	})
}

// heldRows caps the rows this process holds delivered and unsettled.
type heldRows struct {
	slots chan struct{}
}

func newHeldRows(n int) *heldRows { return &heldRows{slots: make(chan struct{}, n)} }

// acquire takes a slot, waiting while every one is taken, false if either
// quit channel closes first.
func (h *heldRows) acquire(a, b <-chan struct{}) bool {
	select {
	case h.slots <- struct{}{}:
		return true
	default:
	}
	heldWaits.Add(context.Background(), 1)
	select {
	case h.slots <- struct{}{}:
		return true
	case <-a:
		return false
	case <-b:
		return false
	}
}

// release frees the slot a row took; each row frees exactly one.
func (h *heldRows) release() { <-h.slots }

func (h *heldRows) count() int { return len(h.slots) }

var (
	claimEvents, _ = otel.Meter("wavehouse-ingest").Int64Counter(
		"wavehouse_ingest_shard_events_total",
		metric.WithDescription("Shard claim events in this process: taken, released (handed on or stopped), reset (a dead owner's unsettled rows redelivered at takeover), handover_timeout (released before its rows settled), lost (the membership lease ended under it)"),
	)
	handoverSeconds, _ = otel.Meter("wavehouse-ingest").Float64Histogram(
		"wavehouse_ingest_shard_handover_seconds",
		metric.WithDescription("Time from giving a shard up to releasing it: the drain of what it had delivered"),
		metric.WithUnit("s"),
	)
	heldWaits, _ = otel.Meter("wavehouse-ingest").Int64Counter(
		"wavehouse_ingest_rows_held_waits_total",
		metric.WithDescription("Deliveries that waited because this process already held its cap of unsettled rows"),
	)
)

// claimMeter is the meter the claim gauges register on (tests substitute
// their own rather than the global provider, which binds only once).
var claimMeter = func() metric.Meter { return otel.Meter("wavehouse-ingest") }

func eventAttr(event string) metric.AddOption {
	return metric.WithAttributes(attribute.String("event", event))
}

// registerClaimGauges registers the gauges on the meter provider in place
// now: an observable made before the provider was set cannot be registered
// with one made after.
func registerClaimGauges(l *claimLoop) (metric.Registration, error) {
	meter := claimMeter()
	owned, err := meter.Int64ObservableGauge("wavehouse_ingest_shards_owned",
		metric.WithDescription("Shards this process holds and consumes"))
	if err != nil {
		return nil, err
	}
	members, err := meter.Int64ObservableGauge("wavehouse_ingest_shard_members",
		metric.WithDescription("Ingest processes this process last counted sharing the shards"))
	if err != nil {
		return nil, err
	}
	unowned, err := meter.Int64ObservableGauge("wavehouse_ingest_shards_unowned",
		metric.WithDescription("Shards with rows waiting that no process holds, as the lowest-ranked member last counted them; the other members report nothing"))
	if err != nil {
		return nil, err
	}
	held, err := meter.Int64ObservableGauge("wavehouse_ingest_rows_held",
		metric.WithDescription("Rows this process holds delivered and not yet acked or nacked, across its shards"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(owned, l.ownedGauge.Load())
		o.ObserveInt64(members, l.membersGauge.Load())
		o.ObserveInt64(held, int64(l.held.count()))
		if l.rankZero.Load() {
			o.ObserveInt64(unowned, l.unownedGauge.Load())
		}
		return nil
	}, owned, members, unowned, held)
}
