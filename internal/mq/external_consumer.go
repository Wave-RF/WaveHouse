package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// pinIDHeader carries, on a row the server delivered to the pinned
	// puller, that puller's pin id.
	pinIDHeader = "Nats-Pin-Id"
	// releaseTimeout bounds one unit's release.
	releaseTimeout = 2 * time.Second
	// renewEvery is how often a unit that fetches nothing (at its cap, or
	// halted) sends the server a pull that keeps its pin. The server resets
	// the pin's timer on each pull from the pinned client, and the verifier
	// holds pinned_ttl to at least twice natsPullExpiry, so the pin cannot
	// lapse between two.
	renewEvery = natsPullExpiry
	// renewWait is how long a renewing pull waits: short, so that most of
	// the time no pull of a paused unit is waiting, and a durable deleted
	// meanwhile answers the next one with no responders.
	renewWait = time.Second
	// fetchExpiry is how long one fetch waits for rows. A halt waits for the
	// fetch in flight to end rather than cut it short, since a row the server
	// already sent to a pull its client dropped would be redelivered only
	// after ack_wait, behind newer rows: so this bounds how long a halt, and
	// with it a handover, takes. An idle unit costs one pull a second.
	fetchExpiry = time.Second
	// fetchRetryWait spaces the fetches of a unit whose last one failed.
	fetchRetryWait = 250 * time.Millisecond
	// noAnswersBeforeCheck is how many pulls in a row may get no responders
	// (the durable is gone, or its leader is moving) before the durable is
	// looked up.
	noAnswersBeforeCheck = 2
	// goneCheckTimeout bounds that lookup.
	goneCheckTimeout = 5 * time.Second
)

// CreateConsumer finds the operator's shard durables — it never creates one
// — and checks each against cfg: its ack_wait must cover cfg.AckWait and its
// max_ack_pending must be set, else ErrConsumerMismatch. cfg.Units picks the
// units (nil: every one, the extras still draining included). A durable name
// that does not map to the operator's, or a configured unit whose durable or
// stream is gone, is ErrConsumerNotFound; an extra whose durable is gone by
// now is skipped.
func (e *ExternalNATS) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	if _, ok := e.durable(cfg.Durable); !ok {
		return nil, fmt.Errorf("consumer %q: %w: the ingest durables are %s-<shard>", cfg.Durable, ErrConsumerNotFound, e.topo.IngestConsumer)
	}
	names := cfg.Units
	if names == nil {
		configured, extra := e.IngestUnits()
		names = slices.Concat(configured, extra)
	}
	c := &externalConsumer{e: e, ctx: ctx, whole: cfg.Units == nil, maxHeld: cfg.MaxHeld, failed: make(chan error, 1)}
	for _, name := range names {
		u, extra, ok := e.unit(name)
		if !ok {
			return nil, fmt.Errorf("consumer unit %q: %w", name, ErrUnitsUnsupported)
		}
		// A handle of its own: the client keeps the pin id per handle.
		h, err := e.js.Consumer(ctx, u.stream, u.durable)
		gone := errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound)
		switch {
		case extra && (gone || errors.Is(err, jetstream.ErrNotPullConsumer)):
			continue // drained and deleted since it was listed
		case gone:
			return nil, fmt.Errorf("consumer %s: %w: %w", name, ErrConsumerNotFound, err)
		case errors.Is(err, jetstream.ErrNotPullConsumer):
			return nil, fmt.Errorf("consumer %s: %w: %w", name, ErrConsumerMismatch, err)
		case err != nil:
			return nil, fmt.Errorf("consumer %s: %w", name, e.apiError(err))
		}
		have := h.CachedInfo().Config
		if have.AckWait < cfg.AckWait {
			return nil, fmt.Errorf("consumer %s: %w: ack_wait %s is shorter than the %s asked for", name, ErrConsumerMismatch, have.AckWait, cfg.AckWait)
		}
		if have.MaxAckPending <= 0 {
			return nil, fmt.Errorf("consumer %s: %w: max_ack_pending must be set", name, ErrConsumerMismatch)
		}
		if extra {
			slog.Info("mq: draining a shard durable outside the configured ones", "component", "nats", "unit", name, "pending", h.CachedInfo().NumPending)
		}
		c.parts = append(c.parts, &consumerPart{unit: u, extra: extra, h: h, ackWait: have.AckWait})
	}
	return c, nil
}

// externalConsumer is the operator's shard durables it was created for.
//
// Each unit has two goroutines. A puller fetches from the server and never
// runs the handler, so it keeps pulling, and with that keeps the unit's pin,
// however long the handler takes. A deliverer hands what was fetched to the
// handler, in order. The puller fetches only as much as the unit's cap
// (MaxHeld, and prefetch for what waits for the handler) leaves room for; at
// the cap, and once halted, it sends pulls that renew the pin and deliver
// nothing (max_bytes 1: the server holds any row back).
type externalConsumer struct {
	e   *ExternalNATS
	ctx context.Context
	// whole is a consumer of every unit, the one a process with no claims
	// runs: it takes orphaned units over itself, and releases them on stop.
	whole   bool
	maxHeld func() int
	parts   []*consumerPart
	failed  chan error
	// reported and stopped keep failed to one error, none after stop.
	reported, stopped atomic.Bool
}

type consumerPart struct {
	unit natsUnit
	// extra is outside the configured units: its delivery ending is the
	// operator deleting it once drained, not a failure.
	extra bool
	h     jetstream.Consumer
	// ackWait is the durable's: a row not settled by then is the server's to
	// redeliver, so it no longer counts against the cap.
	ackWait time.Duration
	// pin is the pin id of the last row this process received from the
	// unit, nil before the first or once the server said it lost the pin.
	pin atomic.Pointer[string]

	share   int
	maxHeld func() int

	mu sync.Mutex
	// queue is fetched and not yet handed to the handler; held is fetched
	// and not yet settled, queue included.
	queue []jetstream.Msg
	held  int

	// wake tells the puller a row settled; ready tells the deliverer rows
	// are queued.
	wake, ready chan struct{}
	// halting stops fetching; end stops renewing the pin. fetched closes
	// once nothing more will be queued, drained once the deliverer has
	// handed everything on.
	halting, end, fetched, drained chan struct{}
	haltOnce, endOnce              sync.Once
	// lastPull is when the puller last sent the server a pull (its
	// goroutine's only).
	lastPull time.Time
}

func (p *consumerPart) start(share int, maxHeld func() int) {
	p.share, p.maxHeld = share, maxHeld
	p.wake, p.ready = make(chan struct{}, 1), make(chan struct{}, 1)
	p.halting, p.end = make(chan struct{}), make(chan struct{})
	p.fetched, p.drained = make(chan struct{}), make(chan struct{})
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (p *consumerPart) halt()        { p.haltOnce.Do(func() { close(p.halting) }) }
func (p *consumerPart) endRenewals() { p.endOnce.Do(func() { close(p.end) }) }

func (p *consumerPart) halted() bool {
	select {
	case <-p.halting:
		return true
	default:
		return false
	}
}

// room is how many rows the unit may fetch now.
func (p *consumerPart) room() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.share - len(p.queue)
	if p.maxHeld != nil {
		n = min(n, p.maxHeld()-p.held)
	}
	return n
}

func (p *consumerPart) enqueue(m jetstream.Msg) {
	if pin := m.Headers().Get(pinIDHeader); pin != "" {
		p.pin.Store(&pin)
	}
	p.mu.Lock()
	p.queue = append(p.queue, m)
	p.held++
	p.mu.Unlock()
	signal(p.ready)
}

func (p *consumerPart) settle() {
	p.mu.Lock()
	p.held--
	p.mu.Unlock()
	signal(p.wake)
}

// next is the next row for the handler, false once fetching has ended and
// every row was handed on.
func (p *consumerPart) next() (jetstream.Msg, bool) {
	for {
		p.mu.Lock()
		if len(p.queue) > 0 {
			m := p.queue[0]
			p.queue[0] = nil
			p.queue = p.queue[1:]
			p.mu.Unlock()
			return m, true
		}
		p.mu.Unlock()
		select {
		case <-p.ready:
		case <-p.fetched:
			p.mu.Lock()
			empty := len(p.queue) == 0
			p.mu.Unlock()
			if empty {
				return nil, false
			}
		}
	}
}

// Consume pulls every unit in the priority group, so the server delivers a
// unit to one pinned puller at a time. Prefetch is split between the
// configured units (at least one each; 0 is the client's default per unit),
// and an extra gets a quarter share. A unit's delivery that ends on its own
// — the durable or its stream deleted, the connection closed for good — is
// reported on failed; an extra's is only logged. stop halts every unit
// without waiting (Halt waits); a consumer of every unit then releases each
// once it has handed on what it fetched, while one of chosen units keeps
// renewing their pins until Release, or for at most a durable's ack_wait.
func (c *externalConsumer) Consume(handler func(msg *Message), prefetch int) (func(), <-chan error, error) {
	own := 0
	for _, part := range c.parts {
		if !part.extra {
			own++
		}
	}
	if c.whole {
		c.takeOver()
	}
	for _, part := range c.parts {
		share := jetstream.DefaultMaxMessages
		if prefetch > 0 {
			share = max(1, prefetch/max(1, own))
		}
		if part.extra {
			share = max(1, share/4)
		}
		part.start(share, c.maxHeld)
		go c.run(part)
		go c.deliver(part, handler)
	}
	untrack := c.e.track(c.shutdown)
	return func() {
		untrack()
		c.stopped.Store(true)
		for _, part := range c.parts {
			part.halt()
			if c.whole {
				go func() {
					<-part.drained
					c.releaseAfterDrain(part)
				}()
			}
		}
	}, c.failed, nil
}

// Halt implements Halter: every unit stops fetching, and Halt returns once
// what was fetched has reached the handler, which may take one fetch's
// expiry. The units keep their pins.
func (c *externalConsumer) Halt() {
	c.stopped.Store(true)
	for _, part := range c.parts {
		part.halt()
	}
	for _, part := range c.parts {
		<-part.drained
	}
}

// shutdown is Close's stop: it halts and ends the renewals without waiting.
func (c *externalConsumer) shutdown() {
	c.stopped.Store(true)
	for _, part := range c.parts {
		part.halt()
		part.endRenewals()
	}
}

func (c *externalConsumer) run(part *consumerPart) {
	ended := c.pull(part)
	close(part.fetched)
	if !ended {
		c.hold(part)
	}
}

// pull fetches part until it is halted, reporting whether its delivery ended
// for good.
func (c *externalConsumer) pull(part *consumerPart) bool {
	noAnswers := 0
	for !part.halted() {
		n := part.room()
		if n <= 0 {
			if c.renewOrWait(part) {
				return true
			}
			continue
		}
		part.lastPull = time.Now()
		batch, err := part.h.Fetch(n, jetstream.FetchMaxWait(fetchExpiry), jetstream.FetchPriorityGroup(natsPriorityGroup))
		if err == nil {
			for m := range batch.Messages() {
				part.enqueue(m)
			}
			err = batch.Error()
		}
		switch {
		case err == nil || errors.Is(err, nats.ErrTimeout):
			noAnswers = 0
		case errors.Is(err, jetstream.ErrPinIDMismatch) || errors.Is(err, jetstream.ErrConsumerLeadershipChanged):
			// Another puller holds the pin, or the consumer's leader moved:
			// the next pull tries again, and nothing is lost.
			noAnswers = 0
			slog.Debug("mq: shard delivery paused", "component", "nats", "unit", part.unit.id(), "reason", err)
		case errors.Is(err, nats.ErrNoResponders):
			if noAnswers++; noAnswers >= noAnswersBeforeCheck && c.gone(part) {
				return true
			}
			c.pause(part, fetchRetryWait)
		default:
			if c.endedBy(part, err) {
				return true
			}
			level := slog.LevelWarn
			if part.extra {
				level = slog.LevelInfo
			}
			slog.Log(context.Background(), level, "mq: consumer reported an error", "component", "nats", "unit", part.unit.id(), "error", err)
			c.pause(part, fetchRetryWait)
		}
	}
	return false
}

// renewOrWait, for a unit at its cap, renews the pin when one is due and
// otherwise waits for a row to settle, a halt, or the renewal; it reports
// whether the unit's delivery ended for good.
func (c *externalConsumer) renewOrWait(part *consumerPart) bool {
	pin := part.pin.Load()
	if pin == nil {
		c.pause(part, 0) // never received: no pin to keep
		return false
	}
	if due := time.Until(part.lastPull.Add(renewEvery)); due > 0 {
		c.pause(part, due)
		return false
	}
	return c.renew(part, *pin)
}

// pause waits up to d (0: no bound) for a row to settle, a halt, or the
// end of the renewals.
func (c *externalConsumer) pause(part *consumerPart, d time.Duration) {
	var timeout <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case <-part.wake:
	case <-part.halting:
	case <-part.end:
	case <-timeout:
	}
}

// hold keeps a halted unit's pin, so that no other process receives from it
// while this one writes what it holds, until Release, Close, the pin is lost,
// or the durable's ack_wait has passed: by then the server redelivers what
// this process still holds anyway.
func (c *externalConsumer) hold(part *consumerPart) {
	until := time.Now().Add(part.ackWait)
	for {
		pin := part.pin.Load()
		if pin == nil || !time.Now().Before(until) {
			return
		}
		t := time.NewTimer(max(0, time.Until(part.lastPull.Add(renewEvery))))
		select {
		case <-part.end:
			t.Stop()
			return
		case <-t.C:
		}
		if c.renew(part, *pin) {
			return
		}
	}
}

// renewRequest is a pull the server answers with no row: max_bytes 1 is
// below any row's size, so it holds the row back (409) and keeps it where it
// is. It still resets the pin's timer, which the server does on any pull that
// carries the current pin id.
type renewRequest struct {
	Expires  time.Duration `json:"expires"`
	Batch    int           `json:"batch"`
	MaxBytes int           `json:"max_bytes"`
	Group    string        `json:"group"`
	ID       string        `json:"id"`
}

// renew sends part's pin-keeping pull, reporting whether the unit's delivery
// ended for good (its durable deleted).
func (c *externalConsumer) renew(part *consumerPart, pin string) bool {
	part.lastPull = time.Now()
	body, err := json.Marshal(renewRequest{Expires: renewWait, Batch: 1, MaxBytes: 1, Group: natsPriorityGroup, ID: pin})
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), renewWait+time.Second)
	defer cancel()
	subj := c.e.apiPrefix + "CONSUMER.MSG.NEXT." + part.unit.stream + "." + part.unit.durable
	resp, err := c.e.nc.RequestWithContext(ctx, subj, body)
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return c.gone(part)
	case err != nil:
		if c.endedBy(part, err) {
			return true
		}
		slog.Debug("mq: a shard's pin renewal got no answer", "component", "nats", "unit", part.unit.id(), "error", err)
		return false
	}
	status, descr := resp.Header.Get("Status"), strings.ToLower(resp.Header.Get("Description"))
	switch {
	case status == "409" && strings.Contains(descr, "consumer deleted"):
		c.end(part, jetstream.ErrConsumerDeleted)
		return true
	case status == "423":
		// The pin lapsed or went to another puller: this unit no longer
		// holds it, and the next fetch pins afresh if it can.
		if cur := part.pin.Load(); cur != nil && *cur == pin {
			part.pin.Store(nil)
		}
		slog.Debug("mq: a shard's pin was lost while it fetched nothing", "component", "nats", "unit", part.unit.id())
	}
	// 408 (nothing waiting), 409 over max_bytes (a row held back) or a
	// leadership change: the pin's timer was reset, or the next pull re-pins.
	return false
}

// gone looks a durable that answers no pull up: deleted, or its stream
// deleted, ends the unit's delivery. Anything else (its leader moving, the
// JetStream API not answering either) is tried again.
func (c *externalConsumer) gone(part *consumerPart) bool {
	ctx, cancel := context.WithTimeout(context.Background(), goneCheckTimeout)
	defer cancel()
	_, err := part.h.Info(ctx)
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		c.end(part, fmt.Errorf("%w: %w", ErrConsumerNotFound, err))
		return true
	}
	if err != nil && !c.stopped.Load() {
		slog.Warn("mq: a shard's pulls get no answer; trying again", "component", "nats", "unit", part.unit.id(), "error", err)
	}
	return false
}

// endedBy reports whether err ends part's delivery for good, and reports it
// if so: the durable deleted under a pull, or the connection closed for good.
func (c *externalConsumer) endedBy(part *consumerPart, err error) bool {
	switch {
	case errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrBadRequest):
		c.end(part, err)
		return true
	case errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionDraining) || c.e.nc.IsClosed():
		if !c.e.closing.Load() {
			c.end(part, nats.ErrConnectionClosed)
		}
		return true
	}
	return false
}

// end reports part's delivery ending on its own: a configured unit's on
// failed, an extra's only in the log.
func (c *externalConsumer) end(part *consumerPart, reason error) {
	err := fmt.Errorf("%w: %w", ErrDeliveryEnded, reason)
	if part.extra {
		slog.Info("mq: stopped draining a shard durable outside the configured ones", "component", "nats", "unit", part.unit.id(), "reason", err)
		return
	}
	c.fail(fmt.Errorf("%s: %w", part.unit.id(), err))
}

// deliver hands part's fetched rows to handler, in order, each counted
// against the unit's cap until it settles or its ack_wait passes.
func (c *externalConsumer) deliver(part *consumerPart, handler func(*Message)) {
	defer close(part.drained)
	for {
		m, ok := part.next()
		if !ok {
			return
		}
		msg := c.e.wrapMsg(c.ctx, m, true)
		letGo := sync.OnceFunc(part.settle)
		expire := time.AfterFunc(part.ackWait, letGo)
		msg.OnSettled(func() { expire.Stop(); letGo() })
		handler(msg)
	}
}

// takeOver resets every unit no one holds that has rows awaiting an ack,
// before this consumer pulls it: whoever received them is gone.
func (c *externalConsumer) takeOver() {
	ctx, cancel := context.WithTimeout(c.ctx, recheckTimeout)
	defer cancel()
	for _, part := range c.parts {
		if reset, err := c.e.ResetOrphaned(ctx, part.unit.id()); errors.Is(err, ErrUnitHeld) {
			continue // another consumer holds it; the pin decides
		} else if err != nil {
			slog.Warn("mq: could not take over a shard's unsettled rows; they come back after ack_wait", "component", "nats", "unit", part.unit.id(), "error", err)
		} else if reset {
			slog.Info("mq: took over a shard's unsettled rows", "component", "nats", "unit", part.unit.id())
		}
	}
}

// releaseAfterDrain releases part once its halt has delivered what it
// fetched. A whole consumer's handler has no settlement to wait for here,
// and an unsettled row comes back to the next owner after ack_wait.
func (c *externalConsumer) releaseAfterDrain(part *consumerPart) {
	defer part.endRenewals()
	if c.e.nc.IsClosed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	if err := c.e.release(ctx, part); err != nil {
		slog.Debug("mq: could not release a shard; its pin lapses on its own", "component", "nats", "unit", part.unit.id(), "error", err)
	}
}

// Release implements Releaser: each unit this consumer was last pinned on,
// and whose pin the server still gives it, is unpinned; then the unit stops
// renewing its pin.
func (c *externalConsumer) Release(ctx context.Context) error {
	var errs []error
	for _, part := range c.parts {
		errs = append(errs, c.e.release(ctx, part))
		part.endRenewals()
	}
	return errors.Join(errs...)
}

// release unpins part if this process still holds its pin. Checking first
// keeps a process that never received from the unit, or lost the pin since,
// from unpinning the real owner.
//
// The check and the unpin are two requests, and the unpin names no pin id:
// if the pin moved to another client between them, that client is unpinned
// and pins again with its next pull. A halted unit renews its pin until its
// release returns, so the pin cannot lapse in between; only a consumer leader
// change there (which releases the pin itself) opens that window.
func (e *ExternalNATS) release(ctx context.Context, part *consumerPart) error {
	pin := part.pin.Load()
	if pin == nil {
		return nil
	}
	info, err := part.h.Info(ctx)
	if err != nil {
		return fmt.Errorf("consumer %s: %w", part.unit.id(), e.apiError(err))
	}
	if pinnedClient(info) != *pin {
		return nil
	}
	s, err := e.js.Stream(ctx, part.unit.stream)
	if err != nil {
		return fmt.Errorf("stream %s: %w", part.unit.stream, e.apiError(err))
	}
	if err := s.UnpinConsumer(ctx, part.unit.durable, natsPriorityGroup); err != nil {
		return fmt.Errorf("unpin %s: %w", part.unit.id(), e.apiError(err))
	}
	return nil
}

func (c *externalConsumer) fail(err error) {
	if c.stopped.Load() || !c.reported.CompareAndSwap(false, true) {
		return
	}
	c.failed <- err
}
