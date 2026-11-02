package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// Lease timings, client-go's leader-election defaults. A holder steps down
// when it has not renewed within the renew deadline, before any candidate can
// take the lease over (the lease duration).
const (
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
)

// leaseKeyPrefix leads every lease's key in the bucket.
const leaseKeyPrefix = "lease."

// LeaseOption adjusts a lease coordinator's timings (tests shorten them).
type LeaseOption func(*leaseTimings)

type leaseTimings struct {
	duration, renewDeadline, renewEvery time.Duration
}

// WithLeaseTimings sets how long a candidate must see a lease unchanged
// before taking it over, how long a holder keeps a lease it cannot renew, and
// how often it renews. Each must be shorter than the one before.
func WithLeaseTimings(duration, renewDeadline, renewEvery time.Duration) LeaseOption {
	return func(t *leaseTimings) { *t = leaseTimings{duration, renewDeadline, renewEvery} }
}

// leaseValue is what a lease's key holds: who holds it, for how long a
// candidate must see it unchanged, the holding coordinator's session, so a
// coordinator recognizes its own writes and no one else's (two processes
// misconfigured with one instance_id still contend), and the term's own id,
// so a term recognizes its own writes and not a later term's.
type leaseValue struct {
	Holder     string `json:"holder"`
	DurationMS int64  `json:"duration_ms"`
	Session    string `json:"session"`
	Term       string `json:"term"`
}

// Leases returns a coord.Coordinator over the operator's KV bucket on this
// broker's connection, holding leases as holder (the process's instance_id).
// A bucket that does not exist is ErrTopology: WaveHouse never creates it.
//
// The KV revision a term was acquired at is its fencing token. Expiry is
// judged on the candidate's own clock, never by comparing clocks: a lease is
// taken over only once a candidate has seen the same revision unchanged for
// the lease duration. The bucket keeps no per-key TTL, since a renewal cannot
// extend one.
func (e *ExternalNATS) Leases(ctx context.Context, bucket, holder string, opts ...LeaseOption) (coord.Coordinator, error) {
	kv, err := e.js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("%w: kv bucket %s does not exist; the operator creates it (wavehouse mq manifests)", ErrTopology, bucket)
	}
	if err != nil {
		return nil, fmt.Errorf("kv bucket %s: %w", bucket, err)
	}
	return newNATSLeases(kv, holder, opts...), nil
}

func newNATSLeases(kv jetstream.KeyValue, holder string, opts ...LeaseOption) *natsLeases {
	t := leaseTimings{defaultLeaseDuration, defaultRenewDeadline, coord.RetryPeriod}
	for _, opt := range opts {
		opt(&t)
	}
	l := &natsLeases{
		kv: kv, timings: t,
		value:     leaseValue{Holder: holder, DurationMS: t.duration.Milliseconds(), Session: nuid.Next()},
		held:      map[string]*natsTerm{},
		acquiring: map[string]struct{}{},
		seen:      map[string]leaseSighting{},
	}
	var err error
	if l.renewalFailures, l.gauges, err = registerLeaseMetrics(l); err != nil {
		slog.Warn("coord: lease metrics not registered", "error", err)
	}
	return l
}

// leaseMeter is the meter the lease metrics register on (tests substitute
// their own rather than the global provider, which binds only once).
var leaseMeter = func() metric.Meter { return otel.Meter("wavehouse-mq") }

// registerLeaseMetrics makes the renewal-failure counter and registers the
// held gauge on the meter provider in place now, since the coordinator is
// opened after the provider is set.
func registerLeaseMetrics(l *natsLeases) (metric.Int64Counter, metric.Registration, error) {
	meter := leaseMeter()
	failures, err := meter.Int64Counter("wavehouse_coord_lease_renewal_failures_total",
		metric.WithDescription("Lease renewals this process could not store (the write failed or got no answer); the term ends at its renew deadline if they keep failing"))
	if err != nil {
		return nil, nil, err
	}
	held, err := meter.Int64ObservableGauge("wavehouse_coord_lease_held",
		metric.WithDescription("1 for each lease this process holds in the NATS KV bucket: its shard claim membership slot"))
	if err != nil {
		return nil, nil, err
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		for name := range l.held {
			o.ObserveInt64(held, 1, metric.WithAttributes(leaseAttr(name)))
		}
		return nil
	}, held)
	if err != nil {
		return nil, nil, err
	}
	return failures, reg, nil
}

func leaseAttr(name string) attribute.KeyValue { return attribute.String("lease", name) }

// natsLeases is the coord.Coordinator over a KV bucket.
type natsLeases struct {
	kv      jetstream.KeyValue
	timings leaseTimings
	value   leaseValue

	renewalFailures metric.Int64Counter
	gauges          metric.Registration

	// mu guards the fields below and is never held across a request, so a
	// slow campaign cannot hold up another term ending.
	mu     sync.Mutex
	closed bool
	held   map[string]*natsTerm
	// acquiring holds the names a TryAcquire is campaigning for.
	acquiring map[string]struct{}
	// seen is, per lease another holder has, the revision last seen and when
	// it was first seen on this process's monotonic clock.
	seen map[string]leaseSighting
}

type leaseSighting struct {
	revision uint64
	since    time.Time
}

var (
	_ coord.Coordinator = (*natsLeases)(nil)
	_ coord.Observer    = (*natsLeases)(nil)
)

// TryAcquire implements coord.Coordinator.
func (l *natsLeases) TryAcquire(ctx context.Context, name string) (coord.Term, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, coord.ErrClosed
	}
	_, held := l.held[name]
	_, busy := l.acquiring[name]
	if held || busy {
		l.mu.Unlock()
		return nil, coord.ErrHeld
	}
	l.acquiring[name] = struct{}{}
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.acquiring, name)
		l.mu.Unlock()
	}()

	key := leaseKeyPrefix + name
	v := l.value
	v.Term = nuid.Next()
	val, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	// The renew deadline runs from before the write, never from its answer:
	// a candidate's clock can start as soon as the write is stored.
	sent := time.Now()
	rev, err := l.campaign(ctx, name, key, val)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	if l.closed {
		// Close ran while the write was in flight; hand the lease straight back.
		l.mu.Unlock()
		_ = l.kv.Delete(ctx, key, jetstream.LastRevision(rev))
		return nil, coord.ErrClosed
	}
	delete(l.seen, name)
	tctx, cancel := context.WithCancel(context.Background())
	t := &natsTerm{
		owner: l, name: name, key: key, val: val, token: rev, rev: rev,
		done: make(chan struct{}), stopped: make(chan struct{}), ctx: tctx, cancel: cancel,
	}
	l.held[name] = t
	l.mu.Unlock()
	observability.ZeroCounter(l.renewalFailures, 0, attribute.NewSet(leaseAttr(name)))
	go func() {
		defer observability.Crash("mq_lease", slog.String("lease", name))
		t.renew(sent)
		close(t.stopped)
	}()
	return t, nil
}

// campaign writes this coordinator's value to key if the lease is free,
// quiet past its duration, or already this coordinator's own write, and
// returns the revision written; coord.ErrHeld otherwise.
func (l *natsLeases) campaign(ctx context.Context, name, key string, val []byte) (uint64, error) {
	entry, err := l.kv.Get(ctx, key)
	var rev uint64
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		// Never held, or resigned: a delete marker is as good as absent.
		rev, err = l.kv.Create(ctx, key, val)
	case err != nil:
	default:
		var cur leaseValue
		mine := json.Unmarshal(entry.Value(), &cur) == nil && cur.Session == l.value.Session
		if !mine && !l.expired(name, entry.Revision(), cur) {
			return 0, coord.ErrHeld
		}
		// This coordinator's own write outlived a term it gave up (a renewal
		// past its deadline), or the holder went quiet: take it at the
		// revision seen, so a renewal in between wins instead.
		rev, err = l.kv.Update(ctx, key, val, entry.Revision())
	}
	if casConflict(err) {
		return 0, coord.ErrHeld
	}
	if err != nil {
		return 0, fmt.Errorf("coord lease %s: %w", name, err)
	}
	return rev, nil
}

// Held implements coord.Observer: a lease this coordinator holds is held; a
// key that is absent, resigned, or this coordinator's own leftover write is
// not; another holder's is held until this coordinator has seen its revision
// unchanged for the lease duration, the clock TryAcquire keeps.
func (l *natsLeases) Held(ctx context.Context, name string) (bool, error) {
	l.mu.Lock()
	_, mine := l.held[name]
	l.mu.Unlock()
	if mine {
		return true, nil
	}
	entry, err := l.kv.Get(ctx, leaseKeyPrefix+name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("coord lease %s: %w", name, err)
	}
	var cur leaseValue
	if json.Unmarshal(entry.Value(), &cur) == nil && cur.Session == l.value.Session {
		return false, nil
	}
	return !l.expired(name, entry.Revision(), cur), nil
}

// expired reports whether another holder's lease at revision has been seen
// unchanged for its duration (the holder's own, or this coordinator's when
// the value does not say), starting the clock on a revision not seen before.
func (l *natsLeases) expired(name string, revision uint64, cur leaseValue) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.seen[name]
	if !ok || s.revision != revision {
		l.seen[name] = leaseSighting{revision: revision, since: now}
		return false
	}
	d := l.timings.duration
	if cur.DurationMS > 0 {
		d = time.Duration(cur.DurationMS) * time.Millisecond
	}
	return now.Sub(s.since) >= d
}

// Close implements coord.Coordinator: every held term is resigned, deleting
// its key, so a candidate need not wait out the lease duration.
func (l *natsLeases) Close(ctx context.Context) error {
	l.mu.Lock()
	l.closed = true
	terms := make([]*natsTerm, 0, len(l.held))
	for _, t := range l.held {
		terms = append(terms, t)
	}
	l.mu.Unlock()
	var errs []error
	for _, t := range terms {
		errs = append(errs, t.Resign(ctx))
	}
	if l.gauges != nil {
		_ = l.gauges.Unregister()
	}
	return errors.Join(errs...)
}

// casConflict reports a write refused because the key is not at the
// revision it expected: someone else wrote it first. Create over a delete
// marker returns the server's error unmapped, and a replicated bucket
// reports the conflict under a code of its own.
func casConflict(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

// natsTerm is one holding of a lease, renewed until it ends.
type natsTerm struct {
	owner *natsLeases
	name  string
	key   string
	val   []byte
	token uint64

	// rev is the revision last written, owned by the renew loop while it
	// runs and read by Resign after it has stopped.
	rev uint64

	done    chan struct{}
	stopped chan struct{}
	// ctx bounds the renew loop's requests; Resign cancels it, so an
	// in-flight renewal never holds a resign up.
	ctx    context.Context
	cancel context.CancelFunc
	err    error // guarded by owner.mu, set before done closes
}

var _ coord.Term = (*natsTerm)(nil)

func (t *natsTerm) Name() string          { return t.name }
func (t *natsTerm) Token() uint64         { return t.token }
func (t *natsTerm) Done() <-chan struct{} { return t.done }

func (t *natsTerm) Err() error {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	return t.err
}

// renew rewrites the key at the revision last written, every renewEvery. A
// write at the wrong revision means another holder took the lease. The term
// also ends the moment the renew deadline passes without a stored renewal,
// timed from when that renewal was sent (last): a candidate may start its
// lease-duration clock as soon as the write is stored, so the holder steps
// down no later than renewDeadline after it, before any takeover. A renewal
// that fails is counted and logged, the first of a streak at WARN, so an
// operator sees the term heading for its deadline before it ends.
func (t *natsTerm) renew(last time.Time) {
	tm := t.owner.timings
	deadline := time.NewTimer(time.Until(last.Add(tm.renewDeadline)))
	defer deadline.Stop()
	tick := time.NewTicker(tm.renewEvery)
	defer tick.Stop()
	var lastErr error
	failures := 0
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-deadline.C:
			err := fmt.Errorf("%w: %s not renewed within %s", coord.ErrLost, t.name, tm.renewDeadline)
			if lastErr != nil {
				err = fmt.Errorf("%w: %w", err, lastErr)
			}
			t.end(err)
			return
		case <-tick.C:
		}
		sent := time.Now()
		ctx, cancel := context.WithDeadline(t.ctx, last.Add(tm.renewDeadline))
		rev, err := t.owner.kv.Update(ctx, t.key, t.val, t.rev)
		if casConflict(err) {
			rev, err = t.adoptLostReply(ctx)
		}
		cancel()
		switch {
		case err == nil:
			t.rev, last = rev, sent
			deadline.Reset(time.Until(last.Add(tm.renewDeadline)))
			if failures > 0 {
				slog.InfoContext(t.ctx, "coord: lease renewed again", "lease", t.name, "failures", failures)
				failures = 0
			}
		case errors.Is(err, coord.ErrLost):
			t.end(err)
			return
		case t.ctx.Err() != nil:
			return // resigned mid-renewal: not a failure
		default:
			// Retried next tick; the deadline timer ends the term on time.
			lastErr = err
			failures++
			if t.owner.renewalFailures != nil {
				t.owner.renewalFailures.Add(t.ctx, 1, metric.WithAttributes(leaseAttr(t.name)))
			}
			level := slog.LevelDebug
			if failures == 1 {
				level = slog.LevelWarn
			}
			slog.Log(t.ctx, level, "coord: lease renewal failed; trying again until the renew deadline",
				"lease", t.name, "failures", failures, "deadline_in", time.Until(last.Add(tm.renewDeadline)).Round(time.Millisecond), "error", err)
		}
	}
}

// adoptLostReply handles a renewal refused for its revision: when the key
// holds this term's own value at a later revision, an earlier renewal was
// stored and only its answer lost, so the term carries on from there.
// Anything else means another holder has the lease.
func (t *natsTerm) adoptLostReply(ctx context.Context) (uint64, error) {
	entry, err := t.owner.kv.Get(ctx, t.key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, err
	}
	if err == nil && entry.Revision() > t.rev && string(entry.Value()) == string(t.val) {
		return t.owner.kv.Update(ctx, t.key, t.val, entry.Revision())
	}
	return 0, fmt.Errorf("%w: %s taken by another holder", coord.ErrLost, t.name)
}

// end records why the term ended and closes Done, once; it frees the name
// for this coordinator to campaign again.
func (t *natsTerm) end(err error) bool {
	l := t.owner
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held[t.name] != t {
		return false
	}
	delete(l.held, t.name)
	t.err = err
	close(t.done)
	return true
}

// Resign implements coord.Term: it stops renewing, aborting a renewal in
// flight, and deletes the key at the revision last written, so a lease
// someone else has taken is left alone. If ctx ends first the term still
// ends here, and the lease runs out on its own.
func (t *natsTerm) Resign(ctx context.Context) error {
	t.cancel()
	select {
	case <-t.stopped:
	case <-ctx.Done():
		t.end(nil)
		return fmt.Errorf("resign coord lease %s: %w", t.name, ctx.Err())
	}
	if !t.end(nil) {
		return nil // already ended: lost, or resigned before
	}
	err := t.owner.kv.Delete(ctx, t.key, jetstream.LastRevision(t.rev))
	if casConflict(err) {
		// A renewal the cancel cut short may still have been stored: delete
		// this term's own value at its later revision, and nothing else.
		var entry jetstream.KeyValueEntry
		if entry, err = t.owner.kv.Get(ctx, t.key); err == nil && entry.Revision() > t.rev && string(entry.Value()) == string(t.val) {
			err = t.owner.kv.Delete(ctx, t.key, jetstream.LastRevision(entry.Revision()))
		} else if err == nil || errors.Is(err, jetstream.ErrKeyNotFound) {
			err = nil
		}
	}
	if err != nil && !casConflict(err) {
		// The term has ended here; the lease runs out on its own.
		return fmt.Errorf("resign coord lease %s: %w", t.name, err)
	}
	return nil
}
