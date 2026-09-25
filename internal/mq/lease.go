package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/Wave-RF/WaveHouse/internal/coord"
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
// candidate must see it unchanged, and the holding coordinator's session, so
// a coordinator recognizes its own writes and no one else's — two processes
// misconfigured with one instance_id still contend.
type leaseValue struct {
	Holder     string `json:"holder"`
	DurationMS int64  `json:"duration_ms"`
	Session    string `json:"session"`
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
	return &natsLeases{
		kv: kv, timings: t,
		value:     leaseValue{Holder: holder, DurationMS: t.duration.Milliseconds(), Session: nuid.Next()},
		held:      map[string]*natsTerm{},
		acquiring: map[string]struct{}{},
		seen:      map[string]leaseSighting{},
	}
}

// natsLeases is the coord.Coordinator over a KV bucket.
type natsLeases struct {
	kv      jetstream.KeyValue
	timings leaseTimings
	value   leaseValue

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

var _ coord.Coordinator = (*natsLeases)(nil)

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
	val, err := json.Marshal(l.value)
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
	go t.renew(sent)
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
// down no later than renewDeadline after it, before any takeover.
func (t *natsTerm) renew(last time.Time) {
	defer close(t.stopped)
	tm := t.owner.timings
	deadline := time.NewTimer(time.Until(last.Add(tm.renewDeadline)))
	defer deadline.Stop()
	tick := time.NewTicker(tm.renewEvery)
	defer tick.Stop()
	var lastErr error
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
		case errors.Is(err, coord.ErrLost):
			t.end(err)
			return
		default:
			// Retried next tick; the deadline timer ends the term on time.
			lastErr = err
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
