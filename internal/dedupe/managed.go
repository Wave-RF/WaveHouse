package dedupe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ErrDisabled is returned by Managed's calls while dedupe is switched off.
// The ingest handler consults the settings snapshot before calling, so it
// only sees this in the window of a reload that flips dedupe.enabled: the
// snapshot and the store transition at different instants, and a record
// caught between them is published un-deduped rather than failed.
var ErrDisabled = errors.New("dedupe is disabled")

// ErrUnavailable is returned by Managed's calls when dedupe is switched on
// but the store failed to open, and wrapped by a backend's error when a
// retry later can succeed. Ingest fails closed on it — the settings asked
// for dedupe, so publishing un-deduped is not a fallback.
var ErrUnavailable = errors.New("dedupe store unavailable")

// hashedIDCounter counts ids stored as their SHA-256 (Key.Hashed): an id
// longer than MaxIDBytes is a producer sending something other than an id.
var hashedIDCounter, _ = otel.Meter("wavehouse-dedupe").Int64Counter(
	"wavehouse_dedupe_hashed_id_total",
	metric.WithDescription("Dedupe ids stored as their SHA-256 because they exceed the verbatim length limit"),
)

// Managed is a Deduplicator whose backing store follows the hot-reloadable
// dedupe.enabled setting: Apply(true) opens it through the function
// NewManaged was given, Apply(false) closes it, and in-flight Reserve,
// Commit and Release calls are serialized against that swap so a reload can
// never close the store under a lookup. Which store that is — a tenant's share of the
// embedded Pebble instance (Embedded.Tenant), a remote backend's view later —
// is the opener's business, so every backend gets the same switch semantics.
type Managed struct {
	open func() (Deduplicator, error)
	mu   sync.RWMutex
	// enabled is the last state passed to Apply; db is nil while disabled
	// and also when an enabling open failed.
	enabled bool
	db      Deduplicator
}

// NewManaged returns a closed Managed store over open. Nothing is opened
// until Apply(true).
func NewManaged(open func() (Deduplicator, error)) *Managed {
	return &Managed{open: open}
}

// Apply reconciles the store with the desired state, idempotently: an
// already-open store stays open, an already-closed one stays closed. A
// failed open leaves the store closed and returns the error — the caller
// decides whether that is fatal (boot) or a logged degradation (reload).
//
// A no-op call — the desired state already holds — returns under the read
// lock alone; only a real transition takes the write lock, re-checked once
// held in case another Apply won the race. This matters because a settings
// reload calls Apply for every tenant under the registry lock: on a network
// backend, Commit and Release can hold the read lock for as long as an
// outage lasts, and the write lock waits out every reader, so an
// unconditional write lock here would serialize the whole reload behind
// them, tenant after tenant.
func (m *Managed) Apply(enabled bool) error {
	if m.settled(enabled) {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settledLocked(enabled) {
		return nil
	}
	m.enabled = enabled
	switch {
	case enabled && m.db == nil:
		db, err := m.open()
		if err != nil {
			return err
		}
		m.db = db
	case !enabled && m.db != nil:
		err := m.db.Close()
		m.db = nil
		return err
	}
	return nil
}

// settled reports whether the store already matches enabled, under its own
// read lock.
func (m *Managed) settled(enabled bool) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.settledLocked(enabled)
}

// settledLocked is settled's condition for a caller already holding mu (read
// or write): an already-open store while enabling, or an already-closed one
// while disabling (db is nil whenever !enabled — Apply's own invariant — so
// disabling never needs the db pointer).
func (m *Managed) settledLocked(enabled bool) bool {
	return m.enabled == enabled && (!enabled || m.db != nil)
}

// Open reports whether the store is currently open.
func (m *Managed) Open() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db != nil
}

// Reserve collapses a key repeated inside keys to one backend claim — later
// occurrences answer Duplicate — reads a lease <= 0 as DefaultLease, and
// delegates the rest to the open store; ErrDisabled while switched off,
// ErrUnavailable while switched on but not open.
func (m *Managed) Reserve(ctx context.Context, keys []Key, lease time.Duration) ([]Claim, error) {
	for _, k := range keys {
		if k.Hashed() {
			hashedIDCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", k.Table)))
		}
	}
	if lease <= 0 {
		lease = DefaultLease
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.usable(); err != nil {
		return nil, err
	}
	first := make(map[Key]int, len(keys))
	unique := make([]Key, 0, len(keys))
	for _, k := range keys {
		if _, seen := first[k]; !seen {
			first[k] = len(unique)
			unique = append(unique, k)
		}
	}
	got, err := m.db.Reserve(ctx, unique, lease)
	if err != nil {
		return nil, err
	}
	if len(got) != len(unique) {
		if claimed := claimedOnly(got); len(claimed) > 0 {
			_ = m.db.Release(context.WithoutCancel(ctx), claimed)
		}
		return nil, fmt.Errorf("dedupe backend answered %d claims for %d keys", len(got), len(unique))
	}
	if len(unique) == len(keys) {
		return got, nil
	}
	claims := make([]Claim, len(keys))
	answered := make([]bool, len(unique))
	for i, k := range keys {
		j := first[k]
		if answered[j] {
			claims[i] = Claim{Key: k, Status: Duplicate}
			continue
		}
		answered[j] = true
		claims[i] = got[j]
	}
	return claims, nil
}

// Commit delegates the Claimed claims to the open store, with Reserve's
// switch semantics.
func (m *Managed) Commit(ctx context.Context, claims []Claim, retention time.Duration) error {
	return m.withClaimed(claims, func(db Deduplicator, claimed []Claim) error {
		return db.Commit(ctx, claimed, retention)
	})
}

// Release delegates the Claimed claims to the open store, with Reserve's
// switch semantics.
func (m *Managed) Release(ctx context.Context, claims []Claim) error {
	return m.withClaimed(claims, func(db Deduplicator, claimed []Claim) error {
		return db.Release(ctx, claimed)
	})
}

func (m *Managed) withClaimed(claims []Claim, do func(Deduplicator, []Claim) error) error {
	claimed := claimedOnly(claims)
	if len(claimed) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.usable(); err != nil {
		return err
	}
	return do(m.db, claimed)
}

// claimedOnly is the claims a backend's Commit and Release may be handed.
func claimedOnly(claims []Claim) []Claim {
	out := make([]Claim, 0, len(claims))
	for _, c := range claims {
		if c.Status == Claimed {
			out = append(out, c)
		}
	}
	return out
}

// usable is the switch's answer: nil when the store may be called. Callers
// hold mu.
func (m *Managed) usable() error {
	switch {
	case !m.enabled:
		return ErrDisabled
	case m.db == nil:
		return ErrUnavailable
	}
	return nil
}

// Close releases the store if open. Safe to call when already closed.
func (m *Managed) Close() error {
	return m.Apply(false)
}
