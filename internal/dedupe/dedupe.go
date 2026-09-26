package dedupe

import (
	"context"
	"time"
)

// DefaultLease is how long a Claimed key stays pending when the caller names
// no lease: long enough to cover a publish, short enough that a request that
// died mid-publish does not hold the id for long.
const DefaultLease = 30 * time.Second

// Key is one record's dedupe identity inside a tenant's store. The tenant is
// bound by the store (Stores.For), so a Key never carries it.
type Key struct {
	Table string
	ID    string
}

// Status is Reserve's verdict for one key.
type Status uint8

const (
	// Claimed is a first sighting within retention. The caller now holds a
	// pending claim and must Commit it once the record is published, or
	// Release it if the publish definitely failed. An abandoned claim lapses
	// after the lease.
	Claimed Status = iota + 1
	// Duplicate means the key was committed earlier and has not expired: skip
	// the record. Managed also answers it for a key repeated inside one
	// Reserve call, after its first occurrence, whatever the first answered.
	Duplicate
	// InFlight means another request holds a live claim on the key. Its
	// outcome is not known yet, so the caller answers 503 and the client
	// retries.
	InFlight
)

func (s Status) String() string {
	switch s {
	case Claimed:
		return "claimed"
	case Duplicate:
		return "duplicate"
	case InFlight:
		return "in_flight"
	default:
		return "unknown"
	}
}

// Claim is Reserve's answer for one key. Token is the backend's proof of
// ownership, opaque to callers; Release compares it.
type Claim struct {
	Key    Key
	Status Status
	Token  string
}

// Deduplicator is a tenant's store of seen ids. Callers reach every backend
// through Managed, which hands a backend distinct keys, a lease > 0, and
// only Claimed claims to Commit and Release — a backend may assume all
// three, and Managed's callers get the behaviour below either way.
//
// Reserve is atomic per key: of any number of concurrent Reserves for the
// same key — in this process or any other sharing the backend — at most one
// returns Claimed. It returns one Claim per key, in input order. On error it
// has released every claim it made (all-or-nothing from the caller's view),
// and the error wraps ErrUnavailable when retrying later can succeed
// (throttled, timed out, backend unreachable).
//
// Commit makes Claimed claims duplicates for retention (0 = no expiry) and
// ignores claims of any other status. It is unconditional: a commit that
// lands after its lease lapsed and another request re-claimed the key is
// still correct, because the committing request did publish.
//
// Release gives up the Claimed claims it still owns (token match); a claim
// that has lapsed or been re-claimed is left alone.
//
// There is deliberately no read-only check: every caller that asks "have I
// seen this" needs the claim too, and a separate read is how #390 happened.
type Deduplicator interface {
	Reserve(ctx context.Context, keys []Key, lease time.Duration) ([]Claim, error)
	Commit(ctx context.Context, claims []Claim, retention time.Duration) error
	Release(ctx context.Context, claims []Claim) error
	Close() error
}
