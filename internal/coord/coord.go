// Package coord holds leases for work that must run in one process at a
// time — the sweeper today, partition claims later. A lease is taken with
// TryAcquire and held as a Term until it is resigned, its coordinator is
// closed, or the backend reports it lost; RunElected drives a leader loop
// over one. Local is the in-process implementation; a distributed one lives
// with the connection it rides on (a NATS KV bucket in internal/mq), so this
// package imports only the standard library.
//
// Every implementation runs the shared suite in coordtest.
package coord

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrHeld is TryAcquire's answer when the lease is live under another
	// holder — or under this coordinator already.
	ErrHeld = errors.New("coord: lease held")
	// ErrLost is what Term.Err wraps when the backend ended the term:
	// renewal failed past its deadline, or another holder took the lease.
	ErrLost = errors.New("coord: lease lost")
	// ErrClosed is TryAcquire's answer once the coordinator is closed.
	ErrClosed = errors.New("coord: coordinator closed")
)

// RetryPeriod is how often a candidate campaigns for a lease it does not
// hold (client-go's leader-election default).
const RetryPeriod = 2 * time.Second

// Coordinator hands out named leases.
type Coordinator interface {
	// TryAcquire takes the named lease if nobody holds a live one and keeps
	// it until Resign, Close, or loss; ctx bounds the call, not the term.
	// ErrHeld when the lease is live, including under this coordinator.
	TryAcquire(ctx context.Context, name string) (Term, error)
	// Close resigns every term this coordinator holds (best effort, within
	// ctx); TryAcquire returns ErrClosed from then on. Safe to call again.
	Close(ctx context.Context) error
}

// Term is one holding of a lease.
type Term interface {
	Name() string
	// Token is the fencing token: strictly greater than every earlier
	// term's token for the same name on the same backend. Anything that
	// needs exclusivity, not just mostly-one-at-a-time, must check it
	// against what it writes: a holder that stalls past the lease duration
	// can overlap its successor.
	Token() uint64
	// Done closes when the term ends: resigned, its coordinator closed, or
	// lost. Work under the lease must stop promptly.
	Done() <-chan struct{}
	// Err is why Done closed: nil after Resign or Close, wrapping ErrLost
	// after a loss. Nil while the term is live.
	Err() error
	// Resign ends the term and frees the lease for the next candidate. A
	// term that has already ended resigns as a no-op.
	Resign(ctx context.Context) error
}
