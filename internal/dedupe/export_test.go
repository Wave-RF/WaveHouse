package dedupe

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// SetClock replaces e's clock, for tests that let leases and retentions lapse
// without sleeping.
func SetClock(e *Embedded, now func() time.Time) { e.now = now }

// FailNextReserve makes e's next Reserve fail after it has claimed n keys,
// once.
func FailNextReserve(e *Embedded, n int) {
	var reads atomic.Int64
	var failed atomic.Bool
	e.readHook = func() error {
		if reads.Add(1) > int64(n) && failed.CompareAndSwap(false, true) {
			return errors.New("injected read failure")
		}
		return nil
	}
}

// mark reserves and commits id in table "events", reporting whether it was
// already committed — the old check-and-mark, for tests about everything
// else.
func mark(ctx context.Context, d Deduplicator, id string) (bool, error) {
	claims, err := d.Reserve(ctx, []Key{{Table: "events", ID: id}}, DefaultLease)
	if err != nil {
		return false, err
	}
	if claims[0].Status != Claimed {
		return true, nil
	}
	return false, d.Commit(ctx, claims, 0)
}
