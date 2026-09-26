// Package coordtest is the behavior every coord.Coordinator must share, as
// one suite each implementation runs against its own backend.
package coordtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/coord"
)

// Factory returns two coordinators over one fresh backend, as two processes
// would hold them. Conformance closes both when each case ends.
type Factory func(t *testing.T) (a, b coord.Coordinator)

// Option adjusts the suite to what a backend can do.
type Option func(*options)

type options struct {
	lose func(t *testing.T, name string)
	wait time.Duration
}

// WithLoss is how the backend ends the live term of name out from under
// its holder, as a lost renewal or a takeover would. Without it the loss
// case is skipped: an in-process lease is never lost.
func WithLoss(lose func(t *testing.T, name string)) Option {
	return func(o *options) { o.lose = lose }
}

// WithWait bounds how long the suite waits for something the backend does
// asynchronously, such as noticing a loss. Default 2s.
func WithWait(d time.Duration) Option {
	return func(o *options) { o.wait = d }
}

// Conformance runs the shared suite: exclusivity, token monotonicity, Resign
// lets the other in, loss closes Done, Close resigns, ctx cancellation.
func Conformance(t *testing.T, newPair Factory, opts ...Option) {
	t.Helper()
	o := options{wait: 2 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	pair := func(t *testing.T) (a, b coord.Coordinator) {
		a, b = newPair(t)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), o.wait)
			defer cancel()
			assert.NoError(t, a.Close(ctx))
			assert.NoError(t, b.Close(ctx))
		})
		return a, b
	}

	t.Run("one holder at a time", func(t *testing.T) {
		a, b := pair(t)
		term := acquire(t, a, "lease")
		assert.Equal(t, "lease", term.Name())
		assertOpen(t, term)

		_, err := b.TryAcquire(t.Context(), "lease")
		require.ErrorIs(t, err, coord.ErrHeld, "another holder's live lease")
		_, err = a.TryAcquire(t.Context(), "lease")
		require.ErrorIs(t, err, coord.ErrHeld, "a lease this coordinator already holds")

		other := acquire(t, b, "other")
		assertOpen(t, other)
		assertOpen(t, term)
	})

	t.Run("resign lets the other in with a greater token", func(t *testing.T) {
		a, b := pair(t)
		first := acquire(t, a, "lease")
		require.NoError(t, first.Resign(t.Context()))
		assertEnded(t, first, o.wait)
		require.NoError(t, first.Err(), "a resigned term ended cleanly")
		require.NoError(t, first.Resign(t.Context()), "resigning an ended term is a no-op")

		second := acquire(t, b, "lease")
		assert.Greater(t, second.Token(), first.Token())
		require.NoError(t, second.Resign(t.Context()))

		third := acquire(t, a, "lease")
		assert.Greater(t, third.Token(), second.Token(), "monotonic across holders, back to the first")
	})

	t.Run("close resigns every term and refuses more", func(t *testing.T) {
		a, b := pair(t)
		one := acquire(t, a, "one")
		two := acquire(t, a, "two")
		kept := acquire(t, b, "kept")

		require.NoError(t, a.Close(t.Context()))
		assertEnded(t, one, o.wait)
		assertEnded(t, two, o.wait)
		require.NoError(t, one.Err(), "a term closed with its coordinator ended cleanly")
		assertOpen(t, kept)

		_, err := a.TryAcquire(t.Context(), "three")
		require.ErrorIs(t, err, coord.ErrClosed)
		require.NoError(t, a.Close(t.Context()), "closing twice")
		acquire(t, b, "one")
	})

	t.Run("ctx bounds the call, not the term", func(t *testing.T) {
		a, b := pair(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := a.TryAcquire(ctx, "lease")
		require.ErrorIs(t, err, context.Canceled)

		ctx, cancel = context.WithCancel(t.Context())
		term, err := b.TryAcquire(ctx, "lease")
		require.NoError(t, err)
		cancel()
		assertOpen(t, term)
		_, err = a.TryAcquire(t.Context(), "lease")
		require.ErrorIs(t, err, coord.ErrHeld, "the term outlives the context it was taken under")
	})

	t.Run("loss closes Done", func(t *testing.T) {
		if o.lose == nil {
			t.Skip("this backend never loses a live term")
		}
		a, _ := pair(t)
		term := acquire(t, a, "lease")
		o.lose(t, "lease")
		assertEnded(t, term, o.wait)
		require.ErrorIs(t, term.Err(), coord.ErrLost)
		require.NoError(t, term.Resign(t.Context()), "resigning a lost term is a no-op")
	})
}

func acquire(t *testing.T, c coord.Coordinator, name string) coord.Term {
	t.Helper()
	term, err := c.TryAcquire(t.Context(), name)
	require.NoError(t, err)
	require.NotNil(t, term)
	return term
}

func assertOpen(t *testing.T, term coord.Term) {
	t.Helper()
	select {
	case <-term.Done():
		t.Fatalf("term %s ended: %v", term.Name(), term.Err())
	default:
	}
	require.NoError(t, term.Err())
}

func assertEnded(t *testing.T, term coord.Term, wait time.Duration) {
	t.Helper()
	select {
	case <-term.Done():
	case <-time.After(wait):
		t.Fatalf("term %s still live after %v", term.Name(), wait)
	}
	if err := term.Err(); err != nil && !errors.Is(err, coord.ErrLost) {
		t.Fatalf("term %s ended with %v, want nil or coord.ErrLost", term.Name(), err)
	}
}
