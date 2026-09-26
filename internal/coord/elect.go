package coord

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// resignTimeout bounds the resign that hands the lease on when fn returns,
// on a context detached from the one that may just have been canceled.
const resignTimeout = 5 * time.Second

// RunElected blocks until ctx is done, running fn only while this process
// holds the named lease. It campaigns every retry, runs fn with a context
// canceled when the term ends, resigns when fn returns, and campaigns
// again. An error fn returns while its term is live is returned — fatal to
// the caller, like any component's; one it returns on its way out of an
// ended term is its stop, not a failure. ErrHeld and a lost term are the
// election working. A failed campaign is logged and retried, since the
// backend may be briefly unreachable; only ErrClosed ends the loop early.
func RunElected(ctx context.Context, c Coordinator, name string, retry time.Duration,
	fn func(ctx context.Context, term Term) error,
) error {
	for {
		term, err := c.TryAcquire(ctx, name)
		switch {
		case err == nil:
			if ferr := serve(ctx, term, fn); ferr != nil {
				return ferr
			}
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, ErrHeld):
		case errors.Is(err, ErrClosed):
			return err
		default:
			slog.WarnContext(ctx, "coord: campaign failed, retrying", "lease", name, "error", err)
		}
		t := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// serve runs fn for the length of one term and resigns it afterwards.
func serve(ctx context.Context, term Term, fn func(context.Context, Term) error) error {
	slog.InfoContext(ctx, "coord: elected", "lease", term.Name(), "token", term.Token())
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	go func() {
		select {
		case <-term.Done():
			cancel()
		case <-stop:
		}
	}()
	err := fn(tctx, term)
	close(stop)
	stopped := tctx.Err() != nil
	cancel()

	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), resignTimeout)
	defer rcancel()
	if rerr := term.Resign(rctx); rerr != nil {
		// The lease then runs out on its own; the successor waits for it.
		slog.WarnContext(ctx, "coord: resign failed", "lease", term.Name(), "error", rerr)
	}
	if lost := term.Err(); lost != nil {
		slog.WarnContext(ctx, "coord: term ended", "lease", term.Name(), "token", term.Token(), "error", lost)
	}
	if stopped {
		return nil
	}
	return err
}
