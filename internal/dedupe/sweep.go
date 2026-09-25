package dedupe

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The sweep's cadence. Expired keys are already absent to Reserve, so the
// sweep only reclaims space and can run rarely; the first pass comes soon
// after the instance opens so an upgrade's version-0 keys go without waiting
// an hour.
const (
	sweepInterval   = time.Hour
	sweepFirstDelay = time.Minute
	// sweepChunk keys are read and deleted per lock hold, with sweepPause
	// between chunks: at most ~100k keys a second, and a Commit never waits
	// longer than one chunk.
	sweepChunk = 1024
	sweepPause = 10 * time.Millisecond
)

// Swept-key reasons, the metric's reason attribute.
const (
	sweptExpired   = "expired"
	sweptVersion0  = "version_0"
	sweptAttribute = "reason"
)

var sweptKeysCounter, _ = otel.Meter("wavehouse-dedupe").Int64Counter(
	"wavehouse_dedupe_swept_keys_total",
	metric.WithDescription("Keys the embedded dedupe sweep deleted, by reason: expired (retention ended) or version_0 (the layout before ids were keyed by table)"),
)

// sweepResult is what a sweep deleted.
type sweepResult struct {
	Expired, Version0 int
}

// startSweep runs the sweep over db until the returned stop is called; stop
// waits for a chunk in progress to finish. Callers hold e.mu.
func (e *Embedded) startSweep(db *pebble.DB) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		wait := e.sweepFirst
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			wait = e.sweepEvery
			res, err := e.sweep(ctx, db)
			switch {
			case err != nil && ctx.Err() == nil:
				slog.WarnContext(ctx, "dedupe sweep failed; retrying next interval", "error", err, "expired", res.Expired, "version_0", res.Version0)
			case res.Expired+res.Version0 > 0:
				slog.InfoContext(ctx, "dedupe sweep deleted keys", "expired", res.Expired, "version_0", res.Version0)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// sweep makes one pass over the whole instance, deleting keys whose
// retention has ended and version-0 keys, which never count: those from
// before ids were keyed by table (tenant ‖ 0x00 ‖ id, or the bare id before
// that). They are told apart by value, since a bare id may be any bytes, a
// current key's included: only commits are stored, and every commit has the
// committedMark layout. It stops early, without error, when ctx ends.
func (e *Embedded) sweep(ctx context.Context, db *pebble.DB) (sweepResult, error) {
	var res sweepResult
	var from []byte
	for {
		next, err := e.sweepChunk(ctx, db, from, &res)
		if err != nil || next == nil {
			return res, err
		}
		from = next
		select {
		case <-ctx.Done():
			return res, nil
		case <-time.After(sweepPause):
		}
	}
}

// sweepChunk deletes the sweepable keys among the next sweepChunk keys from
// from, returning where the next chunk starts (nil at the end). It holds
// commitMu, so no Commit lands between reading a key and deleting it: a key
// re-committed after it expired is never deleted with its new value.
func (e *Embedded) sweepChunk(ctx context.Context, db *pebble.DB, from []byte, res *sweepResult) ([]byte, error) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	now := e.now()
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: from})
	if err != nil {
		return nil, fmt.Errorf("dedupe sweep: %w", err)
	}
	b := db.NewBatch()
	defer func() { _ = b.Close() }()
	var next []byte
	var expired, v0 int64
	seen := 0
	for valid := it.First(); valid; valid = it.Next() {
		if seen == sweepChunk {
			next = bytes.Clone(it.Key())
			break
		}
		seen++
		k := it.Key()
		val := it.Value()
		switch {
		case !isCommit(val):
			v0++
		case committedExpired(val, now):
			expired++
		default:
			continue
		}
		if err := b.Delete(k, nil); err != nil {
			_ = it.Close()
			return nil, fmt.Errorf("dedupe sweep: %w", err)
		}
	}
	if err := it.Close(); err != nil {
		return nil, fmt.Errorf("dedupe sweep: %w", err)
	}
	if e.sweepHook != nil {
		e.sweepHook()
	}
	// NoSync: a delete lost to a crash is redone by the next pass.
	if err := b.Commit(pebble.NoSync); err != nil {
		return nil, fmt.Errorf("dedupe sweep: %w", err)
	}
	res.Expired += int(expired)
	res.Version0 += int(v0)
	if expired > 0 {
		sweptKeysCounter.Add(ctx, expired, metric.WithAttributes(attribute.String(sweptAttribute, sweptExpired)))
	}
	if v0 > 0 {
		sweptKeysCounter.Add(ctx, v0, metric.WithAttributes(attribute.String(sweptAttribute, sweptVersion0)))
	}
	return next, nil
}
