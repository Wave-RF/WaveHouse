package dedupe

import (
	"bytes"
	"context"
	"errors"
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
	// sweepChunk keys are read per chunk, with sweepPause between chunks: at
	// most ~100k keys a second. A Commit waits only for a chunk's re-reads
	// and deletes, never for its read, however many tombstones it skips.
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
// from, returning where the next chunk starts (nil at the end). It reads them
// without commitMu, since Pebble skips the tombstones between keys inside the
// read and a run of them left by an earlier pass would otherwise hold every
// Commit for its whole length.
func (e *Embedded) sweepChunk(ctx context.Context, db *pebble.DB, from []byte, res *sweepResult) ([]byte, error) {
	candidates, next, err := sweepCandidates(db, from, e.now())
	if err != nil || len(candidates) == 0 {
		return next, err
	}
	if e.sweepScanHook != nil {
		e.sweepScanHook()
	}
	expired, v0, err := e.deleteSweepable(db, candidates)
	if err != nil {
		return nil, err
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

// sweepCandidates reads the next sweepChunk keys, starting at from, and
// returns those sweepable at now and where the next chunk starts (nil at the
// end).
func sweepCandidates(db *pebble.DB, from []byte, now time.Time) (candidates [][]byte, next []byte, err error) {
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: from})
	if err != nil {
		return nil, nil, fmt.Errorf("dedupe sweep: %w", err)
	}
	seen := 0
	for valid := it.First(); valid; valid = it.Next() {
		if seen == sweepChunk {
			next = bytes.Clone(it.Key())
			break
		}
		seen++
		if sweepReason(it.Value(), now) != "" {
			candidates = append(candidates, bytes.Clone(it.Key()))
		}
	}
	if err := it.Close(); err != nil {
		return nil, nil, fmt.Errorf("dedupe sweep: %w", err)
	}
	return candidates, next, nil
}

// deleteSweepable re-reads each candidate and deletes those still sweepable,
// holding commitMu so no Commit lands between the re-read and the delete: a
// key re-committed after the unlocked read is never deleted with its new
// value.
func (e *Embedded) deleteSweepable(db *pebble.DB, candidates [][]byte) (expired, v0 int64, err error) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	now := e.now()
	b := db.NewBatch()
	defer func() { _ = b.Close() }()
	for _, k := range candidates {
		val, closer, err := db.Get(k)
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return 0, 0, fmt.Errorf("dedupe sweep: %w", err)
		}
		reason := sweepReason(val, now)
		_ = closer.Close()
		switch reason {
		case sweptVersion0:
			v0++
		case sweptExpired:
			expired++
		default:
			continue
		}
		if err := b.Delete(k, nil); err != nil {
			return 0, 0, fmt.Errorf("dedupe sweep: %w", err)
		}
	}
	if e.sweepDeleteHook != nil {
		e.sweepDeleteHook()
	}
	// NoSync: a delete lost to a crash is redone by the next pass.
	if err := b.Commit(pebble.NoSync); err != nil {
		return 0, 0, fmt.Errorf("dedupe sweep: %w", err)
	}
	return expired, v0, nil
}

// sweepReason is why the sweep deletes a key holding val at now, or "" when
// it keeps it.
func sweepReason(val []byte, now time.Time) string {
	switch {
	case !isCommit(val):
		return sweptVersion0
	case committedExpired(val, now):
		return sweptExpired
	}
	return ""
}
