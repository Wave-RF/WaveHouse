package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/singleflight"
)

// slogNATSLogger adapts the default slog logger to the natsserver.Logger
// interface.
type slogNATSLogger struct{}

func (slogNATSLogger) Noticef(format string, v ...any) {
	slog.Info(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Warnf(format string, v ...any) {
	slog.Warn(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Fatalf(format string, v ...any) {
	slog.Error(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Errorf(format string, v ...any) {
	slog.Error(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Debugf(format string, v ...any) {
	slog.Debug(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Tracef(format string, v ...any) {
	slog.Debug(fmt.Sprintf(format, v...), "component", "nats")
}

// EmbeddedNATS runs an in-process NATS server with JetStream, and gives each
// tenant a queue of its own, laid out as an external NATS is (NATSTopology):
// an ingest stream with work-queue retention, which the ingest worker's
// durable consumer drains and which republishes every row it stores to the
// tenant's live subject; a history stream the worker records every written
// row on, kept for the tenant's replay window; and a dead-letter stream
// (subject.go names them), each with its own byte cap. Nothing outside this
// package sees that layout.
type EmbeddedNATS struct {
	server *natsserver.Server
	conn   *nats.Conn
	js     jetstream.JetStream
	// hist records written rows on the tenants' history streams (Record).
	hist historyWriter

	// mu guards queues, consumers and writes to opened, and serializes
	// opening or resizing a tenant's queue with registering a consumer, so a
	// queue opened while a consumer registers is never missed by it. It is
	// held across the JetStream calls that open or resize a queue.
	mu     sync.Mutex
	queues map[tenant.ID]*tenantQueue
	// consumers are the durable consumers held on every tenant's queue, each
	// joined to a queue as it opens.
	consumers []*fanIn
	// opened holds the tenants whose queue has its three streams and every
	// registered consumer joined to it — what Publish trusts, rather than a
	// stream answering: an open that gave up can leave behind a stream
	// JetStream goes on to create, which no consumer holds, and a queue a
	// consumer could not join takes no row until it has. Written under mu,
	// read without it.
	opened sync.Map // tenant.ID → struct{}
	// windows holds each tenant's replay window as last asked (SetReplayWindow,
	// or read back from its history stream at boot): Record reads it without
	// mu, which a resize can hold for seconds.
	windows sync.Map // tenant.ID → time.Duration
	// reopening merges into one attempt the publishes and parks that find
	// the same tenant's queue not open, and failedOpen holds, for a tenant
	// whose last such attempt failed, its error and until when its publishes
	// and parks take that as their answer (reopenPaced).
	reopening  singleflight.Group
	failedOpen sync.Map // tenant.ID → openFailure
}

// openFailure is a publish's or park's failed attempt to open a tenant's
// queue, and until when the tenant's publishes and parks are refused with its
// error rather than trying again.
type openFailure struct {
	until time.Time
	err   error
}

// tenantQueue is what the broker knows of one tenant's queue.
type tenantQueue struct {
	// ingest, history and dlq report whether each of the tenant's streams
	// exists.
	ingest, history, dlq bool
	// maxBytes is the budget last applied in full (MaxBytes), every
	// registered consumer joined; asked is the budget last asked for, which a
	// publish or park that finds the queue not open opens it at. Boot reads
	// asked back from the ingest stream, so a tenant no longer served keeps
	// the budget it last had, and maxBytes too when the pair is whole at it
	// (takeStock).
	maxBytes, asked int64
	// ingestCap is the cap the ingest stream has — what a failed resize
	// restores it to. Not maxBytes: a queue boot found split has a cap but no
	// budget applied in full, and a cap of 0 would be none at all. historyCap
	// is the history stream's, which a change of window keeps.
	ingestCap, historyCap int64
	// window is the replay window last asked for the tenant, which its
	// history stream is opened with and e.windows mirrors.
	window time.Duration
}

// EmbeddedNATS implements every mq interface.
var _ Broker = (*EmbeddedNATS)(nil)

const (
	// dlqShare is a tenant's dead-letter stream's slice of its byte budget: a
	// tenth of the ingest stream's cap. historyShare is its history stream's,
	// the same: both come on top of the budget.
	dlqShare     = 10
	historyShare = 10
	// noHistoryAge is the max_age of the history stream of a tenant whose
	// replay window is 0: a stream has none of 0, which would be no limit, so
	// it keeps what it holds for a second, and nothing is recorded on it. A
	// window is whole minutes, so an age under a minute read back at boot is
	// this one.
	noHistoryAge = time.Second
	// recordTimeout bounds one Record: what the acks of a written batch wait
	// for the history at most.
	recordTimeout = 5 * time.Second
	// resizeTimeout bounds the JetStream calls SetMaxBytes makes to open a
	// tenant's queue or apply a new cap to it — both streams share it. A
	// settings reload holds the store's lock while its hooks run, so an
	// in-process JetStream call that never returns would otherwise block every
	// later reload.
	resizeTimeout = 10 * time.Second
	// rollbackTimeout is the undo's own budget when the dead-letter resize
	// fails: in-process JetStream fails by stalling rather than erroring, so
	// the likely cause is that resizeTimeout has just run out, and an undo on
	// that context would fail without touching the stream. SetMaxBytes runs
	// for at most the sum of the two when a resize fails, and for two
	// resizeTimeouts otherwise: the consumers join on a budget of their own
	// (joinConsumers).
	rollbackTimeout = 5 * time.Second
	// reopenRetry is how long a tenant's publishes and parks are refused at
	// once after one failed to open its queue (reopenPaced).
	reopenRetry = 5 * time.Second
)

// EmbeddedSyncAlways is NewEmbedded's SyncAlways. Only a TestMain may turn it
// off, before any broker starts: unit tests assert nothing across a crash, and
// on macOS an fsync per write is most of their run time (#617).
var EmbeddedSyncAlways = true

// errNoQueue is why a publish or park finds no queue it can open: no budget
// has been asked for the tenant yet (see SetMaxBytes). Publish reports it as
// ErrQueueFull.
var errNoQueue = errors.New("no queue is open for it yet")

// NewEmbedded starts an embedded NATS server with JetStream over storeDir and
// takes stock of the tenants' queues already there: a consumer created later
// is held on every one of them, those of tenants no longer served included,
// whose queued rows still have to reach the ingest worker. The pair of streams
// an earlier build kept for every tenant together is deleted, since its
// subjects overlap every tenant's, and so is a tenant's ingest stream an
// earlier build created with limits retention, which no update can make a
// work queue; the events they held are not carried over. A tenant's queue is
// opened by SetMaxBytes, the first time its budget is applied, or by a publish
// or park that finds it missing, at the budget last asked for it. The server
// logs through slog's default logger.
func NewEmbedded(storeDir string) (*EmbeddedNATS, error) {
	// A store the server cannot create fails JetStream in the background, and
	// ReadyForConnections would only give up on it after its whole wait.
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		return nil, fmt.Errorf("nats store: %w", err)
	}
	opts := &natsserver.Options{
		DontListen: true,
		JetStream:  true,
		StoreDir:   storeDir,
		SyncAlways: EmbeddedSyncAlways, // fsync every JetStream write — publish ACKs only after data is on disk
		// Without NoSigs, Start() installs a process-wide SIGINT handler that
		// races the app's graceful shutdown (double Shutdown → "close of nil
		// channel" panic) and os.Exit(0)s past its cleanup. WaveHouse owns
		// the lifecycle; Close() shuts the server down. See #287.
		NoSigs: true,
		// JetStream counts every stream's byte cap as reserved disk and
		// refuses a stream once the caps together pass this limit — by
		// default 75% of the free disk at boot. A tenant's mq.max_bytes_gb
		// caps that tenant's queue and nothing else; what the tenants' caps
		// add up to against the disk is #138's to decide, not a limit the
		// server enforces on the side, so its own is set out of reach. Half
		// the int64 range, not all of it: the server subtracts its count of
		// reserved bytes from this limit, and a stream whose store fails to
		// open releases a reservation it never made (nats-server 2.14.6), so
		// the count can fall below zero — at the top of the range that
		// subtraction overflows, and every stream after it is refused.
		JetStreamMaxStore: math.MaxInt64 / 2,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("new nats server: %w", err)
	}
	ns.SetLogger(slogNATSLogger{}, false, false)
	ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, fmt.Errorf("nats server not ready")
	}

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect to embedded nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("jetstream new: %w", err)
	}

	hist, err := newHistoryWriter(nc, "", recordTimeout)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, err
	}

	e := &EmbeddedNATS{server: ns, conn: nc, js: js, hist: hist, queues: map[tenant.ID]*tenantQueue{}}
	if err := e.takeStock(context.Background()); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

// takeStock deletes the pair of streams an earlier build kept for every
// tenant together and each tenant's ingest stream of another retention, then
// records every tenant stream on disk, with the budget its ingest stream last
// had and the replay window its history stream last had.
func (e *EmbeddedNATS) takeStock(ctx context.Context) error {
	for _, name := range []string{legacyIngestStream, legacyDLQStream} {
		if err := e.deleteLegacy(ctx, name); err != nil {
			return err
		}
	}
	type dlqState struct {
		limit int64
		held  uint64
	}
	dlqs := map[tenant.ID]dlqState{}
	// current holds the tenants whose ingest stream is configured as
	// ingestStreamConfig would have it, bar its cap: a stream from before a
	// field took its value, or reopened under another, must not be counted as
	// already at budget below, or SetMaxBytes(same budget) short-circuits and
	// the stale value is never brought forward (measured: a stream with
	// Duplicates=10s kept 10s after NewEmbedded + SetMaxBytes(same budget)).
	current := map[tenant.ID]bool{}
	var earlier []*jetstream.StreamInfo
	streams := e.js.ListStreams(ctx)
	for info := range streams.Info() {
		name := info.Config.Name
		if id, ok := streamTenant(ingestStreamPrefix, name); ok {
			q := e.queue(id)
			q.asked = info.Config.MaxBytes
			if info.Config.Retention != jetstream.WorkQueuePolicy {
				earlier = append(earlier, info)
				continue
			}
			q.ingest, q.ingestCap = true, info.Config.MaxBytes
			current[id] = sameIngestStream(info.Config, ingestStreamConfig(id, info.Config.MaxBytes))
		} else if id, ok := streamTenant(historyStreamPrefix, name); ok {
			q := e.queue(id)
			q.history, q.historyCap = true, info.Config.MaxBytes
			if age := info.Config.MaxAge; age >= time.Minute {
				q.window = age
			}
			e.windows.Store(id, q.window)
		} else if id, ok := streamTenant(dlqStreamPrefix, name); ok {
			e.queue(id).dlq = true
			dlqs[id] = dlqState{limit: info.Config.MaxBytes, held: info.State.Bytes}
		}
	}
	if err := streams.Err(); err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	for _, info := range earlier {
		if err := e.deleteEarlier(ctx, info); err != nil {
			return err
		}
	}
	// A queue is at its budget when its dead-letter stream is at a tenth of
	// the ingest cap, or above it holding more than that: the shrink guard's
	// doing, its history stream at a tenth too, AND its ingest stream is
	// configured as this build configures one. Anything else is a queue a
	// stop or a failed update left split, one missing a stream, or one whose
	// configuration is stale, so its budget stays unapplied and the boot's
	// SetMaxBytes applies it — and the current configuration — to every
	// stream again.
	for id, q := range e.queues {
		d, ok := dlqs[id]
		tenth := q.asked / dlqShare
		guarded := d.limit > tenth && d.held <= math.MaxInt64 && int64(d.held) > tenth
		if q.ingest && ok && (d.limit == tenth || guarded) && q.history && q.historyCap == q.asked/historyShare && current[id] {
			q.maxBytes = q.asked
		}
		e.note(id, q)
	}
	return nil
}

// sameIngestStream reports whether an ingest stream holds the fields this
// package sets on one, bar its cap.
func sameIngestStream(have, want jetstream.StreamConfig) bool {
	return have.Duplicates == want.Duplicates && have.AllowDirect == want.AllowDirect &&
		have.RePublish != nil && *have.RePublish == *want.RePublish
}

// deleteLegacy deletes one stream of the pair an earlier build kept for every
// tenant together, logging what it held; one that is not there is nothing to
// do.
func (e *EmbeddedNATS) deleteLegacy(ctx context.Context, name string) error {
	s, err := e.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("look up stream %s: %w", name, err)
	}
	held := s.CachedInfo().State.Msgs
	if err := e.js.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("delete stream %s: %w", name, err)
	}
	slog.Warn("mq: deleted the stream an earlier build kept for every tenant together; its messages are not carried over",
		"component", "nats", "stream", name, "messages", held)
	return nil
}

// deleteEarlier deletes a tenant's ingest stream an earlier build created
// with limits retention: the server refuses the update that would make it a
// work queue, so the stream is created again when its queue next opens, at
// the budget it had. It logs what the stream held, and how many of those
// messages the ingest worker had not acknowledged — rows that had not reached
// ClickHouse, which are lost with it; the rest was replay history. A stream
// the worker never consumed held nothing but such rows.
func (e *EmbeddedNATS) deleteEarlier(ctx context.Context, info *jetstream.StreamInfo) error {
	name := info.Config.Name
	unwritten := info.State.Msgs
	if c, err := e.js.Consumer(ctx, name, workerDurable); err == nil {
		ci := c.CachedInfo()
		unwritten = ci.NumPending + uint64(max(0, ci.NumAckPending))
	} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return fmt.Errorf("look up the ingest consumer of stream %s: %w", name, err)
	}
	if err := e.js.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("delete stream %s: %w", name, err)
	}
	slog.Warn("mq: deleted the ingest stream an earlier build kept with limits retention, which cannot become a work queue; its messages are not carried over, and the unwritten ones never reached ClickHouse",
		"component", "nats", "stream", name, "messages", info.State.Msgs, "unwritten", unwritten)
	return nil
}

// queue returns what the broker knows of tenant id's queue, recording the
// tenant first if it knows nothing. Under e.mu (or before e is shared).
func (e *EmbeddedNATS) queue(id tenant.ID) *tenantQueue {
	q := e.queues[id]
	if q == nil {
		q = &tenantQueue{}
		e.queues[id] = q
	}
	return q
}

// ingestTenants lists the tenants whose ingest stream exists, in id order.
// Under e.mu.
func (e *EmbeddedNATS) ingestTenants() []tenant.ID {
	var ids []tenant.ID
	for id, q := range e.queues {
		if q.ingest {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// note brings opened in line with what the broker knows of tenant id's
// queue: open with its three streams known and every registered consumer
// joined to it. A queue a consumer could not join stays unnoted, so the
// tenant's publishes are refused until a publish or a reload joins it. Under
// e.mu (or before e is shared).
func (e *EmbeddedNATS) note(id tenant.ID, q *tenantQueue) {
	if q.ingest && q.history && q.dlq && e.joined(id) {
		e.opened.Store(id, struct{}{})
		e.failedOpen.Delete(id)
	} else {
		e.opened.Delete(id)
	}
}

// EmbeddedDuplicateWindow is how long an ingest queue remembers a
// WithIdempotencyKey key. It must be at least lease + ceil(lease) + 1s
// (2*lease + 1s for a whole-second lease), which config checks against
// dedupe.lease at boot. A claim left to lapse after an uncertain publish is
// republished once the lease ends, but the in-flight 503 tells a client to
// retry only after the FULL lease, so an obedient client's retry can land up
// to ~2*lease after the original Reserve; the +1s covers a backend (DynamoDB, for one) that rounds a
// claim's expiry up by as much. Only a window at least that long guarantees
// this queue still drops the retry's second copy.
const EmbeddedDuplicateWindow = 2 * time.Minute

// ingestStreamConfig is tenant id's ingest stream. WorkQueuePolicy: a row
// leaves when the ingest worker acks it and not before, whatever the rows
// around it, so one left unacked holds back only itself (#653). MaxBytes caps
// the tenant's unwritten rows on the disk. DiscardNew rejects new messages
// when full, propagating backpressure to the upstream API — for this tenant
// alone. Every row it stores is republished to the tenant's live subject, and
// a replay reads the ones still waiting by direct get, a work queue taking no
// second consumer.
func ingestStreamConfig(id tenant.ID, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        ingestStreamName(id),
		Subjects:    []string{tenantSubjects(ingestPrefix, id)},
		Retention:   jetstream.WorkQueuePolicy,
		MaxBytes:    maxBytes,
		Discard:     jetstream.DiscardNew,
		Duplicates:  EmbeddedDuplicateWindow,
		AllowDirect: true,
		RePublish:   &jetstream.RePublish{Source: tenantSubjects(ingestPrefix, id), Destination: tenantSubjects(livePrefix, id)},
	}
}

// historyStreamConfig is tenant id's history stream: every row the worker has
// written, kept for window from the write (the stream's own clock starts
// when it stores a row), which is how long a replay can still send it.
// DiscardOld: a full one drops its oldest rows rather than refusing the
// newest, as the dead-letter stream does.
func historyStreamConfig(id tenant.ID, maxBytes int64, window time.Duration) jetstream.StreamConfig {
	age := max(window, noHistoryAge)
	return jetstream.StreamConfig{
		Name:      historyStreamName(id),
		Subjects:  []string{tenantSubjects(histPrefix, id)},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		MaxAge:    age,
		Discard:   jetstream.DiscardOld,
		// No row is published here under an idempotency key, and the server
		// refuses a window longer than max_age.
		Duplicates: min(age, EmbeddedDuplicateWindow),
	}
}

// dlqStreamConfig is tenant id's dead-letter stream. DiscardOld: a full one
// drops its oldest parked rows rather than refusing new ones — backpressure
// belongs to the ingest stream, not the dead-letter one.
func dlqStreamConfig(id tenant.ID, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      dlqStreamName(id),
		Subjects:  []string{tenantSubjects(dlqPrefix, id)},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	}
}

// MaxBytes reports the budget tenant id's queue was last given in full (by
// SetMaxBytes, or read back from disk at boot), 0 when it has none.
func (e *EmbeddedNATS) MaxBytes(id tenant.ID) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if q := e.queues[id]; q != nil {
		return q.maxBytes
	}
	return 0
}

// SetMaxBytes applies tenant id's byte budget (its hot-reloadable
// mq.max_bytes_gb) to its queue: the ingest stream takes maxBytes, and the
// history and dead-letter streams a tenth of it each. A tenant with no queue
// yet has one opened, its dead-letter and history streams first, so no row is
// queued that could not be parked or recorded, and every registered consumer
// joins it. No other tenant's queue is touched.
//
// A consumer that cannot join is this tenant's failure alone: the error says
// so, the queue stays closed to the tenant's publishes (ErrQueueFull), and
// the consumers that had not joined are tried again by the next call and by
// the tenant's next publish (reopenPaced). No consumer's delivery from
// another tenant's queue is touched.
//
// JetStream applies a limit change to a live stream without touching its
// messages: growing takes effect immediately; shrinking the ingest stream
// below its current size makes DiscardNew refuse new publishes until the
// worker has written it back under the cap — nothing queued is dropped. The
// dead-letter stream is DiscardOld, which would delete its oldest parked rows
// to fit a smaller cap, so it is never capped below the bytes it holds (#532):
// it keeps what it has, and that is logged. The history stream is DiscardOld
// too and has no such guard: what a smaller cap drops is its oldest rows,
// which are in ClickHouse.
//
// The streams move together where they can. If the dead-letter update fails
// after the ingest one succeeded, the ingest resize is undone so the two stay
// at the previous budget, and the next call retries both. Safe in that
// direction — the ingest stream is DiscardNew, so shrinking it back drops
// nothing stored. The undo is best effort: if it fails too, the ingest stream
// stays at the new limit and the dead-letter one at the previous, and the
// error says so. The history stream is resized last, and a failure there
// leaves the other two at the new budget. On any error MaxBytes keeps
// reporting the previous budget, so a later call with the new budget
// reapplies all three.
//
// The JetStream calls are bounded by resizeTimeout, plus rollbackTimeout for
// the undo — or another resizeTimeout for the consumers joining a queue they
// do not hold yet — all rooted in ctx. That is deliberate: ctx is the
// process's stop context, so a reload caught mid-hook by a stop gives up —
// undo included — rather than holding the drain past
// server.shutdown_timeout. A cancellation between the two updates is
// therefore the one way to leave the pair split, and only for the rest of a
// process that is exiting: the next boot applies the adopted settings to it
// again.
func (e *EmbeddedNATS) SetMaxBytes(ctx context.Context, id tenant.ID, maxBytes int64) error {
	if _, err := tenant.Parse(string(id)); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queue(id)
	q.asked = maxBytes
	if q.ingest && q.history && q.dlq && maxBytes == q.maxBytes && e.joined(id) {
		return nil
	}
	return e.apply(ctx, id, q, maxBytes)
}

// SetReplayWindow applies tenant id's replay window (its hot-reloadable
// stream.gap_window_minutes) to its history stream, as the stream's max_age:
// a written row is kept, and can be replayed, for that long from its write.
// Shortening it expires what is older at once; lengthening it brings back
// nothing already gone. A window of 0 keeps none: nothing is recorded for the
// tenant from then on. A tenant with no queue yet has the window recorded,
// and its history stream opened with it (SetMaxBytes). On an error the
// previous window stays in force, and the next call retries.
func (e *EmbeddedNATS) SetReplayWindow(ctx context.Context, id tenant.ID, window time.Duration) error {
	if _, err := tenant.Parse(string(id)); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	if window < 0 {
		return fmt.Errorf("replay window %s is negative", window)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queue(id)
	if q.window == window {
		return nil
	}
	if q.history {
		resizeCtx, cancel := context.WithTimeout(ctx, resizeTimeout)
		defer cancel()
		if _, err := e.js.UpdateStream(resizeCtx, historyStreamConfig(id, q.historyCap, window)); err != nil {
			return fmt.Errorf("history stream: %w", err)
		}
	}
	q.window = window
	e.windows.Store(id, window)
	return nil
}

// apply brings tenant id's queue to maxBytes: opening it when its ingest
// stream is missing, resizing it otherwise, and joining to it the consumers
// that do not hold it; only then is the budget applied in full (see
// SetMaxBytes). Under e.mu.
func (e *EmbeddedNATS) apply(ctx context.Context, id tenant.ID, q *tenantQueue, maxBytes int64) error {
	defer e.note(id, q)
	resizeCtx, cancel := context.WithTimeout(ctx, resizeTimeout)
	defer cancel()
	if !q.ingest {
		if err := e.applyDLQ(resizeCtx, id, q, maxBytes); err != nil {
			return err
		}
		if err := e.applyHistory(resizeCtx, id, q, maxBytes); err != nil {
			return err
		}
		if _, err := e.js.CreateOrUpdateStream(resizeCtx, ingestStreamConfig(id, maxBytes)); err != nil {
			return fmt.Errorf("open ingest stream: %w", err)
		}
		q.ingest, q.ingestCap = true, maxBytes
		// Every consumer joins a stream just opened: a durable one held on
		// the stream before it went missing went with it, and so did what
		// its delivery read from.
		for _, f := range e.consumers {
			f.leave(id)
		}
		return e.applied(ctx, id, q, maxBytes)
	}
	prevCap := q.ingestCap
	if _, err := e.js.UpdateStream(resizeCtx, ingestStreamConfig(id, maxBytes)); err != nil {
		return fmt.Errorf("resize ingest stream: %w", err)
	}
	q.ingestCap = maxBytes
	if err := e.applyDLQ(resizeCtx, id, q, maxBytes); err != nil {
		// The undo runs on its own budget, not the one the dead-letter call
		// has likely just exhausted.
		rollbackCtx, cancelRollback := context.WithTimeout(ctx, rollbackTimeout)
		defer cancelRollback()
		if _, rollbackErr := e.js.UpdateStream(rollbackCtx, ingestStreamConfig(id, prevCap)); rollbackErr != nil {
			return fmt.Errorf("%w (ingest stream rollback failed, so it stays at the new limit and the dlq at the previous: %w)", err, rollbackErr)
		}
		q.ingestCap = prevCap
		return fmt.Errorf("%w (ingest stream restored to the previous limit)", err)
	}
	if err := e.applyHistory(resizeCtx, id, q, maxBytes); err != nil {
		return err
	}
	return e.applied(ctx, id, q, maxBytes)
}

// applyHistory gives tenant id's history stream a tenth of maxBytes, at the
// tenant's replay window, creating it when it is missing. Under e.mu.
func (e *EmbeddedNATS) applyHistory(ctx context.Context, id tenant.ID, q *tenantQueue, maxBytes int64) error {
	limit := maxBytes / historyShare
	verb := "resize"
	if !q.history {
		verb = "open"
	}
	if _, err := e.js.CreateOrUpdateStream(ctx, historyStreamConfig(id, limit, q.window)); err != nil {
		return fmt.Errorf("%s history stream: %w", verb, err)
	}
	q.history, q.historyCap = true, limit
	e.windows.Store(id, q.window)
	return nil
}

// applied ends an apply that left tenant id's streams at maxBytes:
// the consumers that do not hold the queue join it, and only then is the
// budget applied in full. Under e.mu.
func (e *EmbeddedNATS) applied(ctx context.Context, id tenant.ID, q *tenantQueue, maxBytes int64) error {
	if err := e.joinConsumers(ctx, id); err != nil {
		return err
	}
	q.maxBytes = maxBytes
	return nil
}

// joined reports whether every registered consumer holds tenant id's queue.
// Under e.mu (or before e is shared).
func (e *EmbeddedNATS) joined(id tenant.ID) bool {
	return !slices.ContainsFunc(e.consumers, func(f *fanIn) bool { return !f.holds(id) })
}

// joinConsumers joins to tenant id's queue every registered consumer that
// does not hold it. One that cannot is named in the error and costs the
// others nothing: each is tried. Under e.mu.
func (e *EmbeddedNATS) joinConsumers(ctx context.Context, id tenant.ID) error {
	// The joins run on a budget of their own: a queue a consumer could not
	// join takes none of its tenant's publishes, so a slow open must not
	// leave the consumers no time.
	ctx, cancel := context.WithTimeout(ctx, resizeTimeout)
	defer cancel()
	var errs []error
	for _, f := range e.consumers {
		if f.holds(id) {
			continue
		}
		if err := f.join(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("consumer %s: join the queue: %w", f.cfg.Durable, err))
		}
	}
	return errors.Join(errs...)
}

// applyDLQ gives tenant id's dead-letter stream a tenth of maxBytes, creating
// it when it is missing, but never caps it below the bytes it holds: those
// stay, the cap is what they take, and the stream then drops its oldest row
// to make room for each new one, as any full dead-letter stream does. Under
// e.mu.
func (e *EmbeddedNATS) applyDLQ(ctx context.Context, id tenant.ID, q *tenantQueue, maxBytes int64) error {
	limit := maxBytes / dlqShare
	verb := "resize"
	s, err := e.js.Stream(ctx, dlqStreamName(id))
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		verb = "open"
	case err != nil:
		return fmt.Errorf("dlq stream info: %w", err)
	default:
		// A stream's size fits an int64 as its cap does; the bound is
		// checked rather than assumed.
		if held := s.CachedInfo().State.Bytes; held <= math.MaxInt64 && int64(held) > limit {
			slog.Warn("mq: dead-letter queue kept at what it holds rather than shrunk to its budget, so no parked row is deleted",
				"component", "nats", "tenant", id, "held_bytes", held, "budget_bytes", limit)
			limit = int64(held)
		}
	}
	if _, err := e.js.CreateOrUpdateStream(ctx, dlqStreamConfig(id, limit)); err != nil {
		return fmt.Errorf("%s dlq stream: %w", verb, err)
	}
	q.dlq = true
	return nil
}

// reopen opens tenant id's queue at the budget last asked for it, for a
// publish that finds the queue not recorded open — a consumer that could not
// join it is tried again — or a publish or park that found one of its streams
// missing. errNoQueue when no budget has been asked for the tenant yet: a
// reload can make a tenant resolvable an instant before its budget arrives.
//
// It runs detached from ctx's cancellation, bounded by its own timeouts:
// ctx is one caller's — an ingest request — while the queue is every
// consumer's, and a client that goes away between the open and the joins
// would leave a queue no consumer holds, refusing the tenant's publishes
// until the next one joins them.
func (e *EmbeddedNATS) reopen(ctx context.Context, id tenant.ID) error {
	ctx = context.WithoutCancel(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queues[id]
	if q == nil || q.asked == 0 {
		return fmt.Errorf("tenant %s: %w", id, errNoQueue)
	}
	defer e.note(id, q)
	// What is missing is asked of JetStream rather than read off the flags,
	// which may still say the stream the publish just missed exists — or it
	// may be back already, opened by a caller that held mu first.
	for name, there := range map[string]*bool{ingestStreamName(id): &q.ingest, historyStreamName(id): &q.history, dlqStreamName(id): &q.dlq} {
		_, err := e.js.Stream(ctx, name)
		switch {
		case errors.Is(err, jetstream.ErrStreamNotFound):
			*there = false
		case err != nil:
			return fmt.Errorf("stream info: %w", err)
		}
	}
	if q.ingest && q.history && q.dlq && e.joined(id) {
		return nil
	}
	return e.apply(ctx, id, q, q.asked)
}

// Publish stores data on topic's ingest subject, in its tenant's queue. A
// topic without a valid tenant is refused before anything is sent (see
// subject). A tenant with no queue has one opened at the budget last asked
// for it (see SetMaxBytes) — and so does one whose stream exists but whose
// queue the broker has not recorded open, since no consumer may hold that
// stream (see reopenPaced for how often a publish tries). A queue that
// cannot be opened — none asked for yet, JetStream refused it, or a consumer
// could not join it — and a queue at its byte budget (DiscardNew) are
// reported as ErrQueueFull: either way the tenant's queue takes nothing now,
// and a retry is the caller's answer.
func (e *EmbeddedNATS) Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) (err error) {
	subj, err := subject(ingestPrefix, topic)
	if err != nil {
		return err
	}
	spanCtx, end := beginPublish(ctx, publishKindIngest)
	attempts := 0
	defer func() { end(attempts, err) }()
	if _, ok := e.opened.Load(topic.Tenant); !ok {
		if openErr := e.reopenPaced(spanCtx, topic.Tenant); openErr != nil {
			return fmt.Errorf("%w: %w", ErrQueueFull, openErr)
		}
	}
	attempts++
	err = e.publish(spanCtx, subj, data, opts)
	if errors.Is(err, jetstream.ErrNoStreamResponse) {
		if openErr := e.reopenPaced(spanCtx, topic.Tenant); openErr != nil {
			return fmt.Errorf("%w: %w", ErrQueueFull, openErr)
		}
		attempts++
		err = e.publish(spanCtx, subj, data, opts)
	}
	if err != nil && strings.Contains(err.Error(), "maximum bytes exceeded") {
		// The server reports a full store as a generic store failure whose
		// text is the only thing that names the cause.
		return fmt.Errorf("%w: %w", ErrQueueFull, err)
	}
	return err
}

// reopenPaced opens tenant id's queue for a publish or park that found it not
// open (reopen). The callers that find it so at the same time share one
// attempt, and after an attempt fails the tenant's publishes and parks get
// its error at once, without taking mu, until reopenRetry has passed: under
// clients retrying, or the worker parking row after row, a queue that cannot
// open would otherwise hold mu for attempt after attempt, and every other
// tenant's open, resize and reload waits on mu. A reload that applies the
// tenant's budget retries it regardless (SetMaxBytes).
func (e *EmbeddedNATS) reopenPaced(ctx context.Context, id tenant.ID) error {
	if v, ok := e.failedOpen.Load(id); ok {
		if f := v.(openFailure); time.Now().Before(f.until) {
			return f.err
		}
	}
	_, err, _ := e.reopening.Do(string(id), func() (any, error) {
		err := e.reopen(ctx, id)
		if err != nil {
			e.failedOpen.Store(id, openFailure{until: time.Now().Add(reopenRetry), err: err})
		}
		return nil, err
	})
	return err
}

// DeadLetter stores msg's data on its topic's dead-letter subject, in its
// tenant's queue — the subject it arrived on with the ingest prefix swapped
// for the dead-letter one, nothing decoded or re-encoded. The dead-letter
// stream is DiscardOld, so a full one drops its oldest parked rows rather than
// refusing. A dead-letter stream found missing is opened again with its
// tenant's queue, paced as Publish's is (reopenPaced); a park refused leaves
// its row unacked, to be redelivered.
func (e *EmbeddedNATS) DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) (err error) {
	spanCtx, end := beginPublish(ctx, publishKindDLQ)
	attempts := 1
	defer func() { end(attempts, err) }()
	subj := dlqPrefix + msg.topicKey
	err = e.publish(spanCtx, subj, msg.Data, opts)
	if errors.Is(err, jetstream.ErrNoStreamResponse) {
		if id, ok := keyTenant(msg.topicKey); ok {
			if err = e.reopenPaced(spanCtx, id); err == nil {
				attempts++
				err = e.publish(spanCtx, subj, msg.Data, opts)
			}
		}
	}
	return err
}

func (e *EmbeddedNATS) publish(ctx context.Context, subj string, data []byte, opts []PublishOpt) error {
	msg := nats.NewMsg(subj)
	msg.Data = data

	// Apply any optional configurations (like headers) to the message
	headers := Headers{}
	for _, opt := range opts {
		opt(headers)
	}

	observability.InjectHeaders(ctx, headers)
	msg.Header = nats.Header(headers)
	// No retry on "no responders": in-process, that only ever means no
	// stream holds the subject — a tenant with no queue, which the callers
	// open rather than wait out.
	_, err := e.js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
	return err
}

// wrapMsg adapts a message the ingest worker's durable delivered to the
// mq-owned Message, accepted when its tenant's ingest stream stored it. ctx
// becomes Message.Ctx as given.
func wrapMsg(ctx context.Context, m jetstream.Msg) *Message {
	msg := newMessage(ctx, topicKey(ingestPrefix, m.Subject()), m.Data(), time.Now(),
		func(ctx context.Context) error {
			return m.DoubleAck(ctx)
		},
		func() error {
			return m.Ack()
		},
		func() error {
			return m.Nak()
		},
		WithNakDelay(m.NakWithDelay),
	)
	if meta, err := m.Metadata(); err == nil {
		msg.accepted = meta.Timestamp
	}
	return msg
}

// Subscribe delivers every row a tenant's ingest stream stores from now on to
// handler, one at a time, with the trace context its headers carry, until ctx
// is done: a plain subscription to the subjects the ingest streams republish
// to, every tenant's, those whose queues open later included. The server is
// in this process, so the subscription loses no connection; a handler slow
// enough that the client drops rows ahead of it is the one way a row is
// skipped, and lost is called before the next one is delivered (liveFeed).
func (e *EmbeddedNATS) Subscribe(ctx context.Context, handler func(msg *Message), lost func()) error {
	feed := newLiveFeed(func(m *nats.Msg) {
		handler(newMessage(observability.ExtractHeaders(ctx, Headers(m.Header)), topicKey(livePrefix, m.Subject), m.Data, time.Now(), nil, nil, nil))
	}, lost, func() bool { return true })
	if _, _, err := subscribeLive(ctx, e.conn, livePrefix+">", feed); err != nil {
		return fmt.Errorf("live subscription: %w", err)
	}
	return nil
}

// CreateConsumer creates or updates a durable explicit-ack pull consumer on
// every tenant's queue, and joins each queue opened later — one it cannot
// join is that tenant's failure (SetMaxBytes), not the consumer's. ctx
// becomes every delivered Message.Ctx (see ConsumerManager); it does not
// stop delivery — Consumer.Consume's stop does. It has no units: a config
// naming any is ErrUnitsUnsupported.
func (e *EmbeddedNATS) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	if cfg.Units != nil {
		return nil, fmt.Errorf("create consumer: %w: the embedded queue is one per tenant", ErrUnitsUnsupported)
	}
	c := &workerConsumer{
		fanIn: e.newFanIn(ctx, jetstream.ConsumerConfig{
			Durable:       cfg.Durable,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       cfg.AckWait,
			MaxAckPending: cfg.MaxAckPending,
		}),
		failed: make(chan error, 1),
	}
	c.fail = func(err error) {
		// Exactly one error, and nothing once stop has been called: a durable
		// deleted on several tenants' queues ends each delivery, and a caller
		// that already drained the first must not see the next.
		if c.stopped.Load() || !c.reported.CompareAndSwap(false, true) {
			return
		}
		c.failed <- err
	}
	if err := e.register(ctx, c.fanIn); err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	return c, nil
}

// newFanIn is a fanIn over cfg, not yet holding any durable; the caller sets
// its fail and registers it.
func (e *EmbeddedNATS) newFanIn(ctx context.Context, cfg jetstream.ConsumerConfig) *fanIn {
	return &fanIn{
		e:       e,
		ctx:     ctx,
		cfg:     cfg,
		handles: map[tenant.ID]jetstream.Consumer{},
		running: map[tenant.ID]jetstream.ConsumeContext{},
	}
}

// register holds f's durable on every tenant's queue there is and registers
// f, so every queue opened from here on is joined too.
func (e *EmbeddedNATS) register(ctx context.Context, f *fanIn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, id := range e.ingestTenants() {
		if err := f.join(ctx, id); err != nil {
			return fmt.Errorf("tenant %s: %w", id, err)
		}
	}
	e.consumers = append(e.consumers, f)
	return nil
}

// unregister stops joining f to the queues that open from here on. Under
// e.mu.
func (e *EmbeddedNATS) unregister(f *fanIn) {
	e.consumers = slices.DeleteFunc(e.consumers, func(c *fanIn) bool { return c == f })
}

// fanIn is one durable consumer held on every tenant's ingest stream — the
// ingest worker's (CreateConsumer), the one consumer a work queue takes —
// delivering them all into one handler: each tenant's messages on a
// goroutine of their own, so a tenant's arrive in order and different
// tenants' concurrently, and a handler blocked on one tenant holds back that
// tenant alone. Its fields are guarded by e.mu, bar stopped.
type fanIn struct {
	e   *EmbeddedNATS
	ctx context.Context // each delivered Message.Ctx
	cfg jetstream.ConsumerConfig

	// fail reports a tenant's delivery that ended on its own.
	fail func(error)

	// handles is the durable on each tenant's ingest stream; running, the
	// delivery started on each once deliver is set.
	handles map[tenant.ID]jetstream.Consumer
	running map[tenant.ID]jetstream.ConsumeContext
	deliver func(jetstream.Msg)
	// prefetch is the fetch-ahead asked for across the tenants together; 0
	// leaves each tenant the client default.
	prefetch int
	stopped  atomic.Bool
}

// join holds f's durable on tenant id's ingest stream — looked up first, and
// created or updated only when missing or configured otherwise, so a boot
// over thousands of queues writes nothing it need not — and starts delivery
// on it when f is delivering. Under e.mu.
func (f *fanIn) join(ctx context.Context, id tenant.ID) error {
	stream := ingestStreamName(id)
	c, err := f.e.js.Consumer(ctx, stream, f.cfg.Durable)
	if err != nil || !sameConsumer(c.CachedInfo().Config, f.cfg) {
		if c, err = f.e.js.CreateOrUpdateConsumer(ctx, stream, f.cfg); err != nil {
			return err
		}
	}
	f.handles[id] = c
	if f.deliver == nil || f.stopped.Load() {
		return nil
	}
	return f.run(id)
}

// holds reports whether f holds its durable on tenant id's ingest stream,
// delivering from it when f is delivering. Under e.mu.
func (f *fanIn) holds(id tenant.ID) bool {
	if _, ok := f.handles[id]; !ok {
		return false
	}
	if f.deliver == nil || f.stopped.Load() {
		return true
	}
	_, ok := f.running[id]
	return ok
}

// leave forgets tenant id's queue, whose ingest stream is gone: the durable
// went with it, and the delivery from it is stopped — the broker's doing, so
// not one that ended on its own (run). Under e.mu.
func (f *fanIn) leave(id tenant.ID) {
	delete(f.handles, id)
	if cctx, ok := f.running[id]; ok {
		delete(f.running, id)
		cctx.Stop()
	}
}

// delivering reports whether cctx is still the delivery from tenant id's
// queue, rather than one leave stopped.
func (f *fanIn) delivering(id tenant.ID, cctx jetstream.ConsumeContext) bool {
	f.e.mu.Lock()
	defer f.e.mu.Unlock()
	return f.running[id] == cctx
}

// sameConsumer reports whether a durable holds the fields this package sets;
// a zero field in want is the server's default, whatever that resolved to.
func sameConsumer(have, want jetstream.ConsumerConfig) bool {
	return have.AckPolicy == want.AckPolicy &&
		have.FilterSubject == want.FilterSubject &&
		(want.AckWait == 0 || have.AckWait == want.AckWait) &&
		(want.MaxAckPending == 0 || have.MaxAckPending == want.MaxAckPending)
}

// share is one tenant's part of the fetch-ahead: the total spread over the
// tenants' queues joined so far, at least one each, fixed when that queue's
// delivery starts. 0 leaves the client default. Under e.mu.
func (f *fanIn) share() int {
	if f.prefetch <= 0 {
		return 0
	}
	return max(1, f.prefetch/max(1, len(f.handles)))
}

// run starts delivery from tenant id's durable, once. Under e.mu.
func (f *fanIn) run(id tenant.ID) error {
	if _, ok := f.running[id]; ok {
		return nil
	}
	// The client reports what goes wrong after Consume returns only through
	// this handler, never through Consume's own error. It calls it for
	// passing conditions too (a missed heartbeat, a leadership change) and
	// keeps delivering; on a terminal one (the consumer was deleted, a bad
	// request, a closed connection) it calls it and then stops the
	// subscription itself. So the handler only records and logs, and "the
	// subscription closed without our stop" below is what terminal means —
	// the client's own verdict, not a list of error names kept in step here.
	var lastErr atomic.Pointer[error]
	opts := []jetstream.PullConsumeOpt{
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			lastErr.Store(&err)
			slog.Warn("mq: consumer reported an error", "component", "nats", "tenant", id, "error", err)
		}),
	}
	if n := f.share(); n > 0 {
		opts = append(opts, jetstream.PullMaxMessages(n))
	}
	// JetStream calls deliver on a goroutine of its own, so it is guarded
	// here like any goroutine WaveHouse starts (#902).
	deliver := f.deliver
	cctx, err := f.handles[id].Consume(func(m jetstream.Msg) {
		defer observability.Crash("mq_delivery", slog.String("tenant", id.String()))
		deliver(m)
	}, opts...)
	if err != nil {
		return err
	}
	f.running[id] = cctx
	go func() {
		defer observability.Crash("mq_embedded")
		<-cctx.Closed()
		if f.stopped.Load() || !f.delivering(id, cctx) {
			return
		}
		reason := ErrDeliveryEnded
		if r := lastErr.Load(); r != nil {
			reason = fmt.Errorf("%w: %w", ErrDeliveryEnded, *r)
		}
		f.fail(fmt.Errorf("tenant %s: %w", id, reason))
	}()
	return nil
}

// start begins delivery to deliver from every tenant's durable, and from
// each queue joined later, fetching about prefetch messages ahead across the
// tenants together (see share); a delivery that ends on its own is reported
// through fail. The returned stop ends every delivery and stops joining new
// queues, without waiting.
func (f *fanIn) start(deliver func(jetstream.Msg), prefetch int) (stop func(), err error) {
	f.e.mu.Lock()
	defer f.e.mu.Unlock()
	f.deliver, f.prefetch = deliver, prefetch
	stop = func() {
		f.e.mu.Lock()
		defer f.e.mu.Unlock()
		f.stopped.Store(true)
		for _, cctx := range f.running {
			cctx.Stop()
		}
		f.e.unregister(f)
	}
	for _, id := range slices.Sorted(maps.Keys(f.handles)) {
		if err := f.run(id); err != nil {
			f.stopped.Store(true)
			for _, cctx := range f.running {
				cctx.Stop()
			}
			f.e.unregister(f)
			return nil, fmt.Errorf("tenant %s: %w", id, err)
		}
	}
	return stop, nil
}

// workerConsumer is the Consumer CreateConsumer returns: a fanIn with the
// failed channel its contract promises.
type workerConsumer struct {
	*fanIn
	failed   chan error
	reported atomic.Bool
}

func (c *workerConsumer) Consume(handler func(msg *Message), prefetch int) (func(), <-chan error, error) {
	stop, err := c.start(func(m jetstream.Msg) {
		handler(wrapMsg(observability.ExtractHeaders(c.ctx, m.Headers()), m))
	}, prefetch)
	if err != nil {
		return nil, nil, fmt.Errorf("consume: %w", err)
	}
	return stop, c.failed, nil
}

// DeadLetterCounts reads tenant id's dead-letter stream's per-subject counts
// and keys them by table (deadLetterTables). The table filter matches every
// scope of that table, so it is applied to the parsed topic rather than as a
// subject filter.
func (e *EmbeddedNATS) DeadLetterCounts(ctx context.Context, id tenant.ID, table string) (DeadLetterCounts, error) {
	if _, err := tenant.Parse(string(id)); err != nil {
		return DeadLetterCounts{}, fmt.Errorf("tenant: %w", err)
	}
	s, err := e.js.Stream(ctx, dlqStreamName(id))
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return DeadLetterCounts{}, fmt.Errorf("%w: %w", ErrNoDeadLetterQueue, err)
		}
		return DeadLetterCounts{}, fmt.Errorf("get dlq stream: %w", err)
	}

	info, err := s.Info(ctx, jetstream.WithSubjectFilter(tenantSubjects(dlqPrefix, id)))
	if err != nil {
		return DeadLetterCounts{}, fmt.Errorf("dlq stream info: %w", err)
	}

	return DeadLetterCounts{Tables: deadLetterTables(info.State.Subjects, dlqPrefix, table), Total: info.State.Msgs}, nil
}

// ReplaySince sends topic's events accepted at or after since: the written
// ones from its tenant's history stream, for as long as the tenant's replay
// window keeps them, then the ones still waiting on its ingest stream, then
// the ones written while those were read (replaySource). The history is read
// through ack-less consumers of its own that expire once idle, the ingest
// stream by direct get. A read that fails before the replay catches up is an
// error — a tenant with no queue has nothing to read from — and a done ctx
// returns ctx's error. A topic without a valid tenant is refused like a
// publish (see subject).
func (e *EmbeddedNATS) ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error {
	subj, err := subject(ingestPrefix, topic)
	if err != nil {
		return err
	}
	src := replaySource{
		nc: e.conn, js: e.js, apiPrefix: jetstream.DefaultAPIPrefix,
		history: historyStreamName(topic.Tenant), historySubject: histPrefix + topic.key(),
		queues:   []replayQueue{{stream: ingestStreamName(topic.Tenant), subject: subj}},
		consumer: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy, InactiveThreshold: replayInactiveThreshold},
	}
	return src.replay(ctx, since, send)
}

// Record stores each written row on its tenant's history stream, under its
// topic and with the time the ingest stream accepted it, in order, within
// recordTimeout for the batch. A row of a tenant whose replay window is 0, or
// was never set, is not recorded: the tenant keeps no history. A history
// stream that is missing costs its rows their replay, and the error says how
// many.
func (e *EmbeddedNATS) Record(ctx context.Context, msgs []*Message) error {
	return e.hist.record(ctx, msgs, func(m *Message) (string, bool) {
		id, ok := keyTenant(m.topicKey)
		if !ok {
			return "", false
		}
		if window, _ := e.windows.Load(id); window == nil || window.(time.Duration) <= 0 {
			return "", false
		}
		return histPrefix + m.topicKey, true
	})
}

// Stats reports the embedded server's client connections and its cumulative
// message counts, across every client connected to it.
func (e *EmbeddedNATS) Stats() (observability.MQStats, error) {
	varz, err := e.server.Varz(nil)
	if err != nil {
		return observability.MQStats{}, err
	}
	return observability.MQStats{
		Embedded:    true,
		Connections: int64(varz.Connections),
		InMsgs:      varz.InMsgs,
		OutMsgs:     varz.OutMsgs,
	}, nil
}

func (e *EmbeddedNATS) Close() error {
	e.conn.Close()
	e.server.Shutdown()
	// Owning the lifecycle (NoSigs, #287) means waiting it out: without this,
	// run()'s remaining defers unwind while JetStream is still tearing down
	// and the process can exit mid-shutdown (as-if-crashed stream state).
	// Milliseconds for an in-process server. It does not join a durable's
	// state flusher: a write under way can land after Close returns, or never
	// if the process exits first, leaving the durable's previous ack state on
	// disk (#665).
	e.server.WaitForShutdown()
	return nil
}
