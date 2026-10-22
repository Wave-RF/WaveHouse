package app

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// ingestLimits is each served tenant's ingest limits as the worker applies
// them — its ingest block and clickhouse.insert_timeout — adopted only when
// they fit boot config's mq.ack_wait: a batch must be flushed and inserted
// before the queue redelivers its rows (max_batch_wait_ms plus
// insert_timeout under ack_wait; what that rule leaves out is at
// checkIngestLimits), or they insert twice. The two tiers meet
// here because pkg/settings cannot see boot config, so `wavehouse validate`
// cannot check the rule. Reconciled from the registry's AfterAdopt hook, as
// the ClickHouse pools are, and with the same answer to a refusal: a tenant
// whose adopted values fail the rule keeps the ones it had, logged at ERROR
// and reported through Reconciled (the reload is partial), and the next
// reload retries; a tenant newly served with none to keep has no limits, and
// the worker hands its rows back to the queue, one ack wait at a time, until
// a reload adopts valid ones — nothing of its is parked, since its ingest
// keeps accepting rows. At boot a refusal follows the registry's rule for
// the shape, as the queue budget and the dedupe stores do: fatal over a flat
// directory, naming both values; over a nested one the tenant is served with
// none, logged and counted as on a reload, and the other tenants boot.
type ingestLimits struct {
	tenants *settings.Registry
	ackWait time.Duration

	mu  sync.Mutex
	cur atomic.Pointer[map[tenant.ID]tenantLimits]
}

// tenantLimits is one served tenant's entry: its limits, or none (ok false)
// when its values were refused and it had none before.
type tenantLimits struct {
	limits ingest.Limits
	ok     bool
}

func newIngestLimits(tenants *settings.Registry, ackWait time.Duration) *ingestLimits {
	l := &ingestLimits{tenants: tenants, ackWait: ackWait}
	l.cur.Store(&map[tenant.ID]tenantLimits{})
	return l
}

// For returns tenant id's limits, or none: the tenant is not served, or
// its values were refused with none to keep. A served tenant always has an
// entry: a lookup that lands after a reload adopted the tenant and before
// this collection's hook ran reconciles first, and only that lookup does.
func (l *ingestLimits) For(id tenant.ID) (ingest.Limits, bool) {
	if e, ok := (*l.cur.Load())[id]; ok {
		return e.limits, e.ok
	}
	if _, served := l.tenants.For(id); !served {
		return ingest.Limits{}, false
	}
	_ = l.reconcile(false)
	if e, ok := (*l.cur.Load())[id]; ok {
		return e.limits, e.ok
	}
	return ingest.Limits{}, false
}

// reconcile adopts every served tenant's limits that fit mq.ack_wait, keeps
// the previous ones of a tenant whose new ones do not, and forgets the
// tenants no longer served. It returns every refusal, which boot over a flat
// directory refuses on; otherwise — a reload, or boot over a nested
// directory, where the other tenants boot — each is logged too.
func (l *ingestLimits) reconcile(boot bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := *l.cur.Load()
	next := make(map[tenant.ID]tenantLimits, len(cur))
	var errs []error
	for id, store := range l.tenants.All() {
		lim := limitsOf(store)
		err := checkIngestLimits(lim, l.ackWait)
		if err == nil {
			next[id] = tenantLimits{limits: lim, ok: true}
			continue
		}
		errs = append(errs, fmt.Errorf("tenant %s: %w", id, err))
		prev, had := cur[id]
		next[id] = prev
		if boot && !l.tenants.Nested() {
			continue // the caller refuses boot on the error, which names the values
		}
		if had && prev.ok {
			slog.Error("ingest limits refused; keeping the previous values, the next reload retries", "tenant", id, "error", err)
		} else {
			slog.Error("ingest limits refused; the tenant has none, so its rows go back to the queue until a reload adopts valid ones", "tenant", id, "error", err)
		}
	}
	l.cur.Store(&next)
	return errors.Join(errs...)
}

// limitsOf is the worker's limits from a tenant's adopted settings.
func limitsOf(store *settings.Store) ingest.Limits {
	i := store.Ingest()
	return ingest.Limits{MaxBatchBytes: i.MaxBatchBytes, MaxBatchWait: i.MaxBatchWait, InsertTimeout: i.InsertTimeout, RetryBackoffMax: i.RetryBackoffMax}
}

// checkIngestLimits is the cross-tier rule: the longest a batch takes in the
// common case — its wait for rows, then its one INSERT — must stay under the
// queue's redelivery timeout, or the queue redelivers the rows of a batch
// that is still inserting, and they insert twice. It bounds that case only:
// a batch queued behind its table's in-flight flush, a flush of several
// column lists, and the row-by-row isolation of a rejected batch each add
// INSERT deadlines the rule does not count, so the seeds keep the sum well
// under the ack wait; a flush-level bound is not built.
func checkIngestLimits(lim ingest.Limits, ackWait time.Duration) error {
	// Compared without adding: each value fits a duration (the validator
	// bounds the file's integers), but two can still sum past one, and a
	// wrapped sum lands anywhere, negative included. So each must be
	// positive on its own — a Limits built outside the validator can hold
	// anything, and a negative insert deadline would leave the INSERT
	// unbounded — and the insert must fit what the wait leaves of the ack
	// wait: what passes lies inside (0, ackWait).
	wait, insert := lim.MaxBatchWait, lim.InsertTimeout
	if wait <= 0 || insert <= 0 || wait >= ackWait || insert >= ackWait-wait {
		return fmt.Errorf("ingest.max_batch_wait_ms %d plus clickhouse.insert_timeout %d is not under mq.ack_wait %s: the queue would redeliver the rows of a batch still inserting, and they would insert twice",
			wait.Milliseconds(), int64(insert/time.Second), ackWait)
	}
	return nil
}
