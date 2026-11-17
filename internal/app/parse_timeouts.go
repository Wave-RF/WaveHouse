package app

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// parseTimeouts is each served tenant's limit on validating one ingest
// request's body, as the API applies it: its own ingest.parse_timeout, or
// boot config's ingest.parse_timeout when it sets none, never above boot
// config's ingest.max_parse_timeout. The two tiers meet here because
// pkg/settings cannot see boot config, so `wavehouse validate` cannot check
// the ceiling. Reconciled from the registry's AfterAdopt hook, with
// ingestLimits' answer to a refusal: a tenant whose value is over the
// ceiling keeps the limit it had, or the default when it had none, logged at
// ERROR and reported through Reconciled (the reload is partial), and the
// next reload retries. At boot a refusal follows the registry's rule for the
// shape: fatal over a flat directory, naming both values; over a nested one
// the tenant is served with the default, logged and counted as on a reload.
type parseTimeouts struct {
	tenants      *settings.Registry
	def, ceiling time.Duration

	mu  sync.Mutex
	cur atomic.Pointer[map[tenant.ID]time.Duration]
}

func newParseTimeouts(tenants *settings.Registry, cfg config.Ingest) *parseTimeouts {
	p := &parseTimeouts{tenants: tenants, def: cfg.ParseTimeout, ceiling: cfg.MaxParseTimeout}
	p.cur.Store(&map[tenant.ID]time.Duration{})
	return p
}

// For is the limit of store's tenant. A tenant with no entry yet — a request
// landing after a reload adopted it and before this hook ran — gets what
// reconcile would give it with nothing to keep.
func (p *parseTimeouts) For(store *settings.Store) time.Duration {
	if d, ok := (*p.cur.Load())[store.Tenant()]; ok {
		return d
	}
	if d, err := p.limit(store); err == nil {
		return d
	}
	return p.def
}

// reconcile adopts every served tenant's limit that fits the ceiling, keeps
// the previous one of a tenant whose new one does not, and forgets the
// tenants no longer served. It returns every refusal, which boot over a flat
// directory refuses on; otherwise each is logged too.
func (p *parseTimeouts) reconcile(boot bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := *p.cur.Load()
	next := make(map[tenant.ID]time.Duration, len(cur))
	var errs []error
	for id, store := range p.tenants.All() {
		d, err := p.limit(store)
		if err == nil {
			next[id] = d
			continue
		}
		errs = append(errs, fmt.Errorf("tenant %s: %w", id, err))
		prev, had := cur[id]
		if !had {
			prev = p.def
		}
		next[id] = prev
		if boot && !p.tenants.Nested() {
			continue // the caller refuses boot on the error, which names both values
		}
		if had {
			slog.Error("ingest.parse_timeout refused; keeping the tenant's previous limit, the next reload retries",
				"tenant", id, "error", err, "limit", prev)
		} else {
			slog.Error("ingest.parse_timeout refused; the tenant gets the boot config's ingest.parse_timeout, the next reload retries",
				"tenant", id, "error", err, "limit", prev)
		}
	}
	p.cur.Store(&next)
	return errors.Join(errs...)
}

// limit is store's own limit, or the default when it sets none; an error
// when its own is over the ceiling.
func (p *parseTimeouts) limit(store *settings.Store) (time.Duration, error) {
	d := store.ParseTimeout()
	switch {
	case d == 0:
		return p.def, nil
	case d > p.ceiling:
		return 0, fmt.Errorf("ingest.parse_timeout %d is over the boot config's ingest.max_parse_timeout %s", int64(d/time.Second), p.ceiling)
	}
	return d, nil
}
