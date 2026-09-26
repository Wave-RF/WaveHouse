package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// errDynamoUnchecked is a store's open before the first table check has run.
var errDynamoUnchecked = errors.New("dedupe: dynamodb table not checked yet")

// wireDynamoDedupe builds the dedupe stores over one DynamoDB table that
// every tenant and every process shares (dedupe.Dynamo), so a tenant's store
// opens for free once the table has passed its check. Boot checks it (after
// creating it, with create_table on dynamodb-local) whether or not any tenant
// has dedupe on, and never creates it otherwise. Boot is refused only when the
// table is misconfigured (a failure that is not ErrUnavailable: missing, the
// wrong key schema, access denied) over a flat directory in which a tenant
// has dedupe on. Otherwise — a transient failure, a nested directory, or no
// tenant deduping yet — the process boots with every switched-on store
// closed, so its ingest fails closed, and the check is retried in the
// background, with backoff, until it passes: a remote table's failure is
// often brief, a nested directory has no watcher to reload it, and a fixed
// table is picked up without a restart.
// The check is network I/O, so the AfterAdopt hook never runs it: the hook
// holds the lock that serializes reloads. It applies every store against the
// last check's result and wakes the retry, so a reload still retries at once.
func (a *App) wireDynamoDedupe(ctx context.Context) error {
	c := a.cfg.Dedupe.DynamoDB
	d, err := dedupe.NewDynamo(ctx, dedupe.DynamoConfig{
		Table: c.Table, Region: c.Region, Endpoint: c.Endpoint,
		Timeout: c.Timeout, MaxAttempts: c.MaxAttempts, RetryMode: c.RetryMode,
		ReserveConcurrency: a.cfg.Dedupe.ReserveConcurrency,
	})
	if err != nil {
		return err
	}
	var mu sync.Mutex
	state := errDynamoUnchecked // nil once the table has passed, for good
	ready := func() error {
		mu.Lock()
		defer mu.Unlock()
		return state
	}
	// check is only ever run by boot, then by the retry loop, one at a time.
	check := func(ctx context.Context) error {
		var err error
		if c.CreateTable {
			err = d.CreateTable(ctx)
		}
		if err == nil {
			err = d.Check(ctx)
		}
		mu.Lock()
		defer mu.Unlock()
		if state != nil {
			state = err
		}
		return state
	}
	stores := dedupe.NewStores(dedupe.Factory(d.Tenant).Gated(ready))
	a.dedup = stores
	a.add(component{name: "dedupe", close: withoutContext(stores.Close)})
	var reconciling sync.Mutex // the hook and the retry loop both apply
	apply := func() {
		reconciling.Lock()
		defer reconciling.Unlock()
		if err := stores.Retain(a.served); err != nil {
			slog.Error("dedupe store close failed", "error", err)
		}
		for id, store := range a.tenants.All() {
			m := stores.For(id)
			enabled := store.DedupeEnabled()
			wasOpen := m.Open()
			// The one failure an open has is the check's, logged where it ran.
			_ = m.Apply(enabled)
			if m.Open() != wasOpen {
				slog.Info("dedupe store reconciled with settings", "tenant", id, "enabled", enabled)
			}
		}
	}
	retry := make(chan struct{}, 1)
	a.tenants.AfterAdopt(func([]tenant.ID) {
		apply()
		if ready() != nil {
			select {
			case retry <- struct{}{}:
			default: // a retry is already due
			}
		}
	})
	if err := check(ctx); err != nil {
		misconfigured := !errors.Is(err, dedupe.ErrUnavailable)
		if misconfigured && !a.tenants.Nested() && a.anyDedupeEnabled() {
			return fmt.Errorf("dedupe open: %w", err)
		}
		if misconfigured {
			slog.Error("dedupe: dynamodb table is misconfigured; ingest with dedupe on fails closed until it is fixed",
				"table", c.Table, "error", err)
		} else {
			slog.Error("dedupe: dynamodb table check failed; ingest with dedupe on fails closed while it is retried",
				"table", c.Table, "error", err)
		}
		a.add(component{name: "dedupe table check", run: func(ctx context.Context) error {
			for wait := time.Second; ready() != nil; wait = min(2*wait, 30*time.Second) {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(wait):
				case <-retry:
				}
				if err := check(ctx); err != nil {
					if ctx.Err() == nil {
						slog.Error("dedupe: dynamodb table check failed again; ingest with dedupe on still fails closed",
							"table", c.Table, "error", err)
					}
					continue
				}
				slog.Info("dedupe: dynamodb table check passed", "table", c.Table)
				apply()
			}
			return nil
		}})
	}
	apply()
	return nil
}

// anyDedupeEnabled reports whether a served tenant has dedupe switched on.
func (a *App) anyDedupeEnabled() bool {
	for _, store := range a.tenants.All() {
		if store.DedupeEnabled() {
			return true
		}
	}
	return false
}
