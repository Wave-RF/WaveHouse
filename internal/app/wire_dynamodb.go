// The wiring for dedupe.backend=dynamodb, apart from wire.go for the same
// reason as wire_nats.go.

package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// wireDynamoDedupe builds the dedupe stores over one DynamoDB table that
// every tenant and every process shares (dedupe.Dynamo), so a tenant's store
// opens for free once the table has passed its check. Boot checks it (after
// creating it, with create_table on dynamodb-local) whether or not any tenant
// has dedupe on, and never creates it otherwise. A table that fails the check
// follows the registry's rule for the shape, as Pebble's instance does: a
// flat directory refuses boot; a nested one boots with every switched-on
// store closed, so its ingest fails closed. Unlike a local disk, a remote
// table's failure is usually brief (a throttle, credentials not yet issued
// mid-rollout), and a nested directory has no watcher to reload it, so the
// check is also retried in the background, with backoff, until it passes.
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
	state := errDynamoUnchecked // nil once the table has passed
	check := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if state == nil {
			return nil
		}
		if c.CreateTable {
			if state = d.CreateTable(ctx); state != nil {
				return state
			}
		}
		state = d.Check(ctx)
		return state
	}
	ready := func() error {
		mu.Lock()
		defer mu.Unlock()
		return state
	}
	stores := dedupe.NewStores(dedupe.Factory(d.Tenant).Gated(ready))
	a.dedup = stores
	a.add(component{name: "dedupe", close: withoutContext(stores.Close)})
	var reconciling sync.Mutex // the hook and the retry loop both reconcile
	reconcile := func(ctx context.Context) error {
		reconciling.Lock()
		defer reconciling.Unlock()
		if err := stores.Retain(a.served); err != nil {
			slog.Error("dedupe store close failed", "error", err)
		}
		checkErr := check(ctx)
		if checkErr != nil {
			slog.Error("dedupe: dynamodb table check failed; ingest with dedupe on fails closed until a reload passes it",
				"table", c.Table, "error", checkErr)
		}
		for id, store := range a.tenants.All() {
			m := stores.For(id)
			enabled := store.DedupeEnabled()
			wasOpen := m.Open()
			// The one failure an open has is the check's, logged above.
			_ = m.Apply(enabled)
			if m.Open() != wasOpen {
				slog.Info("dedupe store reconciled with settings", "tenant", id, "enabled", enabled)
			}
		}
		return checkErr
	}
	a.tenants.AfterAdopt(func([]tenant.ID) { _ = reconcile(a.stopCtx) })
	if err := reconcile(ctx); err != nil {
		if !a.tenants.Nested() {
			return fmt.Errorf("dedupe open: %w", err)
		}
		a.add(component{name: "dedupe table check", run: func(ctx context.Context) error {
			for wait := tableCheckRetry; ready() != nil; wait = min(2*wait, 30*time.Second) {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(wait):
				}
				if reconcile(ctx) == nil {
					slog.Info("dedupe: dynamodb table check passed", "table", c.Table)
				}
			}
			return nil
		}})
	}
	return nil
}
