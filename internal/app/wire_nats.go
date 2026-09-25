// The wiring for mq.backend=nats and coord.backend=nats, apart from
// wire.go so the e2e coverage gate, whose stack runs the embedded broker,
// can leave it to the integration suite (.testcoverage.yml).

package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// wireNATSMQ connects to the operator's NATS (mq.backend: nats) and waits,
// up to mq.nats.topology_wait, for the streams and durables it needs; a
// topology still wrong then refuses boot with every finding. The operator
// owns every limit, so a tenant's mq.max_bytes_gb is not handed over
// (config.Warnings says so at boot).
func (a *App) wireNATSMQ(ctx context.Context) error {
	n := a.cfg.MQ.NATS
	broker, err := mq.NewNATS(ctx, mq.NATSConfig{
		URLs:         n.URLs,
		Name:         n.Name,
		CredsFile:    n.CredsFile,
		NKeySeedFile: n.NKeySeedFile,
		User:         n.User,
		PasswordFile: n.PasswordFile,
		TLS: mq.NATSTLS{
			CAFile: n.TLS.CAFile, CertFile: n.TLS.CertFile, KeyFile: n.TLS.KeyFile,
			ServerName: n.TLS.ServerName, HandshakeFirst: n.TLS.HandshakeFirst,
		},
		JSDomain: n.JSDomain,
		// AckWait, MaxAckPending and Prefetch are left to mq's defaults,
		// which are the ingest worker's own.
		Topology: mq.NATSTopology{
			Prefix:         n.SubjectPrefix,
			Partitions:     n.Partitions,
			IngestConsumer: n.IngestConsumer,
			HistoryStream:  n.HistoryStream,
			PublishTimeout: n.PublishTimeout,
			DedupeLease:    a.dedupeLease(),
			// Boot waits for the lease bucket with the rest of the topology.
			CoordBucket: a.coordBucket(),
		},
		ConnectTimeout: n.ConnectTimeout,
		TopologyWait:   n.TopologyWait,
	})
	if err != nil {
		return fmt.Errorf("mq open: %w", err)
	}
	a.adoptMQ(broker)
	if !a.cfg.Has(config.RoleAPI) {
		return nil // dedupe runs on the API path only
	}
	window, err := broker.DuplicateWindow(ctx)
	if err != nil {
		slog.Warn("mq: could not read the partitions' duplicate window; dedupe retention is not checked against it", "error", err)
		return nil
	}
	a.warnShortRetention(window)
	a.tenants.AfterAdopt(func([]tenant.ID) { a.warnShortRetention(window) })
	return nil
}

// dedupeLease is the lease ingest runs with: dedupe.lease, or the default for 0.
func (a *App) dedupeLease() time.Duration {
	if l := a.cfg.Dedupe.Lease; l > 0 {
		return l
	}
	return dedupe.DefaultLease
}

// warnShortRetention logs each served tenant with dedupe on whose finite
// retention, default or per table, is under the operator's duplicate window.
// settings refuses one under the embedded window; a longer operator window
// can't be seen there. Such an id re-sent after it expires but inside the
// window is claimed again, then dropped by the queue while the client hears
// it was accepted.
func (a *App) warnShortRetention(window time.Duration) {
	for id, store := range a.tenants.All() {
		if !store.DedupeEnabled() {
			continue
		}
		for table, r := range store.DedupeRetentions() {
			if r > 0 && r < window {
				slog.Warn("dedupe retention is shorter than the nats partitions' duplicate_window: an id re-sent between the two is dropped by the queue while the client is told it was accepted; use a retention of at least the window, or \"0\"",
					"tenant", id, "table", table, "retention", r, "duplicate_window", window)
			}
		}
	}
}

// coordBucket is the lease bucket under coord.backend=nats, "" otherwise.
func (a *App) coordBucket() string {
	if a.cfg.Coord.Backend != config.CoordNATS {
		return ""
	}
	if b := a.cfg.Coord.NATS.Bucket; b != "" {
		return b
	}
	return mq.DefaultNATSCoordBucket(a.cfg.MQ.NATS.SubjectPrefix)
}
