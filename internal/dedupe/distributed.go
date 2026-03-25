package dedupe

import (
	"context"
	"time"

	"github.com/gocql/gocql"
)

// DistributedDeduplicator uses ScyllaDB for distributed deduplication.
// Table schema: dedupe (event_hash text, created_at timestamp,
// PRIMARY KEY (event_hash)) — no default TTL.
type DistributedDeduplicator struct {
	session *gocql.Session
}

// NewDistributed creates a ScyllaDB-backed deduplicator.
func NewDistributed(hosts []string, keyspace string) (*DistributedDeduplicator, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &DistributedDeduplicator{session: session}, nil
}

// CheckAndMark uses INSERT IF NOT EXISTS for atomic check-and-mark.
func (d *DistributedDeduplicator) CheckAndMark(ctx context.Context, eventID string) (bool, error) {
	applied, err := d.session.Query(
		`INSERT INTO dedupe (event_hash, created_at) VALUES (?, ?) IF NOT EXISTS`,
		eventID, time.Now(),
	).WithContext(ctx).MapScanCAS(map[string]interface{}{})
	if err != nil {
		return false, err
	}
	// applied=true → row inserted (not a duplicate)
	// applied=false → row already existed (duplicate)
	return !applied, nil
}

func (d *DistributedDeduplicator) Close() error {
	d.session.Close()
	return nil
}
