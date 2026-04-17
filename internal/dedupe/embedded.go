package dedupe

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/cockroachdb/pebble"
)

// EmbeddedDeduplicator uses Pebble for local deduplication.
type EmbeddedDeduplicator struct {
	db *pebble.DB
}

// NewEmbedded opens a Pebble database at the given path.
func NewEmbedded(dir string) (*EmbeddedDeduplicator, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &EmbeddedDeduplicator{db: db}, nil
}

// CheckAndMark returns true if the event was already seen.
func (d *EmbeddedDeduplicator) CheckAndMark(_ context.Context, eventID string) (bool, error) {
	key := []byte(eventID)

	_, closer, err := d.db.Get(key)
	if err == nil {
		_ = closer.Close()
		return true, nil
	}
	if err != pebble.ErrNotFound {
		return false, err
	}

	// Store timestamp as value for future auditing.
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(time.Now().UnixNano()))

	if err := d.db.Set(key, val, pebble.Sync); err != nil {
		return false, err
	}
	return false, nil
}

func (d *EmbeddedDeduplicator) Close() error {
	return d.db.Close()
}

func (d *EmbeddedDeduplicator) Stats() map[string]int64 {
	m := d.db.Metrics()
	return map[string]int64{
		"pebble_wal_size":    int64(m.WAL.Size),
		"pebble_table_count": m.Total().NumFiles,
	}
}
