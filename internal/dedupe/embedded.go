package dedupe

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Embedded is the embedded implementation: every tenant's seen ids in one
// Pebble instance at data_dir/pebble, each key led by its tenant (#583 story
// 3), so a thousand tenants cost one instance's goroutines, open files and
// heap rather than a thousand. The instance opens with the first tenant's
// store switched on and closes with the last one switched off: it is open
// exactly while some tenant has dedupe on, and a tenant switched off,
// rejected or removed keeps its seen ids for when it is back.
type Embedded struct {
	dir string

	mu   sync.Mutex // guards db and open
	db   *pebble.DB
	open int // tenant stores open over db
}

// NewEmbedded returns the embedded implementation under dataDir. Nothing is
// opened until a tenant's store is.
func NewEmbedded(dataDir string) *Embedded {
	return &Embedded{dir: filepath.Join(dataDir, "pebble")}
}

// Dir is where the instance lives.
func (e *Embedded) Dir() string { return e.dir }

// Open reports whether the instance is open: whether some tenant's store is.
func (e *Embedded) Open() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.db != nil
}

// keySeparator ends the tenant at the front of every key. A tenant id has no
// NUL, so the first one in a key is this one, and no two tenants' keys meet.
const keySeparator = 0

// Tenant builds tenant id's store, closed, over its share of the instance —
// the Factory Stores takes.
func (e *Embedded) Tenant(id tenant.ID) *Managed {
	prefix := append([]byte(id), keySeparator)
	return NewManaged(func() (Deduplicator, error) { return e.acquire(prefix) })
}

// acquire opens a tenant's store, and the instance with it when no other
// tenant's store holds it open.
func (e *Embedded) acquire(prefix []byte) (Deduplicator, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.db == nil {
		db, err := pebble.Open(e.dir, &pebble.Options{})
		if err != nil {
			return nil, err
		}
		e.db = db
	}
	e.open++
	return &tenantStore{e: e, db: e.db, prefix: prefix}, nil
}

// release closes a tenant's store, and the instance with it when that was
// the last store open.
func (e *Embedded) release() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.open--
	if e.open > 0 {
		return nil
	}
	err := e.db.Close()
	e.db = nil
	return err
}

// Stats reports the instance's figures for the system gauges — one set,
// however many tenants' stores are open — or nil while it is closed, which
// the metrics scraper skips.
func (e *Embedded) Stats() map[string]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.db == nil {
		return nil
	}
	m := e.db.Metrics()
	walSize := m.WAL.Size
	if walSize > math.MaxInt64 {
		walSize = math.MaxInt64
	}
	return map[string]int64{
		"pebble_wal_size":    int64(walSize),
		"pebble_table_count": m.Total().NumFiles,
	}
}

// tenantStore is one tenant's store: its keys in the shared instance, which
// stays open while the store does.
type tenantStore struct {
	e      *Embedded
	db     *pebble.DB
	prefix []byte
	closed sync.Once
}

// CheckAndMark returns true if the event was already seen.
func (s *tenantStore) CheckAndMark(_ context.Context, eventID string) (bool, error) {
	key := make([]byte, 0, len(s.prefix)+len(eventID))
	key = append(append(key, s.prefix...), eventID...)

	_, closer, err := s.db.Get(key)
	if err == nil {
		_ = closer.Close()
		return true, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return false, err
	}

	// Store timestamp as value for future auditing.
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(time.Now().UnixNano()))

	if err := s.db.Set(key, val, pebble.Sync); err != nil {
		return false, err
	}
	return false, nil
}

// Close releases the store's hold on the instance. Safe to call more than
// once: only the first releases.
func (s *tenantStore) Close() error {
	var err error
	s.closed.Do(func() { err = s.e.release() })
	return err
}
