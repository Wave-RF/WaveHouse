package dedupe

import (
	"context"
	"errors"
	"sync"
)

// ErrDisabled is returned by Managed.CheckAndMark while dedupe is switched
// off. The ingest handler consults the settings snapshot before calling, so
// it only sees this in the window of a reload that flips dedupe.enabled:
// the snapshot and the store transition at different instants, and a record
// caught between them is published un-deduped rather than failed.
var ErrDisabled = errors.New("dedupe is disabled")

// ErrUnavailable is returned by Managed.CheckAndMark when dedupe is switched
// on but the Pebble store failed to open. Ingest fails closed on it — the
// settings asked for dedupe, so publishing un-deduped is not a fallback.
var ErrUnavailable = errors.New("dedupe store is not open")

// Managed is a Deduplicator whose Pebble store follows the hot-reloadable
// dedupe.enabled setting: Apply(true) opens it, Apply(false) closes it, and
// in-flight CheckAndMark calls are serialized against that swap so a reload
// can never close the database under a lookup.
type Managed struct {
	dir string
	mu  sync.RWMutex
	// enabled is the last state passed to Apply; db is nil while disabled
	// and also when an enabling open failed.
	enabled bool
	db      *EmbeddedDeduplicator
}

// NewManaged returns a closed Managed store rooted at dir. Nothing is opened
// until Apply(true).
func NewManaged(dir string) *Managed {
	return &Managed{dir: dir}
}

// Apply reconciles the store with the desired state, idempotently: an
// already-open store stays open, an already-closed one stays closed. A
// failed open leaves the store closed and returns the error — the caller
// decides whether that is fatal (boot) or a logged degradation (reload).
func (m *Managed) Apply(enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = enabled
	switch {
	case enabled && m.db == nil:
		db, err := NewEmbedded(m.dir)
		if err != nil {
			return err
		}
		m.db = db
	case !enabled && m.db != nil:
		err := m.db.Close()
		m.db = nil
		return err
	}
	return nil
}

// Open reports whether the Pebble store is currently open.
func (m *Managed) Open() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db != nil
}

// CheckAndMark delegates to the open store; ErrDisabled while switched off,
// ErrUnavailable while switched on but not open.
func (m *Managed) CheckAndMark(ctx context.Context, eventID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.enabled {
		return false, ErrDisabled
	}
	if m.db == nil {
		return false, ErrUnavailable
	}
	return m.db.CheckAndMark(ctx, eventID)
}

// Stats returns the open store's metrics, or nil while closed (the metrics
// scraper skips a nil map).
func (m *Managed) Stats() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.db == nil {
		return nil
	}
	return m.db.Stats()
}

// Close releases the store if open. Safe to call when already closed.
func (m *Managed) Close() error {
	return m.Apply(false)
}
