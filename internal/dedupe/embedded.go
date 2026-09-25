package dedupe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Embedded is the embedded implementation: every tenant's seen ids in one
// Pebble instance at data_dir/pebble, each key led by its tenant (#583 story
// 3) and then its table (#222), with the pending claims in memory beside it
// (pendingSet). One instance means a thousand tenants cost one instance's
// goroutines, open files and heap rather than a thousand. The instance opens
// with the first tenant's store switched on and closes with the last one
// switched off: it is open exactly while some tenant has dedupe on, and a
// tenant switched off, rejected or removed keeps its seen ids for when it is
// back. Pebble is one process's, so two pods on it do not share seen ids.
type Embedded struct {
	dir string

	mu   sync.Mutex // guards db and open
	db   *pebble.DB
	open int // tenant stores open over db

	pending *pendingSet
	tokens  atomic.Uint64
	now     func() time.Time
	// readHook, when set, runs before each Pebble read in Reserve; a test
	// makes it fail to exercise Reserve's all-or-nothing error path.
	readHook func() error
}

// NewEmbedded returns the embedded implementation under dataDir. Nothing is
// opened until a tenant's store is.
func NewEmbedded(dataDir string) *Embedded {
	return &Embedded{dir: filepath.Join(dataDir, "pebble"), pending: newPendingSet(), now: time.Now}
}

// Dir is where the instance lives.
func (e *Embedded) Dir() string { return e.dir }

// Open reports whether the instance is open: whether some tenant's store is.
func (e *Embedded) Open() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.db != nil
}

// Tenant builds tenant id's store, closed, over its share of the instance —
// the Factory Stores takes.
func (e *Embedded) Tenant(id tenant.ID) *Managed {
	prefix := KeyPrefix(id)
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

// Committed values are committedMark ‖ expiry (big-endian UnixNano, 0 =
// never). Values written before #222 were a bare 8-byte timestamp, so one
// under a key that happens to equal a current one reads as absent.
const (
	committedMark = 2
	valueLen      = 9
)

// Reserve claims each key under its shard's lock: the pending check, the
// Pebble read and the claim happen with no other Reserve for that key in
// between, and Pebble's directory lock keeps a second process off the
// instance, so at most one caller holds a key (#390).
func (s *tenantStore) Reserve(_ context.Context, keys []Key, lease time.Duration) ([]Claim, error) {
	now := s.e.now()
	claims := make([]Claim, 0, len(keys))
	for _, k := range keys {
		c, err := s.reserve(AppendKey(nil, s.prefix, k), k, now, lease)
		if err != nil {
			s.release(claims)
			return nil, err
		}
		claims = append(claims, c)
	}
	return claims, nil
}

func (s *tenantStore) reserve(key []byte, k Key, now time.Time, lease time.Duration) (Claim, error) {
	sh := s.e.pending.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.sweep(now)
	if p, ok := sh.m[string(key)]; ok && now.Before(p.expires) {
		return Claim{Key: k, Status: InFlight}, nil
	}
	if s.e.readHook != nil {
		if err := s.e.readHook(); err != nil {
			return Claim{}, err
		}
	}
	val, closer, err := s.db.Get(key)
	switch {
	case err == nil:
		live := committedLive(val, now)
		_ = closer.Close()
		if live {
			return Claim{Key: k, Status: Duplicate}, nil
		}
	case !errors.Is(err, pebble.ErrNotFound):
		return Claim{}, fmt.Errorf("dedupe read: %w", err)
	}
	token := strconv.FormatUint(s.e.tokens.Add(1), 36)
	sh.m[string(key)] = pending{token: token, expires: now.Add(lease)}
	return Claim{Key: k, Status: Claimed, Token: token}, nil
}

// committedLive reports whether a stored value is a commit that has not
// expired.
func committedLive(val []byte, now time.Time) bool {
	if len(val) != valueLen || val[0] != committedMark {
		return false
	}
	exp := int64(binary.BigEndian.Uint64(val[1:])) //nolint:gosec // written from an int64 below
	return exp == 0 || now.UnixNano() < exp
}

// Commit writes every claim in one batch and one fsync, then drops the
// pending entries it still owns — in that order, so no Reserve in between
// finds the key neither pending nor committed.
func (s *tenantStore) Commit(_ context.Context, claims []Claim, retention time.Duration) error {
	var exp int64
	if retention > 0 {
		exp = s.e.now().Add(retention).UnixNano()
	}
	val := make([]byte, valueLen)
	val[0] = committedMark
	binary.BigEndian.PutUint64(val[1:], uint64(exp))
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	for _, c := range claims {
		if err := b.Set(AppendKey(nil, s.prefix, c.Key), val, nil); err != nil {
			return fmt.Errorf("dedupe commit: %w", err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("dedupe commit: %w", err)
	}
	s.release(claims)
	return nil
}

// Release drops the pending entries the claims still own.
func (s *tenantStore) Release(_ context.Context, claims []Claim) error {
	s.release(claims)
	return nil
}

func (s *tenantStore) release(claims []Claim) {
	for _, c := range claims {
		if c.Status != Claimed {
			continue
		}
		key := AppendKey(nil, s.prefix, c.Key)
		sh := s.e.pending.shard(key)
		sh.mu.Lock()
		if p, ok := sh.m[string(key)]; ok && p.token == c.Token {
			delete(sh.m, string(key))
		}
		sh.mu.Unlock()
	}
}

// Close releases the store's hold on the instance. Safe to call more than
// once: only the first releases.
func (s *tenantStore) Close() error {
	var err error
	s.closed.Do(func() { err = s.e.release() })
	return err
}

// pendingShards spreads the pending claims over independently locked maps,
// so Reserves for different keys rarely wait on each other.
const pendingShards = 64

type pending struct {
	token   string
	expires time.Time
}

type pendingShard struct {
	mu        sync.Mutex
	m         map[string]pending
	nextSweep time.Time
}

// sweep drops lapsed claims at most once a DefaultLease, so a claim nobody
// commits, releases or re-reserves does not stay in memory. Callers hold mu.
func (sh *pendingShard) sweep(now time.Time) {
	if now.Before(sh.nextSweep) {
		return
	}
	sh.nextSweep = now.Add(DefaultLease)
	for k, p := range sh.m {
		if !now.Before(p.expires) {
			delete(sh.m, k)
		}
	}
}

// pendingSet is every tenant's live claims. It lives in memory because one
// process owns the instance: a crash forgets every claim, which is each
// lease lapsing at once.
type pendingSet [pendingShards]pendingShard

func newPendingSet() *pendingSet {
	p := new(pendingSet)
	for i := range p {
		p[i].m = map[string]pending{}
	}
	return p
}

func (p *pendingSet) shard(key []byte) *pendingShard {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return &p[h.Sum32()%pendingShards]
}
