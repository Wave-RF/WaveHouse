// Package chconn owns the process's ClickHouse connections: one pool per
// distinct address, database, user, password and tls tuple (ClickHouse
// authenticates per connection, so different credentials never share one),
// shared by the tenants whose settings name it and reconciled after every
// settings reload under the boot config's connection ceiling. Manager
// implements driver.Conn by delegating every call to the connection current
// at that instant, so a consumer resolves its tenant's Manager per call and
// never learns a resize happened; the HTTP-side consumers (ingest INSERTs,
// the raw-SQL proxy) read their tenant's Target per request and take their
// client from an HTTPClients.
package chconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// TLS is the settings directory's clickhouse.tls block. Enabled switches
// the native protocol to TLS; the material applies to whichever hop uses
// TLS — native when Enabled, HTTP when http_scheme is https.
type TLS struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
	ServerName         string
}

// config builds the tls.Config the block describes, reading the files now.
// Nil for the zero block, so the https hop keeps net/http's defaults.
// settings.Validate checked the shape without opening anything; a file it
// cannot read or parse is the one error left, and it names the key.
func (t TLS) config() (*tls.Config, error) {
	if t == (TLS{}) {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // G402: the operator's clickhouse.tls.insecure_skip_verify, warned about at validation
		ServerName:         t.ServerName,
	}
	if t.CAFile != "" {
		pemBytes, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("clickhouse.tls.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("clickhouse.tls.ca_file: no certificates in %s", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("clickhouse.tls.cert_file: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// Identity is the tuple that decides which pool a tenant shares. Comparable
// as it stands — the TLS block is a value of strings and bools — so it is
// the map key as well.
type Identity struct {
	Addr     string
	Database string
	Username string
	Password string
	TLS      TLS
}

// String names the tuple for logs and errors: the address, database and
// user, never the password.
func (id Identity) String() string {
	return id.Addr + " database " + id.Database + " user " + id.Username
}

// Sizes is a pool's size: the driver's MaxOpenConns and MaxIdleConns.
type Sizes struct {
	MaxOpenConns int
	MaxIdleConns int
}

// max is the larger ask in each dimension: what a pool shared by tenants
// asking s and o is sized to (the #597 shortest-keepalive precedent,
// inverted — the pool must hold the largest ask). Every tenant's own
// open >= idle keeps the result's open >= idle.
func (s Sizes) max(o Sizes) Sizes {
	return Sizes{MaxOpenConns: max(s.MaxOpenConns, o.MaxOpenConns), MaxIdleConns: max(s.MaxIdleConns, o.MaxIdleConns)}
}

// Params is what one tenant asks of its connection: the settings
// directory's clickhouse block plus the boot-config password. The Identity
// picks the pool; the rest is the tenant's own — the HTTP-interface wiring
// and headers, the query deadline, and the pool sizes the shared pool is at
// least as large as.
type Params struct {
	Addr         string
	HTTPPort     int
	HTTPScheme   string
	Database     string
	Username     string
	Password     string
	QueryTimeout time.Duration
	TLS          TLS
	// Headers go on every HTTP-interface request; read-only once handed in.
	Headers      map[string]string
	MaxOpenConns int
	MaxIdleConns int
}

// Identity returns the tuple p names.
func (p Params) Identity() Identity {
	return Identity{Addr: p.Addr, Database: p.Database, Username: p.Username, Password: p.Password, TLS: p.TLS}
}

// Sizes returns the pool size p asks for.
func (p Params) Sizes() Sizes {
	return Sizes{MaxOpenConns: p.MaxOpenConns, MaxIdleConns: p.MaxIdleConns}
}

// Target is what the HTTP-interface consumers need per request.
type Target struct {
	// URL is the HTTP base, e.g. http://host:8123.
	URL      string
	Username string
	Password string
	Database string
	// TLS configures an https URL; nil means net/http's defaults. The
	// pointer is the tuple's, so it changes only when the tenant moves to a
	// tuple with another tls block, which is what HTTPClients keys on.
	TLS *tls.Config
	// Headers go on every request ahead of the consumer's own; read-only.
	Headers map[string]string
}

// target derives the HTTP wiring from the native address's host and the
// tuple's TLS config. Addr is host:port by settings.Validate's contract, so
// the split cannot fail.
func (p Params) target(tlsCfg *tls.Config) Target {
	host, _, _ := net.SplitHostPort(p.Addr)
	return Target{
		URL:      fmt.Sprintf("%s://%s", p.HTTPScheme, net.JoinHostPort(host, strconv.Itoa(p.HTTPPort))),
		Username: p.Username,
		Password: p.Password,
		Database: p.Database,
		TLS:      tlsCfg,
		Headers:  p.Headers,
	}
}

// dialer is the connection factory: clickhouse.Open in production, a fake
// in tests. Like clickhouse.Open it does not dial.
type dialer func(Identity, Sizes, *tls.Config) (driver.Conn, error)

type state struct {
	sizes Sizes
	conn  driver.Conn
}

// Manager is a driver.Conn over one tuple's pool, whose backing connection
// is swapped by Resize.
type Manager struct {
	dial   dialer
	id     Identity
	tlsCfg *tls.Config

	mu  sync.Mutex // serializes Resize/Release/Close against each other
	cur atomic.Pointer[state]
}

var _ driver.Conn = (*Manager)(nil)

// Open builds the pool for id at sizes s. Like clickhouse.Open it does not
// dial — boot tolerates an unreachable ClickHouse (schema discovery degrades
// and retries) — so only a malformed option or a certificate file that
// cannot be read or parsed errors here.
func Open(id Identity, s Sizes) (*Manager, error) { return open(dial, id, s) }

func open(d dialer, id Identity, s Sizes) (*Manager, error) {
	tlsCfg, err := id.TLS.config()
	if err != nil {
		return nil, err
	}
	return openWith(d, id, s, tlsCfg)
}

// openWith is open with the tuple's TLS config already built.
func openWith(d dialer, id Identity, s Sizes, tlsCfg *tls.Config) (*Manager, error) {
	conn, err := d(id, s, tlsCfg)
	if err != nil {
		return nil, err
	}
	m := &Manager{dial: d, id: id, tlsCfg: tlsCfg}
	m.cur.Store(&state{sizes: s, conn: conn})
	return m, nil
}

// options is what the driver opens: the native address and credentials,
// the pool sizes, and the TLS config when the native hop is TLS.
func options(id Identity, s Sizes, tlsCfg *tls.Config) *clickhouse.Options {
	o := &clickhouse.Options{
		Addr:         []string{id.Addr},
		Auth:         clickhouse.Auth{Database: id.Database, Username: id.Username, Password: id.Password},
		MaxOpenConns: s.MaxOpenConns,
		MaxIdleConns: s.MaxIdleConns,
	}
	if id.TLS.Enabled {
		o.TLS = tlsCfg
	}
	return o
}

func dial(id Identity, s Sizes, tlsCfg *tls.Config) (driver.Conn, error) {
	return clickhouse.Open(options(id, s, tlsCfg))
}

// Identity returns the tuple this pool is for.
func (m *Manager) Identity() Identity { return m.id }

// Sizes returns the pool's current size.
func (m *Manager) Sizes() Sizes { return m.cur.Load().sizes }

// TLSConfig returns the tuple's TLS config, nil for a zero tls block: what
// the https hop of the HTTP interface is configured with.
func (m *Manager) TLSConfig() *tls.Config { return m.tlsCfg }

// Resize swaps in a connection sized s when s differs from the current
// size — the tls block is the tuple's and never changes under a Manager, so
// no file is re-read. The replaced connection is closed after grace, so the
// queries in flight on it finish; grace is the longest query timeout among
// the tenants sharing the pool.
func (m *Manager) Resize(s Sizes, grace time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.cur.Load()
	if old.sizes == s {
		return nil
	}
	conn, err := m.dial(m.id, s, m.tlsCfg)
	if err != nil {
		return fmt.Errorf("open %s: %w", m.id.Addr, err)
	}
	m.cur.Store(&state{sizes: s, conn: conn})
	time.AfterFunc(grace, func() { _ = old.conn.Close() })
	return nil
}

// Release closes the pool after grace, for a tuple no tenant names any
// more: a consumer that resolved this Manager just before the reload's
// swap finishes its query on it.
func (m *Manager) Release(grace time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.cur.Load()
	time.AfterFunc(grace, func() { _ = st.conn.Close() })
}

// Close closes the current connection now. Replaced and released
// connections close on their own grace timers.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.cur.Load(); s != nil {
		return s.conn.Close()
	}
	return errors.New("chconn: not open")
}

func (m *Manager) conn() driver.Conn { return m.cur.Load().conn }

// driver.Conn — every call delegates to the connection current at the call.

func (m *Manager) Contributors() []string                        { return m.conn().Contributors() }
func (m *Manager) ServerVersion() (*driver.ServerVersion, error) { return m.conn().ServerVersion() }
func (m *Manager) Select(ctx context.Context, dest any, query string, args ...any) error {
	return m.conn().Select(ctx, dest, query, args...)
}

func (m *Manager) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return m.conn().Query(ctx, query, args...)
}

func (m *Manager) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return m.conn().QueryRow(ctx, query, args...)
}

func (m *Manager) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return m.conn().PrepareBatch(ctx, query, opts...)
}

func (m *Manager) Exec(ctx context.Context, query string, args ...any) error {
	return m.conn().Exec(ctx, query, args...)
}

func (m *Manager) QueryFormat(ctx context.Context, format, query string, args ...any) (io.ReadCloser, error) {
	return m.conn().QueryFormat(ctx, format, query, args...)
}

func (m *Manager) InsertFormat(ctx context.Context, format, query string, data io.Reader) error {
	return m.conn().InsertFormat(ctx, format, query, data)
}

func (m *Manager) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return m.conn().AsyncInsert(ctx, query, wait, args...) //nolint:staticcheck // SA1019: deprecated upstream, but delegation must cover the whole driver.Conn interface
}
func (m *Manager) Ping(ctx context.Context) error { return m.conn().Ping(ctx) }
func (m *Manager) Stats() driver.Stats            { return m.conn().Stats() }

// Member is one tenant's ask of the pools: the tuple it shares, and its own
// sizes, deadline and HTTP wiring.
type Member struct {
	Tenant tenant.ID
	Params Params
}

// tuple is one open pool and the tenants on it, each with the Params last
// applied for it — what its Target is read from, and what the pool's size
// and close grace are the maxima of. manager is nil only inside a
// reconcile, for a pool planned but not opened yet.
type tuple struct {
	manager *Manager
	members map[tenant.ID]Params
}

// sizes is the pool size the members ask for together.
func (t *tuple) sizes() Sizes {
	var s Sizes
	for _, p := range t.members {
		s = s.max(p.Sizes())
	}
	return s
}

// grace is how long a connection the members may be querying stays open
// once replaced or released: the longest of their query timeouts.
func (t *tuple) grace() time.Duration {
	var g time.Duration
	for _, p := range t.members {
		g = max(g, p.QueryTimeout)
	}
	return g
}

func (t *tuple) tenants() []tenant.ID {
	return slices.Sorted(maps.Keys(t.members))
}

// snapshot is the pools at one instant, replaced whole by a reconcile so a
// lookup is one lock-free load.
type snapshot struct {
	tuples  map[Identity]*tuple
	tenants map[tenant.ID]Identity // each tenant's tuple
}

func (s *snapshot) clone() *snapshot {
	next := &snapshot{tuples: make(map[Identity]*tuple, len(s.tuples)), tenants: maps.Clone(s.tenants)}
	for id, t := range s.tuples {
		next.tuples[id] = &tuple{manager: t.manager, members: maps.Clone(t.members)}
	}
	return next
}

// Pools holds one Manager per tuple the served tenants name, reconciled
// after every settings reload: a new tuple opens a pool (no dial), a tenant
// whose tuple changed is repointed, a tuple no tenant names any more is
// released after its grace, and a pool shared by several tenants is sized to
// their largest ask. The ceiling — the boot config's
// clickhouse.max_total_conns, 0 for none — bounds the sum of the open pools'
// MaxOpenConns: at boot it refuses to open, and at a reload a resize above it
// is refused with the pool kept at its size, and a tuple that cannot be
// opened leaves its tenants where they were, on their previous pool, or with
// none when they had none. Both are logged by the caller and retried by the
// next reload. Resolution is per tenant: For, Target, SharingTables.
type Pools struct {
	ceiling int
	dial    dialer

	mu  sync.Mutex // serializes Reconcile and Close against each other
	cur atomic.Pointer[snapshot]
}

// NewPools opens the pools want names, under ceiling. Any refusal — a
// certificate file that cannot be read, or pools that would add up to more
// than the ceiling, named with the sum — refuses boot: nothing is left open.
func NewPools(ceiling int, want []Member) (*Pools, error) {
	p := newPools(ceiling, dial)
	if _, err := p.Reconcile(want); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func newPools(ceiling int, d dialer) *Pools {
	p := &Pools{ceiling: ceiling, dial: d}
	p.cur.Store(&snapshot{tuples: map[Identity]*tuple{}, tenants: map[tenant.ID]Identity{}})
	return p
}

// Reconcile sets the pools to what want names (the served tenants, in id
// order), and returns the tenants it admitted that were not on a pool
// before — new, or back after a rejection or removal — for the caller to
// treat their cache as stale, with every refusal joined. The walk keeps the
// ceiling at every step: tenants no longer wanted leave first, then each
// wanted tenant is placed in turn, and a placement the ceiling refuses is
// undone before the next, so a refused move leaves the tenant on the pool it
// had, with the Params it had. What was refused is placed once more at the
// end, since a shrink or a move placed after it may have freed the budget it
// needed; only what the second pass refuses is reported.
func (p *Pools) Reconcile(want []Member) (readmitted []tenant.ID, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.cur.Load()
	w := &walk{ceiling: p.ceiling, dial: p.dial, next: cur.clone(), planned: map[Identity]Sizes{}, pending: map[Identity]*tls.Config{}}
	for id, t := range w.next.tuples {
		w.planned[id] = t.manager.Sizes()
	}

	wanted := make(map[tenant.ID]bool, len(want))
	for _, m := range want {
		wanted[m.Tenant] = true
	}
	for _, id := range slices.Sorted(maps.Keys(w.next.tenants)) {
		if !wanted[id] {
			w.leave(id)
		}
	}
	var refused []Member
	for _, m := range want {
		if !w.place(m) {
			refused = append(refused, m)
		}
	}
	if len(refused) > 0 {
		w.errs, w.refused = nil, nil
		for _, m := range refused {
			w.place(m)
		}
	}

	// Apply: release the emptied tuples, open the planned ones at the size
	// the walk settled on, resize the rest to their plan. A connection the
	// previous members may still be querying stays open for the longest of
	// their timeouts.
	for _, id := range w.order() {
		t := w.next.tuples[id]
		s := w.planned[id]
		switch {
		case len(t.members) == 0:
			if t.manager != nil {
				t.manager.Release(w.graceBefore(cur, id))
			}
			delete(w.next.tuples, id)
		case t.manager == nil:
			mgr, err := openWith(w.dial, id, s, w.pending[id])
			if err != nil {
				// A malformed option, which settings.Validate excludes: the
				// tenants planned onto it end up on no pool.
				w.errs = append(w.errs, fmt.Errorf("clickhouse pool %s not opened for tenants %v: %w", id, t.tenants(), err))
				for tid := range t.members {
					delete(w.next.tenants, tid)
				}
				delete(w.next.tuples, id)
				continue
			}
			t.manager = mgr
		case s != t.manager.Sizes():
			if err := t.manager.Resize(s, w.graceBefore(cur, id)); err != nil {
				w.errs = append(w.errs, fmt.Errorf("clickhouse pool %s kept at %d open connections: %w", id, t.manager.Sizes().MaxOpenConns, err))
			}
		}
	}
	p.cur.Store(w.next)
	for _, id := range slices.Sorted(maps.Keys(w.next.tenants)) {
		if _, had := cur.tenants[id]; !had {
			readmitted = append(readmitted, id)
		}
	}
	return readmitted, errors.Join(w.errs...)
}

// walk is one Reconcile's working state: the snapshot being built, and the
// size each of its tuples is planned to be resized to, which the ceiling
// check sums over the tuples that still have members.
type walk struct {
	ceiling int
	dial    dialer
	next    *snapshot
	planned map[Identity]Sizes
	// refused is the tuples whose growth this pass already refused, so a
	// shared pool's refusal is reported once, naming every member.
	refused map[Identity]bool
	// pending is the TLS config of each tuple planned but not opened yet:
	// the files are read when the tuple is planned, since an unreadable one
	// is a refusal to undo in place, and the pool opens once the walk knows
	// its size.
	pending map[Identity]*tls.Config
	errs    []error
}

// order is the tuples in a fixed order, for logs and errors.
func (w *walk) order() []Identity {
	ids := slices.Collect(maps.Keys(w.next.tuples))
	slices.SortFunc(ids, func(a, b Identity) int { return strings.Compare(a.String(), b.String()) })
	return ids
}

// graceBefore is the close grace of tuple id as it was before this
// reconcile: the longest query timeout among the members it had, since they
// are who may still be querying its connection.
func (w *walk) graceBefore(cur *snapshot, id Identity) time.Duration {
	if prev, ok := cur.tuples[id]; ok {
		return prev.grace()
	}
	return w.next.tuples[id].grace()
}

// fits reports whether tuple id at size s keeps the open pools within the
// ceiling, and the sum they would be at: the planned sizes of the other
// tuples that have members, plus s.
func (w *walk) fits(id Identity, s Sizes) (bool, int) {
	sum := s.MaxOpenConns
	for other, t := range w.next.tuples {
		if other != id && len(t.members) > 0 {
			sum += w.planned[other].MaxOpenConns
		}
	}
	return w.ceiling <= 0 || sum <= w.ceiling, sum
}

// leave takes id off its tuple. The tuple's plan shrinks to its remaining
// members' ask when that is smaller; a shrink is always within the ceiling.
func (w *walk) leave(id tenant.ID) {
	ident, ok := w.next.tenants[id]
	if !ok {
		return
	}
	delete(w.next.tenants, id)
	t := w.next.tuples[ident]
	delete(t.members, id)
	if s := t.sizes(); s.MaxOpenConns <= w.planned[ident].MaxOpenConns {
		w.planned[ident] = s
	}
}

// place puts m on the tuple its Params name — its current one, updated in
// place, or another, which it moves to when the tuple opens under the
// ceiling — and reports whether the ceiling let it. A refused move leaves m
// on the tuple it had, with the Params it had.
func (w *walk) place(m Member) bool {
	ident := m.Params.Identity()
	prev, had := w.next.tenants[m.Tenant]
	if had && prev == ident {
		w.next.tuples[ident].members[m.Tenant] = m.Params
		return w.grow(ident)
	}
	var prevParams Params
	var prevPlanned Sizes
	if had {
		prevParams, prevPlanned = w.next.tuples[prev].members[m.Tenant], w.planned[prev]
		w.leave(m.Tenant)
	}
	err := w.join(m, ident)
	if err == nil {
		return true
	}
	if had {
		// Back where it was, with the Params it had: the step before this
		// was within the ceiling, so restoring it is too.
		w.next.tuples[prev].members[m.Tenant] = prevParams
		w.next.tenants[m.Tenant] = prev
		w.planned[prev] = prevPlanned
		err = fmt.Errorf("%w; tenant %s keeps its previous pool %s", err, m.Tenant, prev)
	}
	w.errs = append(w.errs, err)
	return false
}

// join adds m to tuple ident, opening the pool when it is new. A refusal —
// over the ceiling, or a pool that cannot be opened — leaves m off it.
func (w *walk) join(m Member, ident Identity) error {
	t, exists := w.next.tuples[ident]
	if !exists {
		s := m.Params.Sizes()
		if ok, sum := w.fits(ident, s); !ok {
			return fmt.Errorf("clickhouse pool %s not opened for tenant %s: its clickhouse.max_open_conns %d would put the open pools at %d, above clickhouse.max_total_conns %d (boot config)", ident, m.Tenant, s.MaxOpenConns, sum, w.ceiling)
		}
		tlsCfg, err := ident.TLS.config()
		if err != nil {
			return fmt.Errorf("clickhouse pool %s not opened for tenant %s: %w", ident, m.Tenant, err)
		}
		w.next.tuples[ident] = &tuple{members: map[tenant.ID]Params{m.Tenant: m.Params}}
		w.next.tenants[m.Tenant] = ident
		w.planned[ident] = s
		w.pending[ident] = tlsCfg
		return nil
	}
	// An existing pool takes a tenant whose ask fits its planned size, and
	// grows for a larger one when the ceiling allows.
	s := w.planned[ident].max(m.Params.Sizes())
	if s.MaxOpenConns > w.planned[ident].MaxOpenConns {
		if ok, sum := w.fits(ident, s); !ok {
			return fmt.Errorf("clickhouse pool %s not grown for tenant %s: its clickhouse.max_open_conns %d would put the open pools at %d, above clickhouse.max_total_conns %d (boot config)", ident, m.Tenant, s.MaxOpenConns, sum, w.ceiling)
		}
	}
	w.planned[ident] = s
	t.members[m.Tenant] = m.Params
	w.next.tenants[m.Tenant] = ident
	return nil
}

// grow re-plans tuple ident's size after a member's Params changed in place:
// its members' ask when that fits the ceiling, else the size it has, which
// the next reload retries. Reports whether the ask fit.
func (w *walk) grow(ident Identity) bool {
	t := w.next.tuples[ident]
	s := t.sizes()
	if s.MaxOpenConns <= w.planned[ident].MaxOpenConns {
		w.planned[ident] = s
		return true
	}
	if ok, sum := w.fits(ident, s); !ok {
		if !w.refused[ident] {
			if w.refused == nil {
				w.refused = map[Identity]bool{}
			}
			w.refused[ident] = true
			w.errs = append(w.errs, fmt.Errorf("clickhouse pool %s not resized for tenants %v: their clickhouse.max_open_conns %d would put the open pools at %d, above clickhouse.max_total_conns %d (boot config); it keeps %d", ident, t.tenants(), s.MaxOpenConns, sum, w.ceiling, w.planned[ident].MaxOpenConns))
		}
		return false
	}
	w.planned[ident] = s
	return true
}

// For returns the pool of tenant id, or nil when the tenant is on none: it
// is not served, or its tuple could not be opened.
func (p *Pools) For(id tenant.ID) *Manager {
	snap := p.cur.Load()
	ident, ok := snap.tenants[id]
	if !ok {
		return nil
	}
	return snap.tuples[ident].manager
}

// Target returns the HTTP-interface wiring of tenant id — its own HTTP
// port, scheme and headers over its pool's host, credentials, database and
// TLS config — or the zero Target when it is on no pool.
func (p *Pools) Target(id tenant.ID) Target {
	snap := p.cur.Load()
	ident, ok := snap.tenants[id]
	if !ok {
		return Target{}
	}
	t := snap.tuples[ident]
	return t.members[id].target(t.manager.tlsCfg)
}

// SharingTables returns the tenants reading the same tables as tenant id —
// the same address and database, whatever their user or tls — id included,
// in id order; nil when id is on no pool.
func (p *Pools) SharingTables(id tenant.ID) []tenant.ID {
	snap := p.cur.Load()
	ident, ok := snap.tenants[id]
	if !ok {
		return nil
	}
	var ids []tenant.ID
	for tid, tident := range snap.tenants {
		if tident.Addr == ident.Addr && tident.Database == ident.Database {
			ids = append(ids, tid)
		}
	}
	slices.Sort(ids)
	return ids
}

// Ping pings every open pool at once and returns nil at the first answer,
// or every pool's error joined when none answers — including when none is
// open. At once, not in turn: the driver waits up to its dial timeout on a
// host that does not answer, longer than a readiness probe's, so one such
// pool must not hide the ones that do answer.
func (p *Pools) Ping(ctx context.Context) error {
	snap := p.cur.Load()
	ids := slices.Collect(maps.Keys(snap.tuples))
	if len(ids) == 0 {
		return errors.New("no ClickHouse pool is open")
	}
	slices.SortFunc(ids, func(a, b Identity) int { return strings.Compare(a.String(), b.String()) })
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		i   int
		err error
	}
	results := make(chan result, len(ids))
	for i, id := range ids {
		go func() { results <- result{i, snap.tuples[id].manager.Ping(ctx)} }()
	}
	errs := make([]error, len(ids))
	for range ids {
		r := <-results
		if r.err == nil {
			return nil
		}
		errs[r.i] = fmt.Errorf("%s: %w", ids[r.i], r.err)
	}
	return errors.Join(errs...)
}

// Close closes every open pool now and leaves none.
func (p *Pools) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := p.cur.Swap(&snapshot{tuples: map[Identity]*tuple{}, tenants: map[tenant.ID]Identity{}})
	var errs []error
	for _, t := range snap.tuples {
		if err := t.manager.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// HTTPClients hands an HTTP-interface consumer the client for a target: one
// per TLS config, made by the consumer's own factory (the worker's tuned
// transport, the proxy's redirect policy) and kept for the process lifetime.
// A config is a tuple's, so the set grows with the distinct tls blocks ever
// applied, not with requests; a client whose tuple is gone keeps only its
// transport, whose idle connections time out on their own.
//
// The factory gets a copy of the config, never the target's own: net/http
// appends its HTTP/2 protocols to TLSClientConfig.NextProtos in place when
// a transport first dials, which would race the driver's handshakes on the
// shared config and offer HTTP protocols on the native hop. The target's
// pointer stays the cache key.
type HTTPClients struct {
	build func(*tls.Config) *http.Client

	mu      sync.Mutex
	clients map[*tls.Config]*http.Client
}

// NewHTTPClients returns a cache whose clients build makes from a target's
// TLS config, nil meaning net/http's defaults.
func NewHTTPClients(build func(*tls.Config) *http.Client) *HTTPClients {
	return &HTTPClients{build: build, clients: map[*tls.Config]*http.Client{}}
}

// For returns the client for t.
func (c *HTTPClients) For(t Target) *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[t.TLS]; ok {
		return cl
	}
	cl := c.build(t.TLS.Clone())
	c.clients[t.TLS] = cl
	return cl
}
