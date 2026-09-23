// Package chconn owns the process's ClickHouse connection so the wiring —
// address, database, user, timeout, TLS, pool size — can follow a settings
// reload without a restart. Manager implements driver.Conn by delegating
// every call to the connection current at that instant, so consumers hold
// one driver.Conn for the process lifetime and never learn a reconnect
// happened; the HTTP-side consumers (ingest INSERTs, the raw-SQL proxy) read
// Target per request and take their client from an HTTPClients.
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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
// settings.Validate checked the shape without opening anything; an
// unreadable path is the one error left, and it names the key.
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

// Params is everything a connection is built from: the settings
// directory's clickhouse block plus the boot-config password.
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

// equal reports whether p and q describe the same connection. Field by
// field, since the headers map keeps Params from being comparable;
// TestParams_EqualCoversEveryField keeps the list complete.
func (p Params) equal(q Params) bool {
	return p.Addr == q.Addr && p.HTTPPort == q.HTTPPort && p.HTTPScheme == q.HTTPScheme &&
		p.Database == q.Database && p.Username == q.Username && p.Password == q.Password &&
		p.QueryTimeout == q.QueryTimeout && p.TLS == q.TLS && maps.Equal(p.Headers, q.Headers) &&
		p.MaxOpenConns == q.MaxOpenConns && p.MaxIdleConns == q.MaxIdleConns
}

// Target is what the HTTP-interface consumers need per request.
type Target struct {
	// URL is the HTTP base, e.g. http://host:8123.
	URL      string
	Username string
	Password string
	Database string
	// TLS configures an https URL; nil means net/http's defaults. The
	// pointer changes only when a reload changes the tls block, which is
	// what HTTPClients keys on.
	TLS *tls.Config
	// Headers go on every request ahead of the consumer's own; read-only.
	Headers map[string]string
}

type state struct {
	params Params
	conn   driver.Conn
	tlsCfg *tls.Config
}

// target derives the HTTP wiring from the native address's host. Addr is
// host:port by settings.Validate's contract, so the split cannot fail.
func (s *state) target() Target {
	host, _, _ := net.SplitHostPort(s.params.Addr)
	return Target{
		URL:      fmt.Sprintf("%s://%s", s.params.HTTPScheme, net.JoinHostPort(host, strconv.Itoa(s.params.HTTPPort))),
		Username: s.params.Username,
		Password: s.params.Password,
		Database: s.params.Database,
		TLS:      s.tlsCfg,
		Headers:  s.params.Headers,
	}
}

// Manager is a driver.Conn whose backing connection is swapped by Reconfigure.
type Manager struct {
	// dial is the connection factory; tests substitute it.
	dial func(Params, *tls.Config) (driver.Conn, error)
	// grace is how long a replaced connection stays open for in-flight
	// queries before it is closed.
	grace time.Duration

	mu  sync.Mutex // serializes Reconfigure/Close against each other
	cur atomic.Pointer[state]
}

var _ driver.Conn = (*Manager)(nil)

// Open builds the boot-time connection. Like clickhouse.Open it does not
// dial — boot tolerates an unreachable ClickHouse (schema discovery degrades
// and retries) — so only a malformed option or an unreadable certificate
// file errors here.
func Open(p Params) (*Manager, error) {
	m := &Manager{dial: dial, grace: p.QueryTimeout}
	st, err := m.open(p, nil)
	if err != nil {
		return nil, err
	}
	m.cur.Store(st)
	return m, nil
}

// open builds p's state. The TLS config is carried over from prev while the
// tls block is unchanged, so a reload that moves only the database neither
// re-reads the certificate files nor makes the HTTP consumers rebuild their
// transports; a changed block reads the files again.
func (m *Manager) open(p Params, prev *state) (*state, error) {
	var tlsCfg *tls.Config
	if prev != nil && prev.params.TLS == p.TLS {
		tlsCfg = prev.tlsCfg
	} else {
		var err error
		if tlsCfg, err = p.TLS.config(); err != nil {
			return nil, err
		}
	}
	conn, err := m.dial(p, tlsCfg)
	if err != nil {
		return nil, err
	}
	return &state{params: p, conn: conn, tlsCfg: tlsCfg}, nil
}

// options is what the driver opens: the native address and credentials,
// the pool sizes, and the TLS config when the native hop is TLS.
func options(p Params, tlsCfg *tls.Config) *clickhouse.Options {
	o := &clickhouse.Options{
		Addr:         []string{p.Addr},
		Auth:         clickhouse.Auth{Database: p.Database, Username: p.Username, Password: p.Password},
		MaxOpenConns: p.MaxOpenConns,
		MaxIdleConns: p.MaxIdleConns,
	}
	if p.TLS.Enabled {
		o.TLS = tlsCfg
	}
	return o
}

func dial(p Params, tlsCfg *tls.Config) (driver.Conn, error) {
	return clickhouse.Open(options(p, tlsCfg))
}

// Reconfigure swaps in a connection built from p when p differs from the
// current wiring. The adopted settings are the authority: the swap is
// unconditional and, like Open, does not dial — an unreachable address
// surfaces where reachability is already handled (schema discovery
// retries, /readyz, query errors) and is fixed by the next reload. Only a
// malformed option, which settings.Validate already excludes, or a
// certificate file that cannot be read errors, and then the current
// connection stays. The replaced connection is closed after the grace
// period so in-flight queries on it finish.
func (m *Manager) Reconfigure(p Params) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.cur.Load()
	if old != nil && old.params.equal(p) {
		return nil
	}
	st, err := m.open(p, old)
	if err != nil {
		return fmt.Errorf("open %s: %w", p.Addr, err)
	}
	m.cur.Store(st)
	if old != nil {
		grace := m.grace
		time.AfterFunc(grace, func() { _ = old.conn.Close() })
	}
	m.grace = p.QueryTimeout
	return nil
}

// Target returns the current HTTP-interface wiring.
func (m *Manager) Target() Target { return m.cur.Load().target() }

// Database returns the current database name.
func (m *Manager) Database() string { return m.cur.Load().params.Database }

// QueryTimeout returns the current read deadline.
func (m *Manager) QueryTimeout() time.Duration { return m.cur.Load().params.QueryTimeout }

// Addr returns the current native address (for logs).
func (m *Manager) Addr() string { return m.cur.Load().params.Addr }

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

// Close closes the current connection. Replaced connections close on their
// own grace timers.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.cur.Load(); s != nil {
		return s.conn.Close()
	}
	return errors.New("chconn: not open")
}

// HTTPClients hands an HTTP-interface consumer the client for the current
// target: one per TLS config, made by the consumer's own factory (the
// worker's tuned transport, the proxy's redirect policy) and replaced when
// a reload changes the tls block, with the replaced transport's idle
// connections closed. Requests in flight on the old client finish.
type HTTPClients struct {
	build func(*tls.Config) *http.Client

	mu  sync.Mutex
	cfg *tls.Config
	cur *http.Client
}

// NewHTTPClients returns a cache whose clients build makes from the
// target's TLS config, nil meaning net/http's defaults.
func NewHTTPClients(build func(*tls.Config) *http.Client) *HTTPClients {
	return &HTTPClients{build: build}
}

// For returns the client for t.
func (c *HTTPClients) For(t Target) *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur == nil || c.cfg != t.TLS {
		if c.cur != nil {
			c.cur.CloseIdleConnections()
		}
		c.cfg, c.cur = t.TLS, c.build(t.TLS)
	}
	return c.cur
}
