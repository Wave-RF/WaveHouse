// Package chconn owns the process's ClickHouse connection so the wiring —
// address, database, user, timeout — can follow a settings reload without a
// restart. Manager implements driver.Conn by delegating every call to the
// connection current at that instant, so consumers hold one driver.Conn for
// the process lifetime and never learn a reconnect happened; the HTTP-side
// consumers (ingest INSERTs, the raw-SQL proxy) read Target per request.
package chconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Params is everything a connection is built from: the settings-directory
// wiring plus the boot-config password.
type Params struct {
	Addr         string
	HTTPPort     int
	HTTPScheme   string
	Database     string
	Username     string
	Password     string
	QueryTimeout time.Duration
}

// Target is what the HTTP-interface consumers need per request.
type Target struct {
	// URL is the HTTP base, e.g. http://host:8123.
	URL      string
	Username string
	Password string
	Database string
}

// target derives the HTTP base URL from the native address's host. Addr is
// host:port by settings.Validate's contract, so the split cannot fail.
func (p Params) target() Target {
	host, _, _ := net.SplitHostPort(p.Addr)
	return Target{
		URL:      fmt.Sprintf("%s://%s", p.HTTPScheme, net.JoinHostPort(host, strconv.Itoa(p.HTTPPort))),
		Username: p.Username,
		Password: p.Password,
		Database: p.Database,
	}
}

type state struct {
	params Params
	conn   driver.Conn
}

// Manager is a driver.Conn whose backing connection is swapped by Reconfigure.
type Manager struct {
	logger *slog.Logger
	// dial is the connection factory; tests substitute it.
	dial func(Params) (driver.Conn, error)
	// grace is how long a replaced connection stays open for in-flight
	// queries before it is closed.
	grace time.Duration

	mu  sync.Mutex // serializes Reconfigure/Close against each other
	cur atomic.Pointer[state]
}

var _ driver.Conn = (*Manager)(nil)

// Open builds the boot-time connection. Like clickhouse.Open it does not
// dial — boot tolerates an unreachable ClickHouse (schema discovery degrades
// and retries) — so only a malformed option errors here.
func Open(p Params, logger *slog.Logger) (*Manager, error) {
	m := &Manager{logger: logger, dial: dial, grace: p.QueryTimeout}
	conn, err := m.dial(p)
	if err != nil {
		return nil, err
	}
	m.cur.Store(&state{params: p, conn: conn})
	return m, nil
}

func dial(p Params) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{p.Addr},
		Auth: clickhouse.Auth{Database: p.Database, Username: p.Username, Password: p.Password},
	})
}

// Reconfigure swaps in a connection built from p when p differs from the
// current wiring. The adopted settings are the authority: the swap is
// unconditional and, like Open, does not dial — an unreachable address
// surfaces where reachability is already handled (schema discovery
// retries, /readyz, query errors) and is fixed by the next reload. Only a
// malformed option errors, which settings.Validate already excludes. The
// replaced connection is closed after the grace period so in-flight
// queries on it finish.
func (m *Manager) Reconfigure(p Params) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.cur.Load()
	if old != nil && old.params == p {
		return nil
	}
	conn, err := m.dial(p)
	if err != nil {
		return fmt.Errorf("open %s: %w", p.Addr, err)
	}
	m.cur.Store(&state{params: p, conn: conn})
	if old != nil {
		grace := m.grace
		time.AfterFunc(grace, func() { _ = old.conn.Close() })
	}
	m.grace = p.QueryTimeout
	return nil
}

// Target returns the current HTTP-interface wiring.
func (m *Manager) Target() Target { return m.cur.Load().params.target() }

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
