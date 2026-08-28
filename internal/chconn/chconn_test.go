package chconn

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConn records closes; embedding the interface leaves the unused
// methods nil, which is fine — they are never called here.
type fakeConn struct {
	driver.Conn
	name   string
	closed atomic.Bool
}

func (f *fakeConn) Close() error { f.closed.Store(true); return nil }

func params(addr string) Params {
	return Params{Addr: addr, HTTPPort: 8123, HTTPScheme: "http", Database: "db", Username: "u", Password: "p", QueryTimeout: time.Second}
}

func newManager(t *testing.T, dial func(Params) (driver.Conn, error)) *Manager {
	t.Helper()
	m := &Manager{dial: dial, grace: 10 * time.Millisecond}
	first, err := dial(params("a:9000"))
	require.NoError(t, err)
	m.cur.Store(&state{params: params("a:9000"), conn: first})
	return m
}

func TestManager_TargetDerivesHTTPURL(t *testing.T) {
	t.Parallel()
	m := newManager(t, func(p Params) (driver.Conn, error) { return &fakeConn{name: p.Addr}, nil })
	assert.Equal(t, Target{URL: "http://a:8123", Username: "u", Password: "p", Database: "db"}, m.Target())
	assert.Equal(t, "db", m.Database())
	assert.Equal(t, time.Second, m.QueryTimeout())
}

func TestManager_ReconfigureSwapsAndClosesOldAfterGrace(t *testing.T) {
	t.Parallel()
	conns := map[string]*fakeConn{}
	m := newManager(t, func(p Params) (driver.Conn, error) {
		c := &fakeConn{name: p.Addr}
		conns[p.Addr] = c
		return c, nil
	})
	require.NoError(t, m.Reconfigure(params("b:9000")))
	assert.Equal(t, "b:9000", m.Addr())
	assert.Equal(t, "http://b:8123", m.Target().URL)
	assert.Same(t, conns["b:9000"], m.conn())
	assert.Eventually(t, func() bool { return conns["a:9000"].closed.Load() }, time.Second, 5*time.Millisecond, "old connection closes after the grace period")
	assert.False(t, conns["b:9000"].closed.Load())
}

func TestManager_ReconfigureSameParamsIsNoop(t *testing.T) {
	t.Parallel()
	dials := 0
	m := newManager(t, func(p Params) (driver.Conn, error) { dials++; return &fakeConn{name: p.Addr}, nil })
	require.NoError(t, m.Reconfigure(params("a:9000")))
	assert.Equal(t, 1, dials, "identical wiring must not re-dial")
}

// TestManager_ReconfigureDialError pins the one way a swap can fail: a
// malformed option (excluded by settings.Validate) leaves the current
// connection in place. Reachability is never checked here.
func TestManager_ReconfigureDialError(t *testing.T) {
	t.Parallel()
	m := newManager(t, func(p Params) (driver.Conn, error) {
		if p.Addr == "bad:9000" {
			return nil, errors.New("bad options")
		}
		return &fakeConn{name: p.Addr}, nil
	})
	require.ErrorContains(t, m.Reconfigure(params("bad:9000")), "open bad:9000")
	assert.Equal(t, "a:9000", m.Addr())
}
