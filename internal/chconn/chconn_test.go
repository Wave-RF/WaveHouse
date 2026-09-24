package chconn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// fakeConn records closes and answers Ping as told; embedding the interface
// leaves the unused methods nil, which is fine — they are never called here.
type fakeConn struct {
	driver.Conn
	id     Identity
	sizes  Sizes
	closed atomic.Bool
	// ping answers Ping; nil means success at once.
	ping func(context.Context) error
}

func (f *fakeConn) Close() error { f.closed.Store(true); return nil }

func (f *fakeConn) Ping(ctx context.Context) error {
	if f.ping == nil {
		return nil
	}
	return f.ping(ctx)
}

// fakeDial stands in for the driver; the TLS config still comes through
// the real path (TLS.config), which is what the TLS tests exercise.
func fakeDial(id Identity, s Sizes, _ *tls.Config) (driver.Conn, error) {
	return &fakeConn{id: id, sizes: s}, nil
}

// dialRecorder is a fakeDial that keeps every connection it made, by
// address, latest last.
type dialRecorder struct {
	mu    sync.Mutex
	conns map[string][]*fakeConn
	// ping is given to every connection made.
	ping func(context.Context) error
}

func (r *dialRecorder) dial(id Identity, s Sizes, _ *tls.Config) (driver.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns == nil {
		r.conns = map[string][]*fakeConn{}
	}
	c := &fakeConn{id: id, sizes: s, ping: r.ping}
	r.conns[id.Addr] = append(r.conns[id.Addr], c)
	return c, nil
}

// latest is the newest connection made for addr.
func (r *dialRecorder) latest(addr string) *fakeConn {
	r.mu.Lock()
	defer r.mu.Unlock()
	cs := r.conns[addr]
	return cs[len(cs)-1]
}

func (r *dialRecorder) count(addr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns[addr])
}

func params(addr string) Params {
	return Params{Addr: addr, HTTPPort: 8123, HTTPScheme: "http", Database: "db", Username: "u", Password: "p", QueryTimeout: time.Second, MaxOpenConns: 10, MaxIdleConns: 5}
}

func newManager(t *testing.T, d dialer) *Manager {
	t.Helper()
	m, err := open(d, params("a:9000").Identity(), Sizes{MaxOpenConns: 10, MaxIdleConns: 5})
	require.NoError(t, err)
	return m
}

// writeTestPKI writes a self-signed authority and a client certificate it
// signed, returning the three paths clickhouse.tls names.
func writeTestPKI(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "wavehouse"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	write := func(name, typ string, der []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
		return path
	}
	return write("ca.pem", "CERTIFICATE", caDER), write("client.pem", "CERTIFICATE", leafDER), write("client.key", "EC PRIVATE KEY", keyDER)
}

func TestParams_TargetDerivesHTTPURL(t *testing.T) {
	t.Parallel()
	assert.Equal(t, Target{URL: "http://a:8123", Username: "u", Password: "p", Database: "db"}, params("a:9000").target(nil))
	p := params("a:9000")
	p.HTTPScheme, p.HTTPPort, p.Headers = "https", 8443, map[string]string{"X-Proxy-Token": "abc"}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	tgt := p.target(cfg)
	assert.Equal(t, "https://a:8443", tgt.URL)
	assert.Same(t, cfg, tgt.TLS)
	assert.Equal(t, map[string]string{"X-Proxy-Token": "abc"}, tgt.Headers)
}

// TestIdentity_IsTheComparableTuple: the tuple is a plain value, so two
// Params naming the same pool compare equal and index the same map entry,
// and every field of the tuple — the tls block included — tells them apart.
func TestIdentity_IsTheComparableTuple(t *testing.T) {
	t.Parallel()
	base := params("a:9000")
	base.TLS = TLS{Enabled: true, ServerName: "ch.internal"}
	same := base
	same.HTTPPort, same.QueryTimeout, same.MaxOpenConns, same.Headers = 9999, time.Hour, 99, map[string]string{"X-A": "1"}
	assert.Equal(t, base.Identity(), same.Identity(), "the HTTP wiring, the deadline, the sizes and the headers are the tenant's own")

	for name, change := range map[string]func(*Params){
		"addr":     func(p *Params) { p.Addr = "b:9000" },
		"database": func(p *Params) { p.Database = "other" },
		"username": func(p *Params) { p.Username = "reporting" },
		"password": func(p *Params) { p.Password = "x" },
		"tls":      func(p *Params) { p.TLS.InsecureSkipVerify = true },
	} {
		changed := base
		change(&changed)
		assert.NotEqual(t, base.Identity(), changed.Identity(), name)
	}
	assert.Equal(t, "a:9000 database db user u", base.Identity().String(), "named without the password")
}

func TestSizes_MaxIsPerDimension(t *testing.T) {
	t.Parallel()
	a, b := Sizes{MaxOpenConns: 10, MaxIdleConns: 5}, Sizes{MaxOpenConns: 8, MaxIdleConns: 8}
	assert.Equal(t, Sizes{MaxOpenConns: 10, MaxIdleConns: 8}, a.max(b))
	assert.Equal(t, a.max(b), b.max(a))
}

func TestManager_ResizeSwapsAndClosesOldAfterGrace(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	m := newManager(t, rec.dial)
	first := rec.latest("a:9000")
	require.NoError(t, m.Resize(Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, 10*time.Millisecond))
	assert.Equal(t, Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, m.Sizes())
	assert.Same(t, rec.latest("a:9000"), m.conn())
	assert.Equal(t, Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, rec.latest("a:9000").sizes, "the driver is opened at the new size")
	assert.Eventually(t, func() bool { return first.closed.Load() }, time.Second, 5*time.Millisecond, "old connection closes after the grace period")
	assert.False(t, rec.latest("a:9000").closed.Load())
	assert.Equal(t, "a:9000", m.Identity().Addr)
}

func TestManager_ResizeSameSizesIsNoop(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	m := newManager(t, rec.dial)
	require.NoError(t, m.Resize(m.Sizes(), time.Millisecond))
	assert.Equal(t, 1, rec.count("a:9000"), "an unchanged size must not re-dial")
}

// TestManager_ResizeDialError: a malformed option (excluded by
// settings.Validate) leaves the current connection in place.
func TestManager_ResizeDialError(t *testing.T) {
	t.Parallel()
	var fail atomic.Bool
	m := newManager(t, func(id Identity, s Sizes, _ *tls.Config) (driver.Conn, error) {
		if fail.Load() {
			return nil, errors.New("bad options")
		}
		return &fakeConn{id: id, sizes: s}, nil
	})
	fail.Store(true)
	require.ErrorContains(t, m.Resize(Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, time.Millisecond), "open a:9000")
	assert.Equal(t, Sizes{MaxOpenConns: 10, MaxIdleConns: 5}, m.Sizes())
}

func TestManager_ReleaseClosesAfterGrace(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	m := newManager(t, rec.dial)
	c := rec.latest("a:9000")
	m.Release(20 * time.Millisecond)
	assert.False(t, c.closed.Load(), "a consumer that resolved the pool before the swap still finishes on it")
	assert.Eventually(t, func() bool { return c.closed.Load() }, time.Second, 5*time.Millisecond)
}

func TestManager_CloseIsImmediate(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	m := newManager(t, rec.dial)
	require.NoError(t, m.Close())
	assert.True(t, rec.latest("a:9000").closed.Load())
}

// TestOpen_UnreadableCertificateNamesTheKeyAndPath: a certificate file that
// cannot be read, which Validate does not open, is the one thing Open
// refuses.
func TestOpen_UnreadableCertificateNamesTheKeyAndPath(t *testing.T) {
	t.Parallel()
	id := params("b:9000").Identity()
	id.TLS = TLS{Enabled: true, CAFile: "/nowhere/ca.pem"}
	_, err := open(fakeDial, id, Sizes{MaxOpenConns: 1, MaxIdleConns: 1})
	require.ErrorContains(t, err, "clickhouse.tls.ca_file")
	require.ErrorContains(t, err, "/nowhere/ca.pem")
}

func TestTLS_ConfigReadsTheFiles(t *testing.T) {
	t.Parallel()
	ca, cert, key := writeTestPKI(t)
	cfg, err := TLS{Enabled: true, CAFile: ca, CertFile: cert, KeyFile: key, InsecureSkipVerify: true, ServerName: "ch.internal"}.config()
	require.NoError(t, err)
	assert.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1)
	assert.True(t, cfg.InsecureSkipVerify)
	assert.Equal(t, "ch.internal", cfg.ServerName)
	assert.EqualValues(t, tls.VersionTLS12, cfg.MinVersion)
}

func TestTLS_ZeroBlockIsNil(t *testing.T) {
	t.Parallel()
	cfg, err := TLS{}.config()
	require.NoError(t, err)
	assert.Nil(t, cfg, "no material and normal verification: the https hop keeps net/http's defaults")
}

func TestTLS_EnabledAloneUsesSystemRoots(t *testing.T) {
	t.Parallel()
	cfg, err := TLS{Enabled: true}.config()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Nil(t, cfg.RootCAs)
	assert.Empty(t, cfg.Certificates)
}

func TestTLS_UnreadableFilesNameTheKeyAndPath(t *testing.T) {
	t.Parallel()
	_, err := TLS{CAFile: "/nowhere/ca.pem"}.config()
	require.ErrorContains(t, err, "clickhouse.tls.ca_file")
	require.ErrorContains(t, err, "/nowhere/ca.pem")

	ca, _, _ := writeTestPKI(t)
	_, err = TLS{CAFile: ca, CertFile: "/nowhere/client.pem", KeyFile: "/nowhere/client.key"}.config()
	require.ErrorContains(t, err, "clickhouse.tls.cert_file")
	require.ErrorContains(t, err, "/nowhere/client.pem")

	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	_, err = TLS{CAFile: notPEM}.config()
	require.ErrorContains(t, err, "no certificates in "+notPEM)
}

func TestOptions_ReachTheDriver(t *testing.T) {
	t.Parallel()
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	id := params("a:9000").Identity()
	o := options(id, Sizes{MaxOpenConns: 10, MaxIdleConns: 5}, cfg)
	assert.Equal(t, []string{"a:9000"}, o.Addr)
	assert.Equal(t, "db", o.Auth.Database)
	assert.Equal(t, "u", o.Auth.Username)
	assert.Equal(t, "p", o.Auth.Password)
	assert.Equal(t, 10, o.MaxOpenConns)
	assert.Equal(t, 5, o.MaxIdleConns)
	assert.Nil(t, o.TLS, "the native hop stays plain while tls.enabled is false")
	id.TLS.Enabled = true
	assert.Same(t, cfg, options(id, Sizes{}, cfg).TLS)
}

// TestManager_TLSConfigIsTheTuplesAndBuiltOnce: the tls block is part of
// the tuple, so a Manager reads the files once and every resize keeps the
// same config — the HTTP transports keyed on it are never rebuilt.
func TestManager_TLSConfigIsTheTuplesAndBuiltOnce(t *testing.T) {
	t.Parallel()
	ca, _, _ := writeTestPKI(t)
	p := params("a:9000")
	p.HTTPScheme = "https"
	p.TLS = TLS{CAFile: ca}
	m, err := open(fakeDial, p.Identity(), p.Sizes())
	require.NoError(t, err)
	first := m.TLSConfig()
	require.NotNil(t, first, "the material applies to the https hop even with tls.enabled false")
	assert.NotNil(t, first.RootCAs)
	require.NoError(t, m.Resize(Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, time.Millisecond))
	assert.Same(t, first, m.TLSConfig())
	assert.Same(t, first, p.target(m.TLSConfig()).TLS)
}

// member is one tenant asking for the pool at addr, with its own sizes.
func member(id tenant.ID, addr string, open, idle int) Member {
	p := params(addr)
	p.MaxOpenConns, p.MaxIdleConns = open, idle
	return Member{Tenant: id, Params: p}
}

func TestPools_DifferentTuplesGetDifferentPools(t *testing.T) {
	t.Parallel()
	p := newPools(0, fakeDial)
	stale, err := p.Reconcile([]Member{member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5)})
	require.NoError(t, err)
	assert.Equal(t, []tenant.ID{"acme", "globex"}, stale)
	require.NotNil(t, p.For("acme"))
	require.NotNil(t, p.For("globex"))
	assert.NotSame(t, p.For("acme"), p.For("globex"))
	assert.Equal(t, "http://a:8123", p.Target("acme").URL)
	assert.Equal(t, "http://b:8123", p.Target("globex").URL)
	assert.Equal(t, []tenant.ID{"acme"}, p.SharingTables("acme"))
	assert.Nil(t, p.For("initech"), "a tenant on no pool")
	assert.Equal(t, Target{}, p.Target("initech"))
	assert.Nil(t, p.SharingTables("initech"))
}

// TestPools_SharedTupleGetsOnePool: tenants naming the same tuple share one
// Manager, sized to the largest ask in each dimension, each with a Target of
// its own HTTP wiring.
func TestPools_SharedTupleGetsOnePool(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(0, rec.dial)
	globex := member("globex", "a:9000", 8, 8)
	globex.Params.HTTPPort, globex.Params.Headers = 8443, map[string]string{"X-Proxy-Token": "g"}
	_, err := p.Reconcile([]Member{member("acme", "a:9000", 10, 5), globex})
	require.NoError(t, err)
	require.NotNil(t, p.For("acme"))
	assert.Same(t, p.For("acme"), p.For("globex"))
	assert.Equal(t, 1, rec.count("a:9000"), "one pool, opened once")
	assert.Equal(t, Sizes{MaxOpenConns: 10, MaxIdleConns: 8}, p.For("acme").Sizes(), "the largest ask in each dimension")
	assert.Equal(t, "http://a:8123", p.Target("acme").URL)
	assert.Equal(t, "http://a:8443", p.Target("globex").URL)
	assert.Equal(t, map[string]string{"X-Proxy-Token": "g"}, p.Target("globex").Headers)
	assert.Equal(t, []tenant.ID{"acme", "globex"}, p.SharingTables("acme"))
	assert.Equal(t, []tenant.ID{"acme", "globex"}, p.SharingTables("globex"))
}

// TestPools_TupleChangeRepointsWithoutTouchingTheOther is the worked
// example: two tenants on one tuple, one changes its username. It moves to a
// pool of its own; the other keeps the very same Manager, resized down to
// its own ask with the old connection closed after the grace; and both still
// read the same tables, so the fan-out still pairs them.
func TestPools_TupleChangeRepointsWithoutTouchingTheOther(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(40, rec.dial)
	acme, globex := member("acme", "a:9000", 10, 5), member("globex", "a:9000", 20, 8)
	acme.Params.QueryTimeout, globex.Params.QueryTimeout = 30*time.Millisecond, 10*time.Millisecond
	_, err := p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	shared := p.For("acme")
	require.Same(t, shared, p.For("globex"))
	require.Equal(t, Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, shared.Sizes())
	before := rec.latest("a:9000")

	globex.Params.Username = "reporting"
	stale, err := p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	assert.Empty(t, stale, "a new username reads the same tables: not stale")
	assert.Same(t, shared, p.For("acme"), "acme keeps its Manager")
	assert.NotSame(t, shared, p.For("globex"), "globex moved to a pool of its own")
	assert.Equal(t, "reporting", p.For("globex").Identity().Username)
	assert.Equal(t, Sizes{MaxOpenConns: 20, MaxIdleConns: 8}, p.For("globex").Sizes())
	assert.Equal(t, Sizes{MaxOpenConns: 10, MaxIdleConns: 5}, shared.Sizes(), "acme's pool shrinks to acme's ask")
	assert.Equal(t, 3, rec.count("a:9000"), "the shrink and the new pool are two new connections")
	assert.Eventually(t, func() bool { return before.closed.Load() }, time.Second, 5*time.Millisecond, "the replaced connection closes after the longest member timeout")
	assert.Equal(t, []tenant.ID{"acme", "globex"}, p.SharingTables("acme"), "same address and database: still the same tables")
	assert.Equal(t, "reporting", p.Target("globex").Username)
	assert.Equal(t, "u", p.Target("acme").Username)
}

// A tenant moved to another address or database reads other tables, so its
// cache is stale like a readmitted tenant's; the tenant left where it was is
// not, and neither is one whose move keeps the address and database.
func TestPools_MoveToOtherTablesIsStale(t *testing.T) {
	t.Parallel()
	p := newPools(0, fakeDial)
	acme, globex := member("acme", "a:9000", 10, 5), member("globex", "a:9000", 10, 5)
	_, err := p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)

	acme.Params.Addr = "b:9000"
	stale, err := p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	assert.Equal(t, []tenant.ID{"acme"}, stale, "another address")

	globex.Params.Database = "other"
	stale, err = p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	assert.Equal(t, []tenant.ID{"globex"}, stale, "another database")

	stale, err = p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	assert.Empty(t, stale, "nothing moved")
}

func TestPools_TenantGoneReleasesItsPoolAfterGrace(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(0, rec.dial)
	acme := member("acme", "a:9000", 10, 5)
	acme.Params.QueryTimeout = 20 * time.Millisecond
	_, err := p.Reconcile([]Member{acme, member("globex", "b:9000", 10, 5)})
	require.NoError(t, err)
	c := rec.latest("a:9000")
	globexPool := p.For("globex")

	stale, err := p.Reconcile([]Member{member("globex", "b:9000", 10, 5)})
	require.NoError(t, err)
	assert.Empty(t, stale)
	assert.Nil(t, p.For("acme"))
	assert.Same(t, globexPool, p.For("globex"))
	assert.False(t, c.closed.Load(), "released after the grace, not at once")
	assert.Eventually(t, func() bool { return c.closed.Load() }, time.Second, 5*time.Millisecond)

	stale, err = p.Reconcile([]Member{acme, member("globex", "b:9000", 10, 5)})
	require.NoError(t, err)
	assert.Equal(t, []tenant.ID{"acme"}, stale, "back after an absence: its cache is stale")
	require.NotNil(t, p.For("acme"))
	assert.NotSame(t, c, p.For("acme").conn())
}

// TestPools_CeilingRefusesBoot: at boot the pools must fit the ceiling
// together, and a refusal names the sum and the ceiling and leaves nothing
// open.
func TestPools_CeilingRefusesBoot(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(15, rec.dial)
	_, err := p.Reconcile([]Member{member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5)})
	require.Error(t, err)
	assert.ErrorContains(t, err, "not opened for tenant globex")
	assert.ErrorContains(t, err, "clickhouse.max_open_conns 10")
	assert.ErrorContains(t, err, "at 20, above clickhouse.max_total_conns 15")
	assert.NotContains(t, err.Error(), "keeps its previous pool", "a tenant that was on no pool ends up on none")
	assert.Nil(t, p.For("globex"))
	require.NotNil(t, p.For("acme"), "the walk opened what fit")
	require.NoError(t, p.Close())
	assert.True(t, rec.latest("a:9000").closed.Load(), "and boot's refusal closes it")
	assert.Nil(t, p.For("acme"))
}

// TestPools_CeilingRefusesAThirdTupleThenOpensIt: a reload's new tuple over
// the ceiling is not opened and the open pools are untouched; the next
// reload that frees the budget opens it.
func TestPools_CeilingRefusesAThirdTupleThenOpensIt(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(25, rec.dial)
	acme, globex, initech := member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5), member("initech", "c:9000", 10, 5)
	_, err := p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	acmePool, globexPool := p.For("acme"), p.For("globex")

	stale, err := p.Reconcile([]Member{acme, globex, initech})
	require.ErrorContains(t, err, "clickhouse pool c:9000 database db user u not opened for tenant initech")
	assert.ErrorContains(t, err, "at 30, above clickhouse.max_total_conns 25")
	assert.Empty(t, stale)
	assert.Nil(t, p.For("initech"), "its tenant fails closed")
	assert.Same(t, acmePool, p.For("acme"))
	assert.Same(t, globexPool, p.For("globex"))
	assert.Equal(t, 0, rec.count("c:9000"), "not even opened")
	assert.Equal(t, 1, rec.count("a:9000"))

	acme.Params.MaxOpenConns = 5
	stale, err = p.Reconcile([]Member{acme, globex, initech})
	require.NoError(t, err)
	assert.Equal(t, []tenant.ID{"initech"}, stale)
	require.NotNil(t, p.For("initech"))
	assert.Same(t, acmePool, p.For("acme"))
	assert.Equal(t, 5, acmePool.Sizes().MaxOpenConns, "the shrink that made room")
}

// TestPools_RefusedResizeKeepsTheSize: a reload that grows a pool above the
// ceiling is refused and the pool keeps its size — the rest of the tenant's
// Params apply — and the next reload retries.
func TestPools_RefusedResizeKeepsTheSize(t *testing.T) {
	t.Parallel()
	p := newPools(20, fakeDial)
	acme, globex := member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5)
	_, err := p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)

	acme.Params.MaxOpenConns, acme.Params.HTTPPort = 15, 8124
	_, err = p.Reconcile([]Member{acme, globex})
	require.ErrorContains(t, err, "clickhouse pool a:9000 database db user u not resized for tenants [acme]")
	assert.ErrorContains(t, err, "clickhouse.max_open_conns 15")
	assert.ErrorContains(t, err, "at 25, above clickhouse.max_total_conns 20")
	assert.ErrorContains(t, err, "it keeps 10")
	assert.Equal(t, 10, p.For("acme").Sizes().MaxOpenConns)
	assert.Equal(t, "http://a:8124", p.Target("acme").URL, "the HTTP wiring applied all the same")

	globex.Params.MaxOpenConns = 5
	_, err = p.Reconcile([]Member{acme, globex})
	require.NoError(t, err)
	assert.Equal(t, 15, p.For("acme").Sizes().MaxOpenConns, "retried by the next reload")
	assert.Equal(t, 5, p.For("globex").Sizes().MaxOpenConns)
}

// TestPools_SharedGrowthIsOneRefusal: a shared pool whose members' ask grows
// past the ceiling is refused once, naming every member, not once per member.
func TestPools_SharedGrowthIsOneRefusal(t *testing.T) {
	t.Parallel()
	p := newPools(20, fakeDial)
	acme, globex, initech := member("acme", "a:9000", 10, 5), member("globex", "a:9000", 10, 5), member("initech", "b:9000", 10, 5)
	_, err := p.Reconcile([]Member{acme, globex, initech})
	require.NoError(t, err)
	acme.Params.MaxOpenConns, globex.Params.MaxOpenConns = 12, 12
	_, err = p.Reconcile([]Member{acme, globex, initech})
	require.Error(t, err)
	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok)
	assert.Len(t, joined.Unwrap(), 1, "one refusal for the shared pool, not one per member")
	assert.ErrorContains(t, err, "for tenants [acme globex]")
	assert.Equal(t, 10, p.For("acme").Sizes().MaxOpenConns)
}

// TestPools_RefusedMoveKeepsThePreviousPool: a tenant whose new tuple the
// ceiling refuses stays where it was, with the Params it had — #603's
// keep-the-previous-wiring rule — and the next reload retries.
func TestPools_RefusedMoveKeepsThePreviousPool(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(15, rec.dial)
	acme := member("acme", "a:9000", 10, 5)
	acme.Params.QueryTimeout = 10 * time.Millisecond
	_, err := p.Reconcile([]Member{acme})
	require.NoError(t, err)
	before := p.For("acme")
	beforeConn := rec.latest("a:9000")

	moved := member("acme", "b:9000", 20, 5)
	moved.Params.HTTPPort = 8124
	stale, err := p.Reconcile([]Member{moved})
	require.ErrorContains(t, err, "clickhouse pool b:9000 database db user u not opened for tenant acme")
	assert.ErrorContains(t, err, "tenant acme keeps its previous pool a:9000 database db user u")
	assert.Empty(t, stale, "a refused move stays on the tables it had")
	assert.Same(t, before, p.For("acme"))
	assert.Equal(t, "http://a:8123", p.Target("acme").URL, "the previous Params, whole")
	assert.False(t, beforeConn.closed.Load())
	assert.Equal(t, 0, rec.count("b:9000"))

	moved.Params.MaxOpenConns = 15
	_, err = p.Reconcile([]Member{moved})
	require.NoError(t, err)
	assert.Equal(t, "b:9000", p.For("acme").Identity().Addr, "the next reload that fits moves it")
	assert.Equal(t, "http://b:8124", p.Target("acme").URL)
	assert.Eventually(t, func() bool { return beforeConn.closed.Load() }, time.Second, 5*time.Millisecond)
}

// TestPools_MoveAtTheCeilingIsAllowed: the pool a move leaves frees its
// budget for the pool the move opens, so a flat directory at the ceiling can
// still change its address.
func TestPools_MoveAtTheCeilingIsAllowed(t *testing.T) {
	t.Parallel()
	p := newPools(10, fakeDial)
	_, err := p.Reconcile([]Member{member("0", "a:9000", 10, 5)})
	require.NoError(t, err)
	_, err = p.Reconcile([]Member{member("0", "b:9000", 10, 5)})
	require.NoError(t, err)
	assert.Equal(t, "b:9000", p.For("0").Identity().Addr)
}

// TestPools_UnreadableCertificateRefusesTheTuple: a tls block whose files
// cannot be read refuses that tuple like the ceiling does — boot refuses, a
// reload leaves the tenant on its previous pool.
func TestPools_UnreadableCertificateRefusesTheTuple(t *testing.T) {
	t.Parallel()
	p := newPools(0, fakeDial)
	acme := member("acme", "a:9000", 10, 5)
	_, err := p.Reconcile([]Member{acme})
	require.NoError(t, err)
	before := p.For("acme")

	acme.Params.TLS = TLS{Enabled: true, CAFile: "/nowhere/ca.pem"}
	_, err = p.Reconcile([]Member{acme})
	require.ErrorContains(t, err, "clickhouse.tls.ca_file")
	assert.ErrorContains(t, err, "/nowhere/ca.pem")
	assert.ErrorContains(t, err, "keeps its previous pool")
	assert.Same(t, before, p.For("acme"))

	_, err = newPools(0, fakeDial).Reconcile([]Member{acme})
	require.ErrorContains(t, err, "not opened for tenant acme")
}

func TestPools_PingFirstSuccessWins(t *testing.T) {
	t.Parallel()
	t.Run("none open", func(t *testing.T) {
		t.Parallel()
		require.ErrorContains(t, newPools(0, fakeDial).Ping(context.Background()), "no ClickHouse pool is open")
	})
	t.Run("every pool down names each", func(t *testing.T) {
		t.Parallel()
		rec := &dialRecorder{ping: func(context.Context) error { return errors.New("connection refused") }}
		p := newPools(0, rec.dial)
		_, err := p.Reconcile([]Member{member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5)})
		require.NoError(t, err)
		err = p.Ping(context.Background())
		require.Error(t, err)
		assert.ErrorContains(t, err, "a:9000 database db user u: connection refused")
		assert.ErrorContains(t, err, "b:9000 database db user u: connection refused")
	})
	t.Run("one answering pool is ready, whatever the others do", func(t *testing.T) {
		t.Parallel()
		rec := &dialRecorder{ping: func(ctx context.Context) error {
			// A host that does not answer: the driver would wait for its dial
			// timeout; here, until the probe gives up on it.
			<-ctx.Done()
			return ctx.Err()
		}}
		p := newPools(0, rec.dial)
		_, err := p.Reconcile([]Member{member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5)})
		require.NoError(t, err)
		rec.latest("b:9000").ping = nil
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := time.Now()
		require.NoError(t, p.Ping(ctx))
		assert.Less(t, time.Since(start), time.Second, "answered at the first success, not after the hung pool")
	})
}

func TestPools_CloseClosesEveryPool(t *testing.T) {
	t.Parallel()
	rec := &dialRecorder{}
	p := newPools(0, rec.dial)
	_, err := p.Reconcile([]Member{member("acme", "a:9000", 10, 5), member("globex", "b:9000", 10, 5)})
	require.NoError(t, err)
	require.NoError(t, p.Close())
	assert.True(t, rec.latest("a:9000").closed.Load())
	assert.True(t, rec.latest("b:9000").closed.Load())
	assert.Nil(t, p.For("acme"))
	assert.Error(t, p.Ping(context.Background()), "nothing left to ping")
}

// closeCounter is a transport that counts CloseIdleConnections, which
// http.Client forwards to it.
type closeCounter struct{ closed atomic.Int32 }

func (c *closeCounter) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unused")
}
func (c *closeCounter) CloseIdleConnections() { c.closed.Add(1) }

func TestHTTPClients_OneClientPerTLSConfig(t *testing.T) {
	t.Parallel()
	var builds int
	clients := NewHTTPClients(func(*tls.Config) *http.Client {
		builds++
		return &http.Client{Transport: &closeCounter{}}
	})
	plain := Target{}
	first := clients.For(plain)
	assert.Same(t, first, clients.For(plain))
	assert.Equal(t, 1, builds)

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	second := clients.For(Target{TLS: cfg})
	assert.NotSame(t, first, second)
	assert.Equal(t, 2, builds)
	assert.Same(t, second, clients.For(Target{TLS: cfg}))
	assert.Same(t, first, clients.For(plain), "tenants on different configs alternate without rebuilding")
	assert.Equal(t, 2, builds)
}

// TestHTTPClients_HandsEachClientItsOwnTLSConfig: net/http rewrites a
// transport's TLSClientConfig.NextProtos on its first dial, so two clients
// built from the target's own config would race each other and the
// driver, and would put HTTP protocols on the native hop. Each client gets
// a copy; the target's config, the driver's, is never touched.
func TestHTTPClients_HandsEachClientItsOwnTLSConfig(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	shared := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	target := Target{URL: srv.URL, TLS: shared}

	var handed []*tls.Config
	build := func(cfg *tls.Config) *http.Client {
		handed = append(handed, cfg)
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = cfg
		transport.ForceAttemptHTTP2 = true
		return &http.Client{Transport: transport}
	}
	// Both clients exist before either dials: the first dial is what
	// rewrites NextProtos, and it must happen on each client's own copy.
	clients := []*http.Client{NewHTTPClients(build).For(target), NewHTTPClients(build).For(target)}

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
			if !assert.NoError(t, err) {
				return
			}
			resp, err := c.Do(req)
			if assert.NoError(t, err) {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	require.Len(t, handed, 2)
	for _, cfg := range handed {
		assert.NotSame(t, shared, cfg, "each client builds from a copy")
		assert.NotEmpty(t, cfg.NextProtos, "net/http configured HTTP/2 on the copy")
	}
	assert.Nil(t, shared.NextProtos, "the target's config, which the driver dials with, is untouched")
	assert.NotSame(t, handed[0], handed[1])
}
