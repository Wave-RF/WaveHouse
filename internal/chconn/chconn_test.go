package chconn

import (
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
	"os"
	"path/filepath"
	"reflect"
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
	return Params{Addr: addr, HTTPPort: 8123, HTTPScheme: "http", Database: "db", Username: "u", Password: "p", QueryTimeout: time.Second, MaxOpenConns: 10, MaxIdleConns: 5}
}

// fakeDial stands in for the driver; the TLS config still comes through
// the real path (TLS.config), which is what the TLS tests exercise.
func fakeDial(p Params, _ *tls.Config) (driver.Conn, error) { return &fakeConn{name: p.Addr}, nil }

func newManager(t *testing.T, dial func(Params, *tls.Config) (driver.Conn, error)) *Manager {
	t.Helper()
	m := &Manager{dial: dial, grace: 10 * time.Millisecond}
	st, err := m.open(params("a:9000"), nil)
	require.NoError(t, err)
	m.cur.Store(st)
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

func TestManager_TargetDerivesHTTPURL(t *testing.T) {
	t.Parallel()
	m := newManager(t, fakeDial)
	assert.Equal(t, Target{URL: "http://a:8123", Username: "u", Password: "p", Database: "db"}, m.Target())
	assert.Equal(t, "db", m.Database())
	assert.Equal(t, time.Second, m.QueryTimeout())
}

func TestManager_ReconfigureSwapsAndClosesOldAfterGrace(t *testing.T) {
	t.Parallel()
	conns := map[string]*fakeConn{}
	m := newManager(t, func(p Params, _ *tls.Config) (driver.Conn, error) {
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
	m := newManager(t, func(p Params, _ *tls.Config) (driver.Conn, error) { dials++; return &fakeConn{name: p.Addr}, nil })
	require.NoError(t, m.Reconfigure(params("a:9000")))
	assert.Equal(t, 1, dials, "identical wiring must not re-dial")
}

// TestManager_ReconfigureDialError pins one of the two ways a swap can
// fail: a malformed option (excluded by settings.Validate) leaves the
// current connection in place. Reachability is never checked here.
func TestManager_ReconfigureDialError(t *testing.T) {
	t.Parallel()
	m := newManager(t, func(p Params, _ *tls.Config) (driver.Conn, error) {
		if p.Addr == "bad:9000" {
			return nil, errors.New("bad options")
		}
		return &fakeConn{name: p.Addr}, nil
	})
	require.ErrorContains(t, m.Reconfigure(params("bad:9000")), "open bad:9000")
	assert.Equal(t, "a:9000", m.Addr())
}

// TestManager_ReconfigureUnreadableCertificateKeepsTheConnection pins the
// other: a certificate file that cannot be read, which Validate does not
// open, errors here and leaves the current connection in place.
func TestManager_ReconfigureUnreadableCertificateKeepsTheConnection(t *testing.T) {
	t.Parallel()
	m := newManager(t, fakeDial)
	p := params("b:9000")
	p.TLS = TLS{Enabled: true, CAFile: "/nowhere/ca.pem"}
	err := m.Reconfigure(p)
	require.ErrorContains(t, err, "open b:9000")
	require.ErrorContains(t, err, "clickhouse.tls.ca_file")
	require.ErrorContains(t, err, "/nowhere/ca.pem")
	assert.Equal(t, "a:9000", m.Addr())
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
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
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
	p := params("a:9000")
	o := options(p, cfg)
	assert.Equal(t, []string{"a:9000"}, o.Addr)
	assert.Equal(t, 10, o.MaxOpenConns)
	assert.Equal(t, 5, o.MaxIdleConns)
	assert.Nil(t, o.TLS, "the native hop stays plain while tls.enabled is false")
	p.TLS.Enabled = true
	assert.Same(t, cfg, options(p, cfg).TLS)
}

func TestManager_TargetCarriesTLSAndHeaders(t *testing.T) {
	t.Parallel()
	ca, _, _ := writeTestPKI(t)
	p := params("a:9000")
	p.HTTPScheme = "https"
	p.TLS = TLS{CAFile: ca}
	p.Headers = map[string]string{"X-Proxy-Token": "abc"}
	m := &Manager{dial: fakeDial}
	st, err := m.open(p, nil)
	require.NoError(t, err)
	m.cur.Store(st)
	tgt := m.Target()
	assert.Equal(t, "https://a:8123", tgt.URL)
	require.NotNil(t, tgt.TLS, "the material applies to the https hop even with tls.enabled false")
	assert.NotNil(t, tgt.TLS.RootCAs)
	assert.Equal(t, map[string]string{"X-Proxy-Token": "abc"}, tgt.Headers)
}

func TestManager_ReconfigureKeepsTLSConfigWhileTheBlockIsUnchanged(t *testing.T) {
	t.Parallel()
	ca, _, _ := writeTestPKI(t)
	p := params("a:9000")
	p.TLS = TLS{Enabled: true, CAFile: ca}
	m := &Manager{dial: fakeDial, grace: 10 * time.Millisecond}
	st, err := m.open(p, nil)
	require.NoError(t, err)
	m.cur.Store(st)
	first := m.Target().TLS
	require.NotNil(t, first)

	q := p
	q.Database = "other"
	require.NoError(t, m.Reconfigure(q))
	assert.Same(t, first, m.Target().TLS, "an unchanged tls block keeps the config, so the HTTP transports are not rebuilt")

	q.TLS.ServerName = "ch.internal"
	require.NoError(t, m.Reconfigure(q))
	assert.NotSame(t, first, m.Target().TLS)
	assert.Equal(t, "ch.internal", m.Target().TLS.ServerName)
}

// TestParams_EqualCoversEveryField mutates each field in turn, so a field
// added to Params without a clause in equal fails here rather than making
// Reconfigure skip a real change.
func TestParams_EqualCoversEveryField(t *testing.T) {
	t.Parallel()
	base := params("a:9000")
	base.Headers = map[string]string{"X-A": "1"}
	require.True(t, base.equal(base))
	rt := reflect.TypeFor[Params]()
	for i := range rt.NumField() {
		changed := base
		f := reflect.ValueOf(&changed).Elem().Field(i)
		switch f.Kind() { //nolint:exhaustive // the default names any kind a new field would add
		case reflect.String:
			f.SetString(f.String() + "x")
		case reflect.Int, reflect.Int64:
			f.SetInt(f.Int() + 1)
		case reflect.Map:
			f.Set(reflect.ValueOf(map[string]string{"X-A": "2"}))
		case reflect.Struct:
			f.Field(0).SetBool(!f.Field(0).Bool())
		default:
			t.Fatalf("field %s: kind %s not covered", rt.Field(i).Name, f.Kind())
		}
		assert.False(t, base.equal(changed), "field %s must take part in equal", rt.Field(i).Name)
	}
}

// closeCounter is a transport that counts CloseIdleConnections, which
// http.Client forwards to it.
type closeCounter struct{ closed atomic.Int32 }

func (c *closeCounter) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unused")
}
func (c *closeCounter) CloseIdleConnections() { c.closed.Add(1) }

func TestHTTPClients_RebuildOnlyWhenTheTLSConfigChanges(t *testing.T) {
	t.Parallel()
	var builds int
	var transports []*closeCounter
	clients := NewHTTPClients(func(*tls.Config) *http.Client {
		builds++
		rt := &closeCounter{}
		transports = append(transports, rt)
		return &http.Client{Transport: rt}
	})
	plain := Target{}
	first := clients.For(plain)
	assert.Same(t, first, clients.For(plain))
	assert.Equal(t, 1, builds)

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	second := clients.For(Target{TLS: cfg})
	assert.NotSame(t, first, second)
	assert.Equal(t, 2, builds)
	assert.Equal(t, int32(1), transports[0].closed.Load(), "the replaced client's idle connections are closed")
	assert.Same(t, second, clients.For(Target{TLS: cfg}))
	assert.Equal(t, int32(0), transports[1].closed.Load())
}
