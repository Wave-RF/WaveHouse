//go:build integration

package mq

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
)

// authFixture starts a JetStream server from opts, connects the operator's
// stand-in with admin, and applies the shipped topology.
func authFixture(t *testing.T, opts *natsserver.Options, admin ...nats.Option) *natsFixture {
	t.Helper()
	opts.Host, opts.Port, opts.NoSigs, opts.NoLog = "127.0.0.1", -1, true, true
	opts.JetStream, opts.StoreDir = true, t.TempDir()
	opts.JetStreamMaxStore, opts.JetStreamMaxMemory = 1<<50, 1<<50
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "nats server not ready")
	t.Cleanup(s.Shutdown)
	f := &natsFixture{server: s, opts: opts}
	url := s.ClientURL()
	if opts.TLS {
		url = "tls://localhost:" + portOf(f)
	}
	nc, err := nats.Connect(url, admin...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	f.admin = js
	f.apply(t, shippedTopology(t))
	return f
}

// connects reports that the broker boots against f with cfg and publishes.
func connects(t *testing.T, url string, cfg NATSConfig) {
	t.Helper()
	cfg.URLs = []string{url}
	cfg.Topology = NATSTopology{Partitions: 4}
	cfg.TopologyWait = 5 * time.Second
	e, err := NewNATS(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	require.NoError(t, e.Publish(t.Context(), Topic{Tenant: "acme", Table: "t"}, []byte("x")))
}

func TestNewNATS_NKeySeed(t *testing.T) {
	t.Parallel()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	seedFile := writeSecret(t, string(seed))

	adminOpt, err := nats.NkeyOptionFromSeed(seedFile)
	require.NoError(t, err)
	f := authFixture(t, &natsserver.Options{Nkeys: []*natsserver.NkeyUser{{Nkey: pub}}}, adminOpt)
	connects(t, f.server.ClientURL(), NATSConfig{NKeySeedFile: seedFile})

	_, err = NewNATS(t.Context(), NATSConfig{URLs: []string{f.server.ClientURL()}, TopologyWait: 300 * time.Millisecond})
	require.ErrorIs(t, err, ErrUnavailable, "no credentials: never connected")
}

func TestNewNATS_CredsFile(t *testing.T) {
	t.Parallel()
	operator, err := nkeys.CreateOperator()
	require.NoError(t, err)
	opub, err := operator.PublicKey()
	require.NoError(t, err)
	oc := jwt.NewOperatorClaims(opub)
	signed, err := oc.Encode(operator)
	require.NoError(t, err)
	oc, err = jwt.DecodeOperatorClaims(signed)
	require.NoError(t, err)

	account, err := nkeys.CreateAccount()
	require.NoError(t, err)
	apub, err := account.PublicKey()
	require.NoError(t, err)
	ac := jwt.NewAccountClaims(apub)
	ac.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	ajwt, err := ac.Encode(operator)
	require.NoError(t, err)
	resolver := &natsserver.MemAccResolver{}
	require.NoError(t, resolver.Store(apub, ajwt))
	// Operator mode wants a system account.
	sys, err := nkeys.CreateAccount()
	require.NoError(t, err)
	spub, err := sys.PublicKey()
	require.NoError(t, err)
	sjwt, err := jwt.NewAccountClaims(spub).Encode(operator)
	require.NoError(t, err)
	require.NoError(t, resolver.Store(spub, sjwt))

	user, err := nkeys.CreateUser()
	require.NoError(t, err)
	upub, err := user.PublicKey()
	require.NoError(t, err)
	ujwt, err := jwt.NewUserClaims(upub).Encode(account)
	require.NoError(t, err)
	seed, err := user.Seed()
	require.NoError(t, err)
	creds, err := jwt.FormatUserConfig(ujwt, seed)
	require.NoError(t, err)
	credsFile := writeSecret(t, string(creds))

	f := authFixture(t, &natsserver.Options{TrustedOperators: []*jwt.OperatorClaims{oc}, AccountResolver: resolver, SystemAccount: spub},
		nats.UserCredentials(credsFile))
	connects(t, f.server.ClientURL(), NATSConfig{CredsFile: credsFile})
}

func TestNewNATS_MutualTLS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ca, caKey := testCA(t, dir)
	serverCert, serverKey := testLeaf(t, dir, "server", ca, caKey, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := testLeaf(t, dir, "client", ca, caKey, x509.ExtKeyUsageClientAuth)
	caFile := filepath.Join(dir, "ca.pem")

	tc, err := natsserver.GenTLSConfig(&natsserver.TLSConfigOpts{CertFile: serverCert, KeyFile: serverKey, CaFile: caFile, Verify: true})
	require.NoError(t, err)
	f := authFixture(t, &natsserver.Options{TLS: true, TLSVerify: true, TLSConfig: tc, TLSTimeout: 5},
		nats.RootCAs(caFile), nats.ClientCert(clientCert, clientKey))
	url := "tls://localhost:" + portOf(f)
	connects(t, url, NATSConfig{TLS: NATSTLS{CAFile: caFile, CertFile: clientCert, KeyFile: clientKey, ServerName: "localhost"}})

	_, err = NewNATS(t.Context(), NATSConfig{URLs: []string{url}, TLS: NATSTLS{CAFile: caFile}, TopologyWait: 300 * time.Millisecond})
	require.ErrorIs(t, err, ErrUnavailable, "no client certificate: never connected")
}

func portOf(f *natsFixture) string {
	_, port, _ := net.SplitHostPort(f.server.Addr().String())
	return port
}

// testCA writes a self-signed CA to dir/ca.pem.
func testCA(t *testing.T, dir string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	writePEM(t, filepath.Join(dir, "ca.pem"), "CERTIFICATE", der)
	ca, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return ca, key
}

// testLeaf writes a localhost certificate signed by ca, and its key.
func testLeaf(t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, usage x509.ExtKeyUsage) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certFile, keyFile = filepath.Join(dir, name+".pem"), filepath.Join(dir, name+"-key.pem")
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600))
}
