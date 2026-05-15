// Package testutil — OTLP gRPC test receiver.
//
// FakeOTLP captures trace/metric/log Export RPCs for test assertions. Call
// the InitProvider shutdown before asserting counts — the OTel SDK only
// drains on shutdown or batch timeout. NewFakeOTLPTLS is the TLS variant.
package testutil

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// FakeOTLP is a gRPC server implementing trace/metric/log Export and
// capturing each request's payload + metadata. Cleanup is auto-registered.
type FakeOTLP struct {
	addr      string
	server    *grpc.Server
	tlsConfig *tls.Config // non-nil only when constructed via NewFakeOTLPTLS

	mu      sync.Mutex
	traces  []*tracepb.ResourceSpans
	metrics []*metricspb.ResourceMetrics
	logs    []*logspb.ResourceLogs

	traceHeaders  []metadata.MD
	metricHeaders []metadata.MD
	logHeaders    []metadata.MD
}

// NewFakeOTLP binds the receiver to 127.0.0.1 on a random port and starts
// serving plaintext gRPC. The server is stopped automatically when the test
// ends.
func NewFakeOTLP(t *testing.T) *FakeOTLP {
	t.Helper()
	return newFakeOTLP(t, nil)
}

// NewFakeOTLPTLS is the TLS variant: mints an ephemeral self-signed cert
// (SAN 127.0.0.1) and exposes the matching client *tls.Config via TLSConfig().
func NewFakeOTLPTLS(t *testing.T) *FakeOTLP {
	t.Helper()

	cert, clientCfg := ephemeralTLSPair(t)
	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	return newFakeOTLP(t, &fakeOTLPTLS{server: serverCfg, client: clientCfg})
}

type fakeOTLPTLS struct {
	server *tls.Config
	client *tls.Config
}

func newFakeOTLP(t *testing.T, tlsCfg *fakeOTLPTLS) *FakeOTLP {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("FakeOTLP listen: %v", err)
	}

	var serverOpts []grpc.ServerOption
	r := &FakeOTLP{addr: lis.Addr().String()}
	if tlsCfg != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsCfg.server)))
		r.tlsConfig = tlsCfg.client
	}
	r.server = grpc.NewServer(serverOpts...)

	coltracepb.RegisterTraceServiceServer(r.server, &fakeTraceServer{parent: r})
	colmetricspb.RegisterMetricsServiceServer(r.server, &fakeMetricsServer{parent: r})
	collogspb.RegisterLogsServiceServer(r.server, &fakeLogsServer{parent: r})

	go func() {
		_ = r.server.Serve(lis)
	}()

	t.Cleanup(func() {
		r.server.GracefulStop()
	})

	return r
}

// ephemeralTLSPair mints a one-shot ECDSA self-signed cert valid for 127.0.0.1
// and returns it along with a client tls.Config that trusts only this cert.
func ephemeralTLSPair(t *testing.T) (tls.Certificate, *tls.Config) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("FakeOTLPTLS: generate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("FakeOTLPTLS: serial: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "FakeOTLP"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("FakeOTLPTLS: create cert: %v", err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("FakeOTLPTLS: parse cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return cert, &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"}
}

// Addr returns the listener address (e.g. "127.0.0.1:42891") suitable for
// passing to observability.ProviderConfig.Endpoint.
func (r *FakeOTLP) Addr() string { return r.addr }

// TLSConfig returns the client-side tls.Config that trusts this server's
// ephemeral cert. Returns nil when the server was constructed via the plaintext
// NewFakeOTLP.
func (r *FakeOTLP) TLSConfig() *tls.Config { return r.tlsConfig }

// SpanCount returns the total number of spans received across all RPCs.
// Spans are flattened across resource and scope groupings.
func (r *FakeOTLP) SpanCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rs := range r.traces {
		for _, ss := range rs.GetScopeSpans() {
			n += len(ss.GetSpans())
		}
	}
	return n
}

// MetricCount returns the total number of metric records received, flattened
// across resource and scope groupings.
func (r *FakeOTLP) MetricCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rm := range r.metrics {
		for _, sm := range rm.GetScopeMetrics() {
			n += len(sm.GetMetrics())
		}
	}
	return n
}

// LogCount returns the total number of log records received across all RPCs.
func (r *FakeOTLP) LogCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rl := range r.logs {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	return n
}

// LogCountAtLevel returns the count of log records whose severity number is
// >= minSeverity. OTel severity numbers: DEBUG=5, INFO=9, WARN=13, ERROR=17.
func (r *FakeOTLP) LogCountAtLevel(minSeverity int32) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rl := range r.logs {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				if int32(lr.GetSeverityNumber()) >= minSeverity {
					n++
				}
			}
		}
	}
	return n
}

// LastTraceHeaders returns the gRPC metadata captured from the most recent
// trace Export RPC, or nil if none.
func (r *FakeOTLP) LastTraceHeaders() metadata.MD { return lastMD(&r.mu, r.traceHeaders) }

// LastMetricHeaders returns the gRPC metadata captured from the most recent
// metric Export RPC, or nil if none.
func (r *FakeOTLP) LastMetricHeaders() metadata.MD { return lastMD(&r.mu, r.metricHeaders) }

// LastLogHeaders returns the gRPC metadata captured from the most recent log
// Export RPC, or nil if none.
func (r *FakeOTLP) LastLogHeaders() metadata.MD { return lastMD(&r.mu, r.logHeaders) }

func lastMD(mu *sync.Mutex, slice []metadata.MD) metadata.MD {
	mu.Lock()
	defer mu.Unlock()
	if len(slice) == 0 {
		return nil
	}
	return slice[len(slice)-1]
}

// Reset clears all captured payloads. Useful between test phases.
func (r *FakeOTLP) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.traces = nil
	r.metrics = nil
	r.logs = nil
	r.traceHeaders = nil
	r.metricHeaders = nil
	r.logHeaders = nil
}

type fakeTraceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	parent *FakeOTLP
}

func (s *fakeTraceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.parent.mu.Lock()
	s.parent.traces = append(s.parent.traces, req.GetResourceSpans()...)
	s.parent.traceHeaders = append(s.parent.traceHeaders, md.Copy())
	s.parent.mu.Unlock()
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type fakeMetricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	parent *FakeOTLP
}

func (s *fakeMetricsServer) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.parent.mu.Lock()
	s.parent.metrics = append(s.parent.metrics, req.GetResourceMetrics()...)
	s.parent.metricHeaders = append(s.parent.metricHeaders, md.Copy())
	s.parent.mu.Unlock()
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type fakeLogsServer struct {
	collogspb.UnimplementedLogsServiceServer
	parent *FakeOTLP
}

func (s *fakeLogsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.parent.mu.Lock()
	s.parent.logs = append(s.parent.logs, req.GetResourceLogs()...)
	s.parent.logHeaders = append(s.parent.logHeaders, md.Copy())
	s.parent.mu.Unlock()
	return &collogspb.ExportLogsServiceResponse{}, nil
}
