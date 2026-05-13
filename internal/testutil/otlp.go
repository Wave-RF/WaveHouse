// Package testutil — OTLP gRPC test receiver.
//
// FakeOTLP is an in-process OTLP gRPC server for verifying telemetry export
// in tests. It accepts trace, metric, and log Export RPCs, records the
// received payloads, and exposes counts (and the raw payloads) for assertions.
//
// Usage:
//
//	r := testutil.NewFakeOTLP(t)
//	cfg := observability.ProviderConfig{Endpoint: r.Addr(), ...}
//	shutdown, _ := observability.InitProvider(ctx, "svc", cfg)
//	... emit spans/metrics/logs ...
//	_ = shutdown(ctx)  // forces a final flush
//	assert.Equal(t, expected, r.SpanCount())
//
// Always call shutdown before asserting counts — the OTel SDK batches
// exports and only drains on shutdown (or after the batch timeout).
package testutil

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// FakeOTLP is a single gRPC server that implements the trace, metric, and
// log Export RPCs and captures every received payload. Cleanup is registered
// on the *testing.T automatically.
type FakeOTLP struct {
	addr   string
	server *grpc.Server

	mu      sync.Mutex
	traces  []*tracepb.ResourceSpans
	metrics []*metricspb.ResourceMetrics
	logs    []*logspb.ResourceLogs
}

// NewFakeOTLP binds the receiver to 127.0.0.1 on a random port and starts
// serving. The server is stopped automatically when the test ends.
func NewFakeOTLP(t *testing.T) *FakeOTLP {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("FakeOTLP listen: %v", err)
	}

	r := &FakeOTLP{
		addr:   lis.Addr().String(),
		server: grpc.NewServer(),
	}
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

// Addr returns the listener address (e.g. "127.0.0.1:42891") suitable for
// passing to observability.ProviderConfig.Endpoint.
func (r *FakeOTLP) Addr() string { return r.addr }

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

// Reset clears all captured payloads. Useful between test phases.
func (r *FakeOTLP) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.traces = nil
	r.metrics = nil
	r.logs = nil
}

type fakeTraceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	parent *FakeOTLP
}

func (s *fakeTraceServer) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.parent.mu.Lock()
	s.parent.traces = append(s.parent.traces, req.GetResourceSpans()...)
	s.parent.mu.Unlock()
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type fakeMetricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	parent *FakeOTLP
}

func (s *fakeMetricsServer) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	s.parent.mu.Lock()
	s.parent.metrics = append(s.parent.metrics, req.GetResourceMetrics()...)
	s.parent.mu.Unlock()
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type fakeLogsServer struct {
	collogspb.UnimplementedLogsServiceServer
	parent *FakeOTLP
}

func (s *fakeLogsServer) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.parent.mu.Lock()
	s.parent.logs = append(s.parent.logs, req.GetResourceLogs()...)
	s.parent.mu.Unlock()
	return &collogspb.ExportLogsServiceResponse{}, nil
}
