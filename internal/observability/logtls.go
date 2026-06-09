package observability

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"google.golang.org/grpc/credentials"
)

// logExporterTLSOptions works around a bug in the pinned gRPC logs exporter
// (otlploggrpc v0.19.0): it reads OTEL_EXPORTER_OTLP_[LOGS_]CERTIFICATE /
// _CLIENT_CERTIFICATE / _CLIENT_KEY into its config but never wires that
// *tls.Config into the gRPC dial — newGRPCDialOptions only honors the
// programmatic WithTLSCredentials option, otherwise defaulting to
// credentials.NewTLS(nil) (system roots). So a custom CA or mutual-TLS supplied
// via env silently falls back to system roots for the logs signal, while the
// trace/metric exporters (v1.43) apply the same env vars correctly.
//
// We rebuild the credentials from the standard env vars and hand them to the
// log exporter via WithTLSCredentials — the path it does honor — so custom-CA
// and mTLS work uniformly across all three signals. When no custom trust
// material is configured this returns nil, leaving the public-CA path fully
// delegated to the SDK (system roots). Drop this once the exporter honors the
// env TLS config (track upstream before bumping otlploggrpc).
func logExporterTLSOptions() ([]otlploggrpc.Option, error) {
	caFile := firstNonEmptyEnv("OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE", "OTEL_EXPORTER_OTLP_CERTIFICATE")
	clientCertFile := firstNonEmptyEnv("OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE")
	clientKeyFile := firstNonEmptyEnv("OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY", "OTEL_EXPORTER_OTLP_CLIENT_KEY")

	// No custom CA and no client cert → the SDK's default (system roots for an
	// https endpoint, plaintext for http) is already correct; stay delegated.
	if caFile == "" && clientCertFile == "" {
		return nil, nil
	}

	tlsCfg := &tls.Config{}
	if caFile != "" {
		// #nosec G304 -- caFile is operator-configured via OTEL_EXPORTER_OTLP_CERTIFICATE, not request input.
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("otlp logs: read OTEL_EXPORTER_OTLP_CERTIFICATE %q: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("otlp logs: no certificates found in %q", caFile)
		}
		tlsCfg.RootCAs = pool
	}
	if clientCertFile != "" && clientKeyFile != "" {
		pair, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("otlp logs: load client cert/key for mTLS: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}
	return []otlploggrpc.Option{otlploggrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg))}, nil
}

// firstNonEmptyEnv returns the value of the first set, non-empty env var among
// keys (per-signal override before the base var, matching OTLP precedence).
func firstNonEmptyEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}
