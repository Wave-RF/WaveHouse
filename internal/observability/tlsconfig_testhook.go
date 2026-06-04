//go:build integration

package observability

import "crypto/tls"

// SetTLSConfigForTesting injects a client *tls.Config for `https://` OTLP
// endpoints. Only compiled when built with `-tags integration` — the
// production binary doesn't see this method at all, so a TLS override hook
// that only works during tests cannot be called in prod. FakeOTLPTLS uses
// it to trust its self-signed cert.
func (c *ProviderConfig) SetTLSConfigForTesting(cfg *tls.Config) {
	c.tlsConfig = cfg
}
