package config

import (
	"fmt"
	"net/netip"
	"strings"
)

// ParseTrustedProxies reads server.trusted_proxies: each entry a CIDR, or a
// bare address standing for itself alone. Prefixes are masked, so
// 10.1.2.3/8 is 10.0.0.0/8, and IPv4-mapped IPv6 entries are read as IPv4.
func ParseTrustedProxies(entries []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		p, err := parseTrustedProxy(e)
		if err != nil {
			return nil, fmt.Errorf("server.trusted_proxies: %q is neither a CIDR nor an IP address", e)
		}
		out = append(out, p)
	}
	return out, nil
}

func parseTrustedProxy(e string) (netip.Prefix, error) {
	if !strings.Contains(e, "/") {
		a, err := netip.ParseAddr(e)
		if err != nil || a.Zone() != "" {
			return netip.Prefix{}, fmt.Errorf("not an address")
		}
		a = a.Unmap()
		return netip.PrefixFrom(a, a.BitLen()), nil
	}
	p, err := netip.ParsePrefix(e)
	if err != nil {
		return netip.Prefix{}, err
	}
	if a := p.Addr(); a.Is4In6() && p.Bits() >= 96 {
		p = netip.PrefixFrom(a.Unmap(), p.Bits()-96)
	}
	return p.Masked(), nil
}

// TrustedProxyPrefixes is server.trusted_proxies parsed. Validate has refused an
// entry that does not parse, so a loaded Config never drops one here.
func (s Server) TrustedProxyPrefixes() []netip.Prefix {
	p, _ := ParseTrustedProxies(s.TrustedProxies)
	return p
}

func (c *Config) validateTrustedProxies() error {
	_, err := ParseTrustedProxies(c.Server.TrustedProxies)
	return err
}

// trustedProxyWarnings flags a /0 entry: it trusts every peer, so a client
// reaching the port directly chooses the address it is logged under.
func (c *Config) trustedProxyWarnings() []string {
	for _, p := range c.Server.TrustedProxyPrefixes() {
		if p.Bits() == 0 {
			return []string{"server.trusted_proxies includes " + p.String() + ": every peer is trusted, so a client that reaches server.port directly sets its own client.address through X-Forwarded-For and has its traceparent adopted; list only your proxies' addresses"}
		}
	}
	return nil
}
