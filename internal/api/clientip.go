package api

import (
	"net/http"
	"net/netip"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// forwardedForHeader lists the hops a request passed through, each proxy
// appending the peer it received the request from.
const forwardedForHeader = "X-Forwarded-For"

// trustedProxies are the peers whose X-Forwarded-For and traceparent are
// believed (server.trusted_proxies). Empty trusts no peer.
type trustedProxies []netip.Prefix

func (t trustedProxies) trusts(a netip.Addr) bool {
	if !a.IsValid() {
		return false
	}
	for _, p := range t {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// trustsPeer reports whether the request arrived from a trusted proxy.
func (t trustedProxies) trustsPeer(r *http.Request) bool {
	return t.trusts(peerAddr(r))
}

// clientAddr is the address the request came from. From an untrusted peer
// it is the peer, whatever the headers say. From a trusted proxy it walks
// X-Forwarded-For right to left, the order the proxies appended it in, and
// takes the first hop that is not itself a trusted proxy: every hop to its
// right was written by a proxy that is trusted, and everything to its left
// by the client, which may have forged it. A hop that is not an address
// stops the walk at the nearest proxy, since nothing past it can be read; a
// header of trusted hops alone yields the leftmost of them.
func (t trustedProxies) clientAddr(r *http.Request) netip.Addr {
	client := peerAddr(r)
	if !t.trusts(client) {
		return client
	}
	values := r.Header.Values(forwardedForHeader)
	for i := len(values) - 1; i >= 0; i-- {
		hops := strings.Split(values[i], ",")
		for j := len(hops) - 1; j >= 0; j-- {
			hop := strings.TrimSpace(hops[j])
			if hop == "" {
				continue
			}
			a, ok := parseHop(hop)
			if !ok {
				return client
			}
			client = a
			if !t.trusts(a) {
				return client
			}
		}
	}
	return client
}

// peerAddr is the connection's own address, r.RemoteAddr without its port;
// invalid when RemoteAddr is not an IP address (a Unix socket, say).
func peerAddr(r *http.Request) netip.Addr {
	if ap, err := netip.ParseAddrPort(r.RemoteAddr); err == nil {
		return normalizeAddr(ap.Addr())
	}
	if a, err := netip.ParseAddr(r.RemoteAddr); err == nil {
		return normalizeAddr(a)
	}
	return netip.Addr{}
}

// parseHop reads one X-Forwarded-For entry: an address, optionally with a
// port (some proxies append one) or, for IPv6, in brackets.
func parseHop(hop string) (netip.Addr, bool) {
	if a, err := netip.ParseAddr(hop); err == nil {
		return normalizeAddr(a), true
	}
	if ap, err := netip.ParseAddrPort(hop); err == nil {
		return normalizeAddr(ap.Addr()), true
	}
	if strings.HasPrefix(hop, "[") && strings.HasSuffix(hop, "]") {
		if a, err := netip.ParseAddr(hop[1 : len(hop)-1]); err == nil {
			return normalizeAddr(a), true
		}
	}
	return netip.Addr{}, false
}

// normalizeAddr reads an IPv4-mapped IPv6 address as IPv4, so an IPv4
// prefix matches it, and drops an IPv6 zone, which a prefix never matches.
func normalizeAddr(a netip.Addr) netip.Addr {
	return a.Unmap().WithZone("")
}

// clientAddress records the request's client address in the request scope,
// so its log records and server span carry client.address. It runs after
// requestID, which opens the scope.
func clientAddress(t trustedProxies) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if a := t.clientAddr(r); a.IsValid() {
				observability.SetRequestClient(r.Context(), a.String())
			}
			next.ServeHTTP(w, r)
		})
	}
}
