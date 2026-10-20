package config

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTrustedProxies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		entries []string
		want    []string
		wantErr string
	}{
		{name: "none", entries: nil, want: []string{}},
		{name: "CIDRs", entries: []string{"10.0.0.0/8", "172.16.0.0/12", "fd00::/8"}, want: []string{"10.0.0.0/8", "172.16.0.0/12", "fd00::/8"}},
		{name: "bare addresses stand for themselves", entries: []string{"10.1.2.3", "2001:db8::1"}, want: []string{"10.1.2.3/32", "2001:db8::1/128"}},
		{name: "host bits masked", entries: []string{"10.1.2.3/8"}, want: []string{"10.0.0.0/8"}},
		{name: "IPv4-mapped read as IPv4", entries: []string{"::ffff:10.0.0.0/104", "::ffff:10.1.2.3"}, want: []string{"10.0.0.0/8", "10.1.2.3/32"}},
		{name: "spaces and empty entries", entries: []string{" 10.0.0.0/8 ", "", "  "}, want: []string{"10.0.0.0/8"}},
		{name: "hostname refused", entries: []string{"proxy.internal"}, wantErr: `"proxy.internal"`},
		{name: "bad prefix length refused", entries: []string{"10.0.0.0/33"}, wantErr: `"10.0.0.0/33"`},
		{name: "zoned address refused", entries: []string{"fe80::1%eth0"}, wantErr: `"fe80::1%eth0"`},
		{name: "IPv4-mapped prefix shorter than /96 refused", entries: []string{"::ffff:0:0/80"}, wantErr: `"::ffff:0:0/80" is an IPv4-mapped prefix`},
		{name: "port refused", entries: []string{"10.0.0.1:80"}, wantErr: `"10.0.0.1:80"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseTrustedProxies(tt.entries)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Contains(t, err.Error(), "server.trusted_proxies")
				return
			}
			require.NoError(t, err)
			strs := make([]string, 0, len(got))
			for _, p := range got {
				strs = append(strs, p.String())
			}
			assert.Equal(t, tt.want, strs)
		})
	}
}

func TestValidate_TrustedProxies(t *testing.T) {
	t.Parallel()
	cfg := defaults()
	cfg.Settings.Dir = t.TempDir()
	cfg.Server.TrustedProxies = []string{"10.0.0.0/8", "nope"}
	require.ErrorContains(t, cfg.Validate(), `"nope"`)

	cfg.Server.TrustedProxies = []string{"10.0.0.0/8", "192.0.2.7"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("192.0.2.7/32")}, cfg.Server.TrustedProxyPrefixes())
	assert.Empty(t, cfg.Warnings())
}

func TestWarnings_TrustEveryPeer(t *testing.T) {
	t.Parallel()
	for all, family := range map[string]string{"0.0.0.0/0": "IPv4", "::/0": "IPv6"} {
		cfg := defaults()
		cfg.Server.TrustedProxies = []string{"10.0.0.0/8", all}
		w := cfg.Warnings()
		require.Len(t, w, 1, all)
		assert.Contains(t, w[0], "every "+family+" peer is trusted")
	}

	// Each /0 trusts only its own family, so both are named.
	cfg := defaults()
	cfg.Server.TrustedProxies = []string{"0.0.0.0/0", "::/0"}
	assert.Len(t, cfg.Warnings(), 2)
}

func TestLoad_TrustedProxiesFromEnv(t *testing.T) {
	t.Setenv("WH_SERVER_TRUSTED_PROXIES", "10.0.0.0/8,172.16.0.0/12")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.0/8", "172.16.0.0/12"}, cfg.Server.TrustedProxies)
}
