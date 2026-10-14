package config

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults_ProfilingDisabledOnLoopback(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.False(t, cfg.Profiling.Enabled)
	assert.Equal(t, "127.0.0.1:6060", cfg.Profiling.Addr)
}

func TestValidate_Profiling(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		addr    string
		prom    Prometheus
		wantErr string
	}{
		{name: "loopback", addr: "127.0.0.1:6060"},
		{name: "every interface", addr: ":6060"},
		{name: "hostname", addr: "localhost:6060"},
		{name: "no port", addr: "127.0.0.1", wantErr: "must be host:port"},
		{name: "empty", addr: "", wantErr: "must be host:port"},
		{name: "port zero", addr: "127.0.0.1:0", wantErr: "out of range"},
		{name: "port too large", addr: "127.0.0.1:65536", wantErr: "out of range"},
		{name: "named port", addr: "127.0.0.1:http", wantErr: "out of range"},
		{name: "server port", addr: "127.0.0.1:8080", wantErr: "collides with server.port"},
		{name: "prometheus sidecar", addr: ":9091", prom: Prometheus{Enabled: true, Path: "/metrics", Port: 9091}, wantErr: "collides with prometheus.port"},
		{name: "prometheus sidecar disabled", addr: ":9091", prom: Prometheus{Path: "/metrics", Port: 9091}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load("nonexistent.yaml")
			require.NoError(t, err)
			cfg.Profiling = Profiling{Enabled: true, Addr: tt.addr}
			cfg.Prometheus = tt.prom
			err = cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidate_ProfilingAddrIgnoredWhenDisabled(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	cfg.Profiling = Profiling{Addr: "not an address"}
	require.NoError(t, cfg.Validate())
}

func TestWarnings_ProfilingBeyondLoopback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		p    Profiling
		warn bool
	}{
		{name: "disabled", p: Profiling{Addr: ":6060"}},
		{name: "loopback v4", p: Profiling{Enabled: true, Addr: "127.0.0.1:6060"}},
		{name: "loopback v6", p: Profiling{Enabled: true, Addr: "[::1]:6060"}},
		{name: "localhost", p: Profiling{Enabled: true, Addr: "localhost:6060"}},
		{name: "every interface", p: Profiling{Enabled: true, Addr: ":6060"}, warn: true},
		{name: "unspecified", p: Profiling{Enabled: true, Addr: "0.0.0.0:6060"}, warn: true},
		{name: "pod address", p: Profiling{Enabled: true, Addr: "10.0.0.7:6060"}, warn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Roles: AllRoles(), Profiling: tt.p}
			found := slices.ContainsFunc(cfg.Warnings(), func(w string) bool { return strings.HasPrefix(w, "profiling.addr") })
			assert.Equal(t, tt.warn, found, "warnings: %v", cfg.Warnings())
		})
	}
}
