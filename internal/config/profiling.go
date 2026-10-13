package config

import (
	"fmt"
	"net"
	"strconv"
)

// Profiling serves Go's net/http/pprof endpoints on a listener of its own,
// never on the API router: a profile exposes the process's internals and the
// endpoints take no credential. Off by default; the default address is the
// loopback interface, so turning it on reaches nothing beyond the host (a
// `kubectl port-forward` still reaches it).
type Profiling struct {
	Enabled bool   `yaml:"enabled" env:"WH_PROFILING_ENABLED"`
	Addr    string `yaml:"addr" env:"WH_PROFILING_ADDR"`
}

// validateProfiling checks profiling.addr when profiling is on: a host and a
// port, the port not one another listener of this process already binds.
func (c *Config) validateProfiling() error {
	p := c.Profiling
	if !p.Enabled {
		return nil
	}
	_, portStr, err := net.SplitHostPort(p.Addr)
	if err != nil {
		return fmt.Errorf("profiling.addr %q must be host:port: %w", p.Addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("profiling.addr %q: port out of range 1-65535", p.Addr)
	}
	if port == c.Server.Port {
		return fmt.Errorf("profiling.addr %q collides with server.port (%d)", p.Addr, port)
	}
	if c.Prometheus.Enabled && port == c.Prometheus.Port {
		return fmt.Errorf("profiling.addr %q collides with prometheus.port (%d)", p.Addr, port)
	}
	return nil
}

// profilingWarnings flags a profiling listener reachable beyond the host.
func (c *Config) profilingWarnings() []string {
	if !c.Profiling.Enabled {
		return nil
	}
	host, _, err := net.SplitHostPort(c.Profiling.Addr)
	if err != nil {
		return nil
	}
	if ip := net.ParseIP(host); host == "localhost" || ip != nil && ip.IsLoopback() {
		return nil
	}
	return []string{fmt.Sprintf("profiling.addr %q is not a loopback address: anyone who can reach it can read heap and goroutine dumps and the command line, unauthenticated; firewall it to operators", c.Profiling.Addr)}
}
