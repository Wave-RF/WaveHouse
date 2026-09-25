package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Redis deployment modes for cache.redis.mode.
const (
	RedisStandalone = "standalone"
	RedisCluster    = "cluster"
	RedisSentinel   = "sentinel"
)

// CacheRedisConfig configures cache.backend=redis: one Redis-compatible server
// (Redis, Valkey, Dragonfly, ElastiCache, MemoryDB) shared by every process.
// Read only when that backend is selected.
type CacheRedisConfig struct {
	// Addrs are host:port pairs: the server, or seeds for a cluster, or the
	// sentinels.
	Addrs          []string      `yaml:"addrs" env:"WH_CACHE_REDIS_ADDRS"`
	Mode           string        `yaml:"mode" env:"WH_CACHE_REDIS_MODE"`
	SentinelMaster string        `yaml:"sentinel_master" env:"WH_CACHE_REDIS_SENTINEL_MASTER"`
	Username       string        `yaml:"username" env:"WH_CACHE_REDIS_USERNAME"`
	Password       string        `yaml:"password" env:"WH_CACHE_REDIS_PASSWORD"`
	DB             int           `yaml:"db" env:"WH_CACHE_REDIS_DB"`
	TLS            CacheRedisTLS `yaml:"tls"`
	// KeyPrefix leads every key, so deployments can share one server.
	KeyPrefix   string        `yaml:"key_prefix" env:"WH_CACHE_REDIS_KEY_PREFIX"`
	Timeout     time.Duration `yaml:"timeout" env:"WH_CACHE_REDIS_TIMEOUT"`
	DialTimeout time.Duration `yaml:"dial_timeout" env:"WH_CACHE_REDIS_DIAL_TIMEOUT"`
	// MaxValueBytes is the largest value stored, after compression.
	MaxValueBytes int `yaml:"max_value_bytes" env:"WH_CACHE_REDIS_MAX_VALUE_BYTES"`
	// CompressMinBytes is the smallest value zstd-compressed; 0 never
	// compresses.
	CompressMinBytes int `yaml:"compress_min_bytes" env:"WH_CACHE_REDIS_COMPRESS_MIN_BYTES"`
	// VersionTTL is how long a version token outlives its last bump.
	VersionTTL time.Duration `yaml:"version_ttl" env:"WH_CACHE_REDIS_VERSION_TTL"`
}

// defaultCacheRedis is the cache.redis part of defaults(). internal/app's
// TestRedisConfig_FromLoadedDefaults pins it to the backend's own defaults.
func defaultCacheRedis() CacheRedisConfig {
	return CacheRedisConfig{
		Mode: RedisStandalone, KeyPrefix: "wh",
		Timeout: 100 * time.Millisecond, DialTimeout: time.Second,
		MaxValueBytes: 1 << 20, CompressMinBytes: 1 << 10, VersionTTL: 168 * time.Hour,
	}
}

// CacheRedisTLS is cache.redis.tls. The files are paths, read at boot.
type CacheRedisTLS struct {
	Enabled            bool   `yaml:"enabled" env:"WH_CACHE_REDIS_TLS_ENABLED"`
	CAFile             string `yaml:"ca_file" env:"WH_CACHE_REDIS_TLS_CA_FILE"`
	CertFile           string `yaml:"cert_file" env:"WH_CACHE_REDIS_TLS_CERT_FILE"`
	KeyFile            string `yaml:"key_file" env:"WH_CACHE_REDIS_TLS_KEY_FILE"`
	ServerName         string `yaml:"server_name" env:"WH_CACHE_REDIS_TLS_SERVER_NAME"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" env:"WH_CACHE_REDIS_TLS_INSECURE_SKIP_VERIFY"`
}

// hasAddrs reports whether any address is set; a YAML `addrs: [""]` is none.
func (r CacheRedisConfig) hasAddrs() bool {
	return len(r.Addrs) > 1 || len(r.Addrs) == 1 && r.Addrs[0] != ""
}

func (r CacheRedisConfig) validate() error {
	if !r.hasAddrs() {
		return errors.New("cache.backend=redis needs cache.redis.addrs (WH_CACHE_REDIS_ADDRS): the server's host:port, or a cluster's seeds, or the sentinels")
	}
	for _, a := range r.Addrs {
		if strings.TrimSpace(a) != a {
			return fmt.Errorf("cache.redis.addrs (WH_CACHE_REDIS_ADDRS) %q: no spaces around an address", a)
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			return fmt.Errorf("cache.redis.addrs (WH_CACHE_REDIS_ADDRS) %q: want host:port: %w", a, err)
		}
	}
	switch r.Mode {
	case RedisStandalone, RedisCluster, RedisSentinel:
	default:
		return fmt.Errorf("cache.redis.mode (WH_CACHE_REDIS_MODE) %q: valid: %s, %s, %s", r.Mode, RedisStandalone, RedisCluster, RedisSentinel)
	}
	if r.Mode == RedisSentinel && r.SentinelMaster == "" {
		return errors.New("cache.redis.mode=sentinel needs cache.redis.sentinel_master (WH_CACHE_REDIS_SENTINEL_MASTER), the master set name")
	}
	if r.DB < 0 {
		return fmt.Errorf("cache.redis.db (WH_CACHE_REDIS_DB) %d is negative", r.DB)
	}
	if r.Mode == RedisCluster && r.DB != 0 {
		return fmt.Errorf("cache.redis.db (WH_CACHE_REDIS_DB) %d: a Redis cluster has only database 0", r.DB)
	}
	if r.KeyPrefix == "" || strings.ContainsAny(r.KeyPrefix, "{}") {
		return fmt.Errorf("cache.redis.key_prefix (WH_CACHE_REDIS_KEY_PREFIX) %q: want a non-empty prefix without a hash-tag brace", r.KeyPrefix)
	}
	for _, d := range []struct {
		key string
		v   time.Duration
	}{
		{"cache.redis.timeout (WH_CACHE_REDIS_TIMEOUT)", r.Timeout},
		{"cache.redis.dial_timeout (WH_CACHE_REDIS_DIAL_TIMEOUT)", r.DialTimeout},
	} {
		if d.v <= 0 {
			return fmt.Errorf("%s %s must be positive", d.key, d.v)
		}
	}
	if r.VersionTTL < 2*time.Second {
		return fmt.Errorf("cache.redis.version_ttl (WH_CACHE_REDIS_VERSION_TTL) %s is under 2s", r.VersionTTL)
	}
	if r.MaxValueBytes <= 0 {
		return fmt.Errorf("cache.redis.max_value_bytes (WH_CACHE_REDIS_MAX_VALUE_BYTES) %d must be positive", r.MaxValueBytes)
	}
	if r.CompressMinBytes < 0 {
		return fmt.Errorf("cache.redis.compress_min_bytes (WH_CACHE_REDIS_COMPRESS_MIN_BYTES) %d: want a size in bytes, or 0 to never compress", r.CompressMinBytes)
	}
	if _, err := r.TLS.Config(); err != nil {
		return err
	}
	return nil
}

// Config builds the tls.Config the block describes, reading its files, or
// nil when TLS is off. A file set while TLS is off is an error rather than
// a silently plaintext connection.
func (t CacheRedisTLS) Config() (*tls.Config, error) {
	if !t.Enabled {
		if t != (CacheRedisTLS{}) {
			return nil, errors.New("cache.redis.tls: files, server_name or insecure_skip_verify are set but cache.redis.tls.enabled (WH_CACHE_REDIS_TLS_ENABLED) is off")
		}
		return nil, nil
	}
	if (t.CertFile == "") != (t.KeyFile == "") {
		return nil, errors.New("cache.redis.tls: cert_file and key_file must be set together")
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // G402: the operator's cache.redis.tls.insecure_skip_verify, warned about at boot
	}
	if t.CAFile != "" {
		pemBytes, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("cache.redis.tls.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("cache.redis.tls.ca_file: no certificates in %s", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("cache.redis.tls.cert_file: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
