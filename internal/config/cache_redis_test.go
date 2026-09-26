package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisBackend is what Load produces for cache.backend=redis with only the
// address set.
func redisBackend() Config {
	c := defaultBackends()
	c.Cache.Backend = CacheRedis
	c.Cache.Redis = CacheRedisConfig{
		Addrs: []string{"redis:6379"}, Mode: RedisStandalone, KeyPrefix: "wh",
		Timeout: 100 * time.Millisecond, DialTimeout: time.Second,
		MaxValueBytes: 1 << 20, CompressMinBytes: 1 << 10, VersionTTL: 168 * time.Hour,
	}
	return c
}

func TestLoad_CacheRedisDefaults(t *testing.T) {
	t.Setenv("WH_CACHE_BACKEND", "redis")
	t.Setenv("WH_CACHE_REDIS_ADDRS", "redis:6379")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	want := redisBackend()
	assert.Equal(t, want.Cache.Redis, cfg.Cache.Redis)
	assert.Empty(t, cfg.Warnings())
}

func TestLoad_CacheRedisFromEnv(t *testing.T) {
	dir := t.TempDir()
	caFile, certFile, keyFile := writeTestPKI(t, dir)
	for k, v := range map[string]string{
		"WH_CACHE_BACKEND":                        "redis",
		"WH_CACHE_REDIS_ADDRS":                    "r1:6379,r2:6379",
		"WH_CACHE_REDIS_MODE":                     "standalone",
		"WH_CACHE_REDIS_USERNAME":                 "wavehouse",
		"WH_CACHE_REDIS_PASSWORD":                 "s3cret",
		"WH_CACHE_REDIS_DB":                       "2",
		"WH_CACHE_REDIS_TLS_ENABLED":              "true",
		"WH_CACHE_REDIS_TLS_CA_FILE":              caFile,
		"WH_CACHE_REDIS_TLS_CERT_FILE":            certFile,
		"WH_CACHE_REDIS_TLS_KEY_FILE":             keyFile,
		"WH_CACHE_REDIS_TLS_SERVER_NAME":          "redis.internal",
		"WH_CACHE_REDIS_KEY_PREFIX":               "staging",
		"WH_CACHE_REDIS_TIMEOUT":                  "250ms",
		"WH_CACHE_REDIS_DIAL_TIMEOUT":             "2s",
		"WH_CACHE_REDIS_MAX_VALUE_BYTES":          "2048",
		"WH_CACHE_REDIS_COMPRESS_MIN_BYTES":       "0",
		"WH_CACHE_REDIS_VERSION_TTL":              "24h",
		"WH_CACHE_REDIS_TLS_INSECURE_SKIP_VERIFY": "false",
	} {
		t.Setenv(k, v)
	}
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, CacheRedisConfig{
		Addrs: []string{"r1:6379", "r2:6379"}, Mode: RedisStandalone,
		Username: "wavehouse", Password: "s3cret", DB: 2,
		TLS: CacheRedisTLS{
			Enabled: true, CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "redis.internal",
		},
		KeyPrefix: "staging", Timeout: 250 * time.Millisecond, DialTimeout: 2 * time.Second,
		MaxValueBytes: 2048, CompressMinBytes: 0, VersionTTL: 24 * time.Hour,
	}, cfg.Cache.Redis)
	tc, err := cfg.Cache.Redis.TLS.Config()
	require.NoError(t, err)
	assert.Equal(t, "redis.internal", tc.ServerName)
	assert.NotNil(t, tc.RootCAs)
	assert.Len(t, tc.Certificates, 1)
}

func TestLoad_CacheRedisFromYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
settings:
  dir: ./settings
cache:
  backend: redis
  redis:
    addrs: ["n1:6379", "n2:6379"]
    mode: cluster
    key_prefix: prod
    timeout: 50ms
    version_ttl: 72h
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	r := cfg.Cache.Redis
	assert.Equal(t, CacheRedis, cfg.Cache.Backend)
	assert.Equal(t, []string{"n1:6379", "n2:6379"}, r.Addrs)
	assert.Equal(t, RedisCluster, r.Mode)
	assert.Equal(t, "prod", r.KeyPrefix)
	assert.Equal(t, 50*time.Millisecond, r.Timeout)
	assert.Equal(t, 72*time.Hour, r.VersionTTL)
	assert.Equal(t, time.Second, r.DialTimeout, "an unset key takes its default")
	assert.Equal(t, 1024, r.CompressMinBytes)
}

// A 0 in the file is kept, and is the backend's "never compress".
func TestLoad_CacheRedisCompressZeroInYAMLIsNever(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
settings:
  dir: ./settings
cache:
  backend: redis
  redis:
    addrs: ["r:6379"]
    compress_min_bytes: 0
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Zero(t, cfg.Cache.Redis.CompressMinBytes)
}

func TestLoad_CacheRedisRefusesUnknownKeys(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
settings:
  dir: ./settings
cache:
  backend: redis
  redis:
    addr: r:6379
    near_cache:
      max_cost: 1
    sentinel_master: mymaster
    tls:
      ca: /x
  memcached:
    addrs: ["m:11211"]
`), 0o600))
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cache.memcached, cache.redis.addr, cache.redis.near_cache, cache.redis.sentinel_master, cache.redis.tls.ca")
}

// The documented env file lists WH_CACHE_REDIS_ADDRS blank: that is no
// address, not an unread block to warn about, and not a valid redis one.
func TestLoad_CacheRedisBlankAddrs(t *testing.T) {
	t.Setenv("WH_CACHE_REDIS_ADDRS", "")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Empty(t, cfg.Warnings())

	t.Setenv("WH_CACHE_BACKEND", "redis")
	_, err = Load("nonexistent.yaml")
	require.ErrorContains(t, err, "cache.backend=redis needs cache.redis.addrs")
}

// A URL-style address carries its password, and a boot error reaches the
// logs: the refusal names the entry, never its value.
func TestLoad_CacheRedisRefusesURLAddrsWithoutEchoingThem(t *testing.T) {
	for _, addr := range []string{
		"redis://default:s3cret@redis:6379",
		"rediss://default:s3cret@redis:6380",
		"default:s3cret@redis:6379",
		"redis://redis:6379",
	} {
		t.Run(addr, func(t *testing.T) {
			t.Setenv("WH_CACHE_BACKEND", "redis")
			t.Setenv("WH_CACHE_REDIS_ADDRS", "ok:6379,"+addr)
			_, err := Load("nonexistent.yaml")
			require.ErrorContains(t, err, "cache.redis.addrs (WH_CACHE_REDIS_ADDRS) entry 2 is a URL or holds credentials")
			assert.Contains(t, err.Error(), "WH_CACHE_REDIS_USERNAME and WH_CACHE_REDIS_PASSWORD")
			assert.NotContains(t, err.Error(), "s3cret")
			assert.NotContains(t, err.Error(), addr)
		})
	}
}

func TestUnboundEnv_KnowsTheCacheRedisVariables(t *testing.T) {
	t.Parallel()
	assert.Empty(t, unboundEnv([]string{
		"WH_CACHE_REDIS_ADDRS=r:6379", "WH_CACHE_REDIS_PASSWORD=x", "WH_CACHE_REDIS_TLS_CA_FILE=/ca.pem",
		"WH_CACHE_REDIS_VERSION_TTL=1h", "WH_CACHE_REDIS_COMPRESS_MIN_BYTES=0",
	}))
	assert.Equal(t, []string{"WH_CACHE_REDIS_ADDR"}, unboundEnv([]string{"WH_CACHE_REDIS_ADDR=r:6379"}))
	assert.Equal(t, []string{"WH_CACHE_REDIS_SENTINEL_MASTER"}, unboundEnv([]string{"WH_CACHE_REDIS_SENTINEL_MASTER=m"}), "no sentinel mode until #656")
}

func TestValidate_CacheRedis(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFile, keyFile := writeTestPKI(t, dir)
	notPEM := filepath.Join(dir, "not.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("hello"), 0o600))
	cases := []struct {
		name string
		set  func(*CacheRedisConfig)
		want string // "" = valid
	}{
		{"defaults", func(*CacheRedisConfig) {}, ""},
		{"no addrs", func(r *CacheRedisConfig) { r.Addrs = nil }, "cache.backend=redis needs cache.redis.addrs (WH_CACHE_REDIS_ADDRS)"},
		{"addr with space", func(r *CacheRedisConfig) { r.Addrs = []string{"a:6379", " b:6379"} }, "no spaces around an address"},
		{"addr without port", func(r *CacheRedisConfig) { r.Addrs = []string{"redis"} }, `cache.redis.addrs (WH_CACHE_REDIS_ADDRS) "redis": want host:port`},
		{"mode", func(r *CacheRedisConfig) { r.Mode = "replica" }, `cache.redis.mode (WH_CACHE_REDIS_MODE) "replica": valid: standalone, cluster`},
		{"sentinel refused", func(r *CacheRedisConfig) { r.Mode = RedisSentinel }, `cache.redis.mode (WH_CACHE_REDIS_MODE) "sentinel" is not supported yet: the cache neither authenticates to the sentinels nor refreshes their topology (https://github.com/Wave-RF/WaveHouse/issues/656)`},
		{"cluster", func(r *CacheRedisConfig) { r.Mode = RedisCluster }, ""},
		{"cluster db", func(r *CacheRedisConfig) { r.Mode, r.DB = RedisCluster, 1 }, "a Redis cluster has only database 0"},
		{"standalone db", func(r *CacheRedisConfig) { r.DB = 3 }, ""},
		{"negative db", func(r *CacheRedisConfig) { r.DB = -1 }, "is negative"},
		{"empty prefix", func(r *CacheRedisConfig) { r.KeyPrefix = "" }, "cache.redis.key_prefix"},
		{"brace prefix", func(r *CacheRedisConfig) { r.KeyPrefix = "a{b}" }, "hash-tag brace"},
		{"zero timeout", func(r *CacheRedisConfig) { r.Timeout = 0 }, "cache.redis.timeout (WH_CACHE_REDIS_TIMEOUT) 0s must be positive"},
		{"negative dial timeout", func(r *CacheRedisConfig) { r.DialTimeout = -time.Second }, "cache.redis.dial_timeout"},
		{"dial timeout at the cap", func(r *CacheRedisConfig) { r.DialTimeout = 2 * time.Second }, ""},
		{"dial timeout over the cap", func(r *CacheRedisConfig) { r.DialTimeout = 2*time.Second + time.Millisecond }, "cache.redis.dial_timeout (WH_CACHE_REDIS_DIAL_TIMEOUT) 2.001s is over 2s"},
		{"short version ttl", func(r *CacheRedisConfig) { r.VersionTTL = time.Second }, "cache.redis.version_ttl (WH_CACHE_REDIS_VERSION_TTL) 1s is under 2s"},
		{"zero max value", func(r *CacheRedisConfig) { r.MaxValueBytes = 0 }, "cache.redis.max_value_bytes"},
		{"compress never", func(r *CacheRedisConfig) { r.CompressMinBytes = 0 }, ""},
		{"compress negative", func(r *CacheRedisConfig) { r.CompressMinBytes = -1 }, "-1 is negative: want a size, or 0 to never compress"},
		{"tls files while off", func(r *CacheRedisConfig) { r.TLS.CAFile = caFile }, "cache.redis.tls.enabled (WH_CACHE_REDIS_TLS_ENABLED) is off"},
		{"tls system roots", func(r *CacheRedisConfig) { r.TLS.Enabled = true }, ""},
		{"tls full", func(r *CacheRedisConfig) {
			r.TLS = CacheRedisTLS{Enabled: true, CAFile: caFile, CertFile: certFile, KeyFile: keyFile}
		}, ""},
		{"tls cert without key", func(r *CacheRedisConfig) { r.TLS = CacheRedisTLS{Enabled: true, CertFile: certFile} }, "cert_file and key_file must be set together"},
		{"tls missing ca", func(r *CacheRedisConfig) {
			r.TLS = CacheRedisTLS{Enabled: true, CAFile: filepath.Join(dir, "missing.pem")}
		}, "cache.redis.tls.ca_file"},
		{"tls ca not pem", func(r *CacheRedisConfig) { r.TLS = CacheRedisTLS{Enabled: true, CAFile: notPEM} }, "no certificates in"},
		{"tls bad pair", func(r *CacheRedisConfig) {
			r.TLS = CacheRedisTLS{Enabled: true, CertFile: certFile, KeyFile: notPEM}
		}, "cache.redis.tls.cert_file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := redisBackend()
			tc.set(&cfg.Cache.Redis)
			err := cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// The redis block is read only when selected: an invalid one under
// backend=local does not refuse boot, it warns that it is ignored.
func TestValidate_CacheRedisIgnoredUnlessSelected(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	cfg.Cache.Redis.Addrs = []string{"no-port"}
	require.NoError(t, cfg.Validate())
	got := cfg.Warnings()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "cache.redis.addrs is set but cache.backend is local")
}

func TestWarnings_CacheRedis(t *testing.T) {
	t.Parallel()
	cfg := redisBackend()
	assert.Empty(t, cfg.Warnings())
	cfg.Cache.Redis.TLS = CacheRedisTLS{Enabled: true, InsecureSkipVerify: true}
	require.NoError(t, cfg.Validate())
	got := cfg.Warnings()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "cache.redis.tls.insecure_skip_verify is on")

	// A shared cache clears the shared-queue warning about a local one.
	cfg = redisBackend()
	cfg.MQ.Backend = "shared"
	got = cfg.Warnings()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "dedupe.backend=pebble")
}

// writeTestPKI writes a self-signed authority and a client certificate it
// signed, returning the three paths cache.redis.tls names.
func writeTestPKI(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "wavehouse"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	write := func(name, typ string, der []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
		return path
	}
	return write("ca.pem", "CERTIFICATE", caDER), write("client.pem", "CERTIFICATE", leafDER), write("client.key", "EC PRIVATE KEY", keyDER)
}
