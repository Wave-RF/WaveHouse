package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// natsBackends is a valid mq.backend=nats config.
func natsBackends() Config {
	c := defaultBackends()
	c.MQ.Backend = MQNATS
	c.MQ.NATS = defaultMQNATS()
	c.MQ.NATS.URLs = []string{"nats://nats:4222"}
	return c
}

func TestLoad_MQNATSDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, defaultMQNATS(), cfg.MQ.NATS)
	assert.False(t, cfg.MQ.NATS.isSet())
	assert.Empty(t, cfg.Warnings())
}

func TestLoad_MQNATSFromEnv(t *testing.T) {
	for k, v := range map[string]string{ //nolint:gosec // G101: a secret's file path, not the secret
		"WH_MQ_BACKEND":                  "nats",
		"WH_MQ_NATS_URLS":                "nats://a:4222, nats://b:4222",
		"WH_MQ_NATS_NAME":                "wh-api-0",
		"WH_MQ_NATS_USER":                "wavehouse",
		"WH_MQ_NATS_PASSWORD_FILE":       "/var/run/secrets/nats/password",
		"WH_MQ_NATS_TLS_CA_FILE":         "/ca.pem",
		"WH_MQ_NATS_TLS_CERT_FILE":       "/cert.pem",
		"WH_MQ_NATS_TLS_KEY_FILE":        "/key.pem",
		"WH_MQ_NATS_TLS_SERVER_NAME":     "nats.internal",
		"WH_MQ_NATS_TLS_HANDSHAKE_FIRST": "true",
		"WH_MQ_NATS_JS_DOMAIN":           "hub",
		"WH_MQ_NATS_SUBJECT_PREFIX":      "whprod",
		"WH_MQ_NATS_PARTITIONS":          "4",
		"WH_MQ_NATS_INGEST_CONSUMER":     "ingest",
		"WH_MQ_NATS_HISTORY_STREAM":      "HIST",
		"WH_MQ_NATS_CONNECT_TIMEOUT":     "2s",
		"WH_MQ_NATS_PUBLISH_TIMEOUT":     "3s",
		"WH_MQ_NATS_TOPOLOGY_WAIT":       "2m",
	} {
		t.Setenv(k, v)
	}
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, MQNATSConfig{ //nolint:gosec // G101: a secret's file path, not the secret
		URLs: []string{"nats://a:4222", "nats://b:4222"}, Name: "wh-api-0",
		User: "wavehouse", PasswordFile: "/var/run/secrets/nats/password",
		TLS:           MQNATSTLS{CAFile: "/ca.pem", CertFile: "/cert.pem", KeyFile: "/key.pem", ServerName: "nats.internal", HandshakeFirst: true},
		JSDomain:      "hub",
		SubjectPrefix: "whprod", Partitions: 4, IngestConsumer: "ingest", HistoryStream: "HIST",
		ConnectTimeout: 2 * time.Second, PublishTimeout: 3 * time.Second, TopologyWait: 2 * time.Minute,
	}, cfg.MQ.NATS)
	assert.True(t, cfg.Distributed())
}

func TestLoad_MQNATSFromYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mq:
  backend: nats
  nats:
    urls: ["nats://a:4222", "nats://b:4222"]
    creds_file: /var/run/secrets/nats/wavehouse.creds
    tls:
      ca_file: /ca.pem
    partitions: 4
    publish_timeout: 2s
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	want := defaultMQNATS()
	want.URLs = []string{"nats://a:4222", "nats://b:4222"}
	want.CredsFile = "/var/run/secrets/nats/wavehouse.creds"
	want.TLS.CAFile = "/ca.pem"
	want.Partitions = 4
	want.PublishTimeout = 2 * time.Second
	assert.Equal(t, want, cfg.MQ.NATS)
}

// Secrets are file paths only: an inline one is an unknown key or an unbound
// variable, never read.
func TestLoad_MQNATSRefusesInlineSecrets(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mq:
  backend: nats
  nats:
    urls: ["nats://a:4222"]
    user: wavehouse
    password: hunter2
    token: abc
    tls:
      key: inline
`), 0o600))
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mq.nats.password, mq.nats.tls.key, mq.nats.token")

	assert.Equal(t, []string{"WH_MQ_NATS_PASSWORD", "WH_MQ_NATS_TOKEN"}, unboundEnv([]string{
		"WH_MQ_NATS_PASSWORD=hunter2", "WH_MQ_NATS_TOKEN=abc", "WH_MQ_NATS_PASSWORD_FILE=/p",
	}))
}

func TestUnboundEnv_KnowsTheMQNATSVariables(t *testing.T) {
	t.Parallel()
	assert.Empty(t, unboundEnv([]string{
		"WH_MQ_NATS_URLS=x", "WH_MQ_NATS_NAME=x", "WH_MQ_NATS_CREDS_FILE=x", "WH_MQ_NATS_NKEY_SEED_FILE=x",
		"WH_MQ_NATS_USER=x", "WH_MQ_NATS_PASSWORD_FILE=x", "WH_MQ_NATS_TLS_CA_FILE=x", "WH_MQ_NATS_TLS_CERT_FILE=x",
		"WH_MQ_NATS_TLS_KEY_FILE=x", "WH_MQ_NATS_TLS_SERVER_NAME=x", "WH_MQ_NATS_TLS_HANDSHAKE_FIRST=x",
		"WH_MQ_NATS_JS_DOMAIN=x", "WH_MQ_NATS_SUBJECT_PREFIX=x", "WH_MQ_NATS_PARTITIONS=x",
		"WH_MQ_NATS_INGEST_CONSUMER=x", "WH_MQ_NATS_HISTORY_STREAM=x", "WH_MQ_NATS_CONNECT_TIMEOUT=x",
		"WH_MQ_NATS_PUBLISH_TIMEOUT=x", "WH_MQ_NATS_TOPOLOGY_WAIT=x",
	}))
}

func TestValidate_MQNATS(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		set  func(*MQNATSConfig)
		want string
	}{
		{"valid", func(*MQNATSConfig) {}, ""},
		{"user and password file", func(n *MQNATSConfig) { n.User, n.PasswordFile = "wavehouse", "/p" }, ""},
		{"user alone", func(n *MQNATSConfig) { n.User = "wavehouse" }, ""},
		{"mutual tls", func(n *MQNATSConfig) { n.TLS.CertFile, n.TLS.KeyFile = "/c", "/k" }, ""},
		{"no urls", func(n *MQNATSConfig) { n.URLs = nil }, "mq.nats.urls (WH_MQ_NATS_URLS) is required with mq.backend=nats"},
		{"empty url", func(n *MQNATSConfig) { n.URLs = []string{"nats://a:4222", ""} }, "has an empty entry"},
		{"prefix with a dot", func(n *MQNATSConfig) { n.SubjectPrefix = "wh.prod" }, `mq.nats.subject_prefix (WH_MQ_NATS_SUBJECT_PREFIX) "wh.prod" must be one token`},
		{"prefix upper case", func(n *MQNATSConfig) { n.SubjectPrefix = "WH" }, "must be one token"},
		{"empty prefix", func(n *MQNATSConfig) { n.SubjectPrefix = "" }, "must be one token"},
		{"no partitions", func(n *MQNATSConfig) { n.Partitions = 0 }, "mq.nats.partitions (WH_MQ_NATS_PARTITIONS) must be at least 1, got 0"},
		{"no ingest consumer", func(n *MQNATSConfig) { n.IngestConsumer = "" }, "mq.nats.ingest_consumer"},
		{"creds and user", func(n *MQNATSConfig) { n.CredsFile, n.User = "/c", "u" }, "set at most one of creds_file, nkey_seed_file and user"},
		{"creds and nkey", func(n *MQNATSConfig) { n.CredsFile, n.NKeySeedFile = "/c", "/n" }, "set at most one"},
		{"password without user", func(n *MQNATSConfig) { n.PasswordFile = "/p" }, "mq.nats.password_file needs mq.nats.user"},
		{"cert without key", func(n *MQNATSConfig) { n.TLS.CertFile = "/c" }, "cert_file and key_file come as a pair"},
		{"key without cert", func(n *MQNATSConfig) { n.TLS.KeyFile = "/k" }, "come as a pair"},
		{"zero connect timeout", func(n *MQNATSConfig) { n.ConnectTimeout = 0 }, "mq.nats.connect_timeout (WH_MQ_NATS_CONNECT_TIMEOUT) must be positive"},
		{"negative publish timeout", func(n *MQNATSConfig) { n.PublishTimeout = -time.Second }, "mq.nats.publish_timeout (WH_MQ_NATS_PUBLISH_TIMEOUT) must be positive"},
		{"zero topology wait", func(n *MQNATSConfig) { n.TopologyWait = 0 }, "mq.nats.topology_wait"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := natsBackends()
			tc.set(&cfg.MQ.NATS)
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

// The block is checked only when it is selected: under embedded it is not
// read, so it cannot refuse boot, and boot says it is ignored.
func TestValidate_MQNATSIgnoredUnderEmbedded(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	cfg.MQ.NATS = defaultMQNATS()
	cfg.MQ.NATS.Partitions = 0
	require.NoError(t, cfg.Validate())
	assert.Equal(t, []string{"mq.nats is set but mq.backend=embedded: the block is ignored"}, cfg.Warnings())
}

// On a shared queue every role split boots except the one the local cache
// cannot serve (rule 5, until a shared cache exists). There is no rule 4 yet:
// coord.backend=local is a warning.
func TestValidate_SplitsBootOnNATS(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		roles []Role
		want  string
	}{
		{AllRoles(), ""},
		{[]Role{RoleAPI, RoleIngest}, ""},
		{[]Role{RoleSweeper}, ""},
		{[]Role{RoleAPI}, "roles api with cache.backend=local"},
		{[]Role{RoleIngest, RoleSweeper}, "roles ingest,sweeper with cache.backend=local"},
	} {
		cfg := natsBackends()
		cfg.Roles = tc.roles
		err := cfg.Validate()
		if tc.want == "" {
			assert.NoError(t, err, "roles %v", tc.roles)
			continue
		}
		require.Error(t, err, "roles %v", tc.roles)
		assert.Contains(t, err.Error(), tc.want)
	}
}

func TestWarnings_MQNATS(t *testing.T) {
	t.Parallel()
	const (
		maxBytes = "mq.max_bytes_gb (settings directory) is not applied with mq.backend=nats"
		coord    = "coord.backend=local with mq.backend=nats"
		cache    = "cache.backend=local"
		dedupe   = "dedupe.backend=pebble"
	)
	warnings := func(roles ...Role) []string {
		cfg := natsBackends()
		cfg.Roles = roles
		require.NoError(t, cfg.Validate())
		var got []string
		for _, w := range cfg.Warnings() {
			for _, key := range []string{maxBytes, coord, cache, dedupe} {
				if strings.HasPrefix(w, key) {
					got = append(got, key)
				}
			}
		}
		require.Len(t, got, len(cfg.Warnings()), "every warning is one of the known ones")
		return got
	}
	assert.Equal(t, []string{maxBytes, coord, cache, dedupe}, warnings(AllRoles()...))
	assert.Equal(t, []string{maxBytes, cache, dedupe}, warnings(RoleAPI, RoleIngest), "no sweeper, no lease to share")
	assert.Equal(t, []string{maxBytes, coord}, warnings(RoleSweeper), "no api, no cache or dedupe store")
}
