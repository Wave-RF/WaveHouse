package config

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Each layer's implementation is chosen here, once, at boot: `<layer>.backend`
// names it, and the default is today's in-process one. Settings for one
// backend go in `<layer>.<backend>`, a sub-block read only when that backend
// is selected. Adding a backend is its constant in the layer's list, a case
// in the layer's validate for its sub-block, and a case in the layer's
// wire function in internal/app — nothing else in Validate changes.

// MQBackend names the message queue implementation.
type MQBackend string

// MQEmbedded is the NATS JetStream server inside this process, under
// <data_dir>/nats.
const MQEmbedded MQBackend = "embedded"

// MQNATS is a NATS JetStream cluster the operator runs, holding the streams
// and durables deployments/nats describes; every process naming it shares
// one queue. Its settings are the mq.nats block.
const MQNATS MQBackend = "nats"

var mqBackends = []MQBackend{MQEmbedded, MQNATS}

// MQ selects the message queue. The per-tenant byte budget, mq.max_bytes_gb,
// is a settings-directory key, not this block's.
type MQ struct {
	Backend MQBackend `yaml:"backend" env:"WH_MQ_BACKEND"`
	// NATS is read only when Backend is nats.
	NATS MQNATSConfig `yaml:"nats"`
}

// MQNATSConfig is how to reach the operator's NATS and what topology to
// expect there (mq.NATSConfig, which internal/app builds from it). Secrets
// are file paths only: nothing inline.
type MQNATSConfig struct {
	URLs []string `yaml:"urls" env:"WH_MQ_NATS_URLS"`
	// Name is the connection name the server reports; empty is
	// wavehouse-<hostname>.
	Name string `yaml:"name" env:"WH_MQ_NATS_NAME"`
	// CredsFile, NKeySeedFile and User are exclusive: one way to
	// authenticate, or none.
	CredsFile    string    `yaml:"creds_file" env:"WH_MQ_NATS_CREDS_FILE"`
	NKeySeedFile string    `yaml:"nkey_seed_file" env:"WH_MQ_NATS_NKEY_SEED_FILE"`
	User         string    `yaml:"user" env:"WH_MQ_NATS_USER"`
	PasswordFile string    `yaml:"password_file" env:"WH_MQ_NATS_PASSWORD_FILE"`
	TLS          MQNATSTLS `yaml:"tls"`
	// JSDomain is the JetStream domain, for a leafnode or hub-and-spoke
	// deployment.
	JSDomain       string `yaml:"js_domain" env:"WH_MQ_NATS_JS_DOMAIN"`
	SubjectPrefix  string `yaml:"subject_prefix" env:"WH_MQ_NATS_SUBJECT_PREFIX"`
	Partitions     int    `yaml:"partitions" env:"WH_MQ_NATS_PARTITIONS"`
	IngestConsumer string `yaml:"ingest_consumer" env:"WH_MQ_NATS_INGEST_CONSUMER"`
	// HistoryStream has no subjects to be found by, so it is named; empty is
	// <SUBJECT_PREFIX>_HISTORY, the name the generated manifests give it.
	HistoryStream  string        `yaml:"history_stream" env:"WH_MQ_NATS_HISTORY_STREAM"`
	ConnectTimeout time.Duration `yaml:"connect_timeout" env:"WH_MQ_NATS_CONNECT_TIMEOUT"`
	PublishTimeout time.Duration `yaml:"publish_timeout" env:"WH_MQ_NATS_PUBLISH_TIMEOUT"`
	TopologyWait   time.Duration `yaml:"topology_wait" env:"WH_MQ_NATS_TOPOLOGY_WAIT"`
}

// MQNATSTLS is the client side of TLS to the NATS servers.
type MQNATSTLS struct {
	CAFile         string `yaml:"ca_file" env:"WH_MQ_NATS_TLS_CA_FILE"`
	CertFile       string `yaml:"cert_file" env:"WH_MQ_NATS_TLS_CERT_FILE"`
	KeyFile        string `yaml:"key_file" env:"WH_MQ_NATS_TLS_KEY_FILE"`
	ServerName     string `yaml:"server_name" env:"WH_MQ_NATS_TLS_SERVER_NAME"`
	HandshakeFirst bool   `yaml:"handshake_first" env:"WH_MQ_NATS_TLS_HANDSHAKE_FIRST"`
}

// defaultMQNATS is the mq.nats part of defaults()
// (TestLoad_MQNATSDefaults pins what Load returns to it).
func defaultMQNATS() MQNATSConfig {
	return MQNATSConfig{
		SubjectPrefix: "wh", Partitions: 1, IngestConsumer: "wh-ingest",
		ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: time.Minute,
	}
}

// natsSubjectPrefix is internal/mq's grammar for the prefix: one subject
// token.
var natsSubjectPrefix = regexp.MustCompile(`^[a-z0-9_-]+$`)

func (m MQ) validate() error {
	if err := checkBackend("mq.backend", "WH_MQ_BACKEND", m.Backend, mqBackends); err != nil {
		return err
	}
	if m.Backend == MQNATS {
		return m.NATS.validate()
	}
	return nil
}

func (n MQNATSConfig) validate() error {
	if len(n.URLs) == 0 {
		return errors.New("mq.nats.urls (WH_MQ_NATS_URLS) is required with mq.backend=nats")
	}
	for _, u := range n.URLs {
		if u == "" {
			return fmt.Errorf("mq.nats.urls (WH_MQ_NATS_URLS) %q has an empty entry", strings.Join(n.URLs, ","))
		}
		// A user, password or token in the URL is an inline secret, and would
		// also sidestep the one-way-to-authenticate check below.
		if strings.Contains(u, "@") {
			return errors.New("mq.nats.urls (WH_MQ_NATS_URLS) must not carry credentials (an '@' in a URL): use password_file, nkey_seed_file or creds_file")
		}
	}
	if !natsSubjectPrefix.MatchString(n.SubjectPrefix) {
		return fmt.Errorf("mq.nats.subject_prefix (WH_MQ_NATS_SUBJECT_PREFIX) %q must be one token of [a-z0-9_-]", n.SubjectPrefix)
	}
	if n.Partitions < 1 {
		return fmt.Errorf("mq.nats.partitions (WH_MQ_NATS_PARTITIONS) must be at least 1, got %d", n.Partitions)
	}
	if n.IngestConsumer == "" {
		return errors.New("mq.nats.ingest_consumer (WH_MQ_NATS_INGEST_CONSUMER) must not be empty")
	}
	auth := 0
	for _, set := range []string{n.CredsFile, n.NKeySeedFile, n.User} {
		if set != "" {
			auth++
		}
	}
	if auth > 1 {
		return errors.New("mq.nats: set at most one of creds_file, nkey_seed_file and user")
	}
	if n.PasswordFile != "" && n.User == "" {
		return errors.New("mq.nats.password_file needs mq.nats.user")
	}
	if (n.TLS.CertFile == "") != (n.TLS.KeyFile == "") {
		return errors.New("mq.nats.tls: cert_file and key_file come as a pair")
	}
	for _, d := range []struct {
		key string
		v   time.Duration
	}{
		{"connect_timeout (WH_MQ_NATS_CONNECT_TIMEOUT)", n.ConnectTimeout},
		{"publish_timeout (WH_MQ_NATS_PUBLISH_TIMEOUT)", n.PublishTimeout},
		{"topology_wait (WH_MQ_NATS_TOPOLOGY_WAIT)", n.TopologyWait},
	} {
		if d.v <= 0 {
			return fmt.Errorf("mq.nats.%s must be positive, got %s", d.key, d.v)
		}
	}
	return nil
}

// isSet reports whether the block says anything beyond its defaults (or the
// zero value a Config built without Load carries).
func (n MQNATSConfig) isSet() bool {
	return !reflect.DeepEqual(n, MQNATSConfig{}) && !reflect.DeepEqual(n, defaultMQNATS())
}

// CacheBackend names the query-result cache implementation.
type CacheBackend string

// CacheLocal is the in-process Ristretto cache, sized by cache.l1_max_cost.
const CacheLocal CacheBackend = "local"

var cacheBackends = []CacheBackend{CacheLocal}

// Cache selects and sizes the query-result cache. The time-range bucket
// structured queries normalize to is a settings-directory key
// (query.timestamp_bucket_seconds) — query shaping, not process memory.
type Cache struct {
	Backend   CacheBackend `yaml:"backend" env:"WH_CACHE_BACKEND"`
	L1MaxCost int64        `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST"`
}

func (c Cache) validate() error {
	return checkBackend("cache.backend", "WH_CACHE_BACKEND", c.Backend, cacheBackends)
}

// DedupeBackend names where ingest dedupe keeps the ids it has seen.
type DedupeBackend string

// DedupePebble is the Pebble instance inside this process, under
// <data_dir>/pebble, opened while any tenant has dedupe on.
const DedupePebble DedupeBackend = "pebble"

var dedupeBackends = []DedupeBackend{DedupePebble}

// Dedupe selects the dedupe store. Whether a tenant dedupes, and on which
// field, are settings-directory keys, not this block's.
type Dedupe struct {
	Backend DedupeBackend `yaml:"backend" env:"WH_DEDUPE_BACKEND"`
}

func (d Dedupe) validate() error {
	return checkBackend("dedupe.backend", "WH_DEDUPE_BACKEND", d.Backend, dedupeBackends)
}

// CoordBackend names where leases for singleton work (the sweeper) are held.
type CoordBackend string

// CoordLocal holds leases in this process, which is enough while no other
// process shares its queue.
const CoordLocal CoordBackend = "local"

var coordBackends = []CoordBackend{CoordLocal}

// Coord selects the coordination layer.
type Coord struct {
	Backend CoordBackend `yaml:"backend" env:"WH_COORD_BACKEND"`
}

func (c Coord) validate() error {
	return checkBackend("coord.backend", "WH_COORD_BACKEND", c.Backend, coordBackends)
}

// checkBackend refuses a backend this build has no implementation for,
// listing the ones it has. env repeats the struct tag's literal: a tag can't
// reference a constant.
func checkBackend[T ~string](key, env string, got T, valid []T) error {
	if slices.Contains(valid, got) {
		return nil
	}
	names := make([]string, len(valid))
	for i, v := range valid {
		names[i] = string(v)
	}
	return fmt.Errorf("%s (%s) %q is not a backend this build has; valid: %s", key, env, got, strings.Join(names, ", "))
}

// validateBackends checks every layer's backend and its sub-block.
func (c *Config) validateBackends() error {
	for _, check := range []func() error{c.MQ.validate, c.Cache.validate, c.Dedupe.validate, c.Coord.validate} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// Distributed reports whether the message queue is shared with other
// processes. The embedded one listens on no port, so while it is selected
// every process is an island: nothing else can reach its queue.
func (c *Config) Distributed() bool { return c.MQ.Backend != MQEmbedded }

// NeedsDataDir reports whether a backend this process opens keeps state
// under data_dir, and so whether boot must probe it (CheckDataDir). Only the
// api role opens the dedupe stores.
func (c *Config) NeedsDataDir() bool {
	return c.MQ.Backend == MQEmbedded || (c.Has(RoleAPI) && c.Dedupe.Backend == DedupePebble)
}

// Warnings returns what a valid configuration is still likely to get wrong,
// one line each, for boot to log at WARN. They are not errors: each is
// harmless or correct for a single replica, and one process cannot count its
// replicas.
func (c *Config) Warnings() []string {
	var out []string
	if c.MQ.Backend != MQNATS && c.MQ.NATS.isSet() {
		out = append(out, fmt.Sprintf("mq.nats is set but mq.backend=%s: the block is ignored", c.MQ.Backend))
	}
	if c.MQ.Backend == MQNATS {
		// WARN although it is by design and fires on every nats boot: the
		// key is required in every tenant's config.json, so an operator
		// setting a budget there must hear it does nothing (#613 core G.3).
		out = append(out, "mq.max_bytes_gb (settings directory) is not applied with mq.backend=nats: a tenant's queue is bounded by its partition stream's limits, which are the operator's")
		// Harmless until the sweeper has something to do under nats: its
		// PurgeAcked removes nothing (retention is the operator's), so two
		// replicas sweeping at once cost two no-op calls a minute.
		if c.Coord.Backend == CoordLocal && c.Has(RoleSweeper) {
			out = append(out, "coord.backend=local with mq.backend=nats: every replica running the sweeper holds its own sweeper lease; harmless while the sweeper removes nothing from NATS, and a shared coord.backend will be required once this build has one")
		}
	}
	if !c.Distributed() {
		return out
	}
	// Both are the api role's: a process without it opens neither a cache it
	// reads nor a dedupe store (a split that would need the cache shared is
	// refused, validateTopology).
	if !c.Has(RoleAPI) {
		return out
	}
	if c.Cache.Backend == CacheLocal {
		out = append(out, "cache.backend=local with a shared mq.backend is correct for one replica only: an event ingested on another replica never invalidates this one's cache, so its reads stay stale until the cached entry expires")
	}
	if c.Dedupe.Backend == DedupePebble {
		out = append(out, "dedupe.backend=pebble with a shared mq.backend dedupes per replica only: an id seen by another replica is not seen by this one")
	}
	return out
}
