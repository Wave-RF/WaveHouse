package config

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// The mq.nats and coord.nats blocks and their rules, apart from backends.go
// and config.go as cache_redis.go is: the e2e stack never selects
// mq.backend: nats, so its coverage gate leaves this file to the unit and
// integration suites (.testcoverage.yml).

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
	JSDomain      string `yaml:"js_domain" env:"WH_MQ_NATS_JS_DOMAIN"`
	SubjectPrefix string `yaml:"subject_prefix" env:"WH_MQ_NATS_SUBJECT_PREFIX"`
	Partitions    int    `yaml:"partitions" env:"WH_MQ_NATS_PARTITIONS"`
	// Shards is how many shards each partition has, one durable each.
	Shards int `yaml:"shards" env:"WH_MQ_NATS_SHARDS"`
	// IngestConsumer names the shard durables: <ingest_consumer>-<shard>.
	IngestConsumer string `yaml:"ingest_consumer" env:"WH_MQ_NATS_INGEST_CONSUMER"`
	// HistoryStream is looked up by name; empty is <SUBJECT_PREFIX>_HISTORY,
	// the name the generated manifests give it.
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
		SubjectPrefix: "wh", Partitions: 1, Shards: 32, IngestConsumer: "wh-ingest",
		ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: time.Minute,
	}
}

// MaxNATSShards is the largest mq.nats.shards boot accepts: internal/mq's
// own bound, which config cannot import (internal/app pins the two together).
const MaxNATSShards = 256

// natsSubjectPrefix is internal/mq's grammar for the prefix: one subject
// token.
var natsSubjectPrefix = regexp.MustCompile(`^[a-z0-9_-]+$`)

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
	if n.Shards < 1 || n.Shards > MaxNATSShards {
		return fmt.Errorf("mq.nats.shards (WH_MQ_NATS_SHARDS) must be from 1 to %d, got %d", MaxNATSShards, n.Shards)
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

// trimURLs drops the spaces a comma-separated WH_MQ_NATS_URLS leaves.
func (n *MQNATSConfig) trimURLs() {
	for i, u := range n.URLs {
		n.URLs[i] = strings.TrimSpace(u)
	}
}

// natsWarnings are Warnings' lines about the nats blocks, for every role.
func (c *Config) natsWarnings() []string {
	var out []string
	if c.MQ.Backend != MQNATS && c.MQ.NATS.isSet() {
		out = append(out, fmt.Sprintf("mq.nats is set but mq.backend=%s: the block is ignored", c.MQ.Backend))
	}
	if c.MQ.Backend == MQNATS {
		// WARN although it is by design and fires on every nats boot: the
		// key is required in every tenant's config.json, so an operator
		// setting a budget there must hear it does nothing (#613).
		out = append(out, "mq.max_bytes_gb (settings directory) is not applied with mq.backend=nats: a tenant's queue is bounded by its partition stream's limits, which are the operator's")
	}
	if c.Coord.Backend != CoordNATS && c.Coord.NATS != (CoordNATSConfig{}) {
		out = append(out, fmt.Sprintf("coord.nats is set but coord.backend=%s: the block is ignored", c.Coord.Backend))
	}
	return out
}

// validateNATSTopology is validateTopology's pair of rules for the nats
// backends.
func (c *Config) validateNATSTopology() error {
	if c.Coord.Backend == CoordNATS && c.MQ.Backend != MQNATS {
		return fmt.Errorf("coord.backend=nats with mq.backend=%s: the NATS leases ride mq.nats's connection — set mq.backend=nats, or coord.backend=local", c.MQ.Backend)
	}
	// Under nats the sweeper is not wired (retention is the operator's), so
	// a process that runs only it would run nothing.
	if c.MQ.Backend == MQNATS && len(c.Roles) == 1 && c.Roles[0] == RoleSweeper {
		return fmt.Errorf("roles sweeper with mq.backend=nats: this process would run nothing, since the streams' own retention replaces the sweeper there — remove this process")
	}
	// Ingest processes on a shared queue share its shards through leases, so
	// each needs the shared coordinator.
	if c.MQ.Backend == MQNATS && c.Coord.Backend == CoordLocal && c.Has(RoleIngest) {
		return fmt.Errorf("coord.backend=local with mq.backend=nats in a process running ingest: ingest processes share the queue's shards through shared leases — set coord.backend=nats")
	}
	return nil
}

// CoordNATSConfig names the operator's KV bucket. There is no connection
// block: coord.backend=nats rides mq.nats's connection and credentials.
type CoordNATSConfig struct {
	// Bucket is the KV bucket the leases live in; empty is
	// <mq.nats.subject_prefix>_coord, the name the generated manifests give it.
	Bucket string `yaml:"bucket" env:"WH_COORD_NATS_BUCKET"`
}

// natsBucketName is JetStream's grammar for a KV bucket name.
var natsBucketName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func (n CoordNATSConfig) validate() error {
	if n.Bucket != "" && !natsBucketName.MatchString(n.Bucket) {
		return fmt.Errorf("coord.nats.bucket (WH_COORD_NATS_BUCKET) %q must be a KV bucket name of [a-zA-Z0-9_-]", n.Bucket)
	}
	return nil
}
