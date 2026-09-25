package config

import (
	"errors"
	"fmt"
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

var mqBackends = []MQBackend{MQEmbedded}

// MQ selects the message queue. The per-tenant byte budget, mq.max_bytes_gb,
// is a settings-directory key, not this block's.
type MQ struct {
	Backend MQBackend `yaml:"backend" env:"WH_MQ_BACKEND" env-default:"embedded"`
}

func (m MQ) validate() error {
	return checkBackend("mq.backend", "WH_MQ_BACKEND", m.Backend, mqBackends)
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
	Backend   CacheBackend `yaml:"backend" env:"WH_CACHE_BACKEND" env-default:"local"`
	L1MaxCost int64        `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST" env-default:"67108864"`
}

func (c Cache) validate() error {
	return checkBackend("cache.backend", "WH_CACHE_BACKEND", c.Backend, cacheBackends)
}

// DedupeBackend names where ingest dedupe keeps the ids it has seen.
type DedupeBackend string

const (
	// DedupePebble is the Pebble instance inside this process, under
	// <data_dir>/pebble, opened while any tenant has dedupe on. Seen ids are
	// per process.
	DedupePebble DedupeBackend = "pebble"
	// DedupeDynamoDB is one DynamoDB table every tenant and every process
	// shares, configured by dedupe.dynamodb.
	DedupeDynamoDB DedupeBackend = "dynamodb"
)

var dedupeBackends = []DedupeBackend{DedupePebble, DedupeDynamoDB}

// Dedupe selects the dedupe store. Whether a tenant dedupes, on which field,
// and for how long are settings-directory keys, not this block's.
type Dedupe struct {
	Backend DedupeBackend `yaml:"backend" env:"WH_DEDUPE_BACKEND" env-default:"pebble"`
	// Lease is how long a claimed id stays pending while its record is
	// published; a claim its request never settles lapses after it.
	Lease time.Duration `yaml:"lease" env:"WH_DEDUPE_LEASE" env-default:"30s"`
	// ReserveConcurrency bounds the parallel calls one request makes to a
	// remote backend. Pebble ignores it.
	ReserveConcurrency int                  `yaml:"reserve_concurrency" env:"WH_DEDUPE_RESERVE_CONCURRENCY" env-default:"64"`
	DynamoDB           DedupeDynamoDBConfig `yaml:"dynamodb"`
}

// DedupeDynamoDBConfig is the dynamodb backend's block, read only when it is
// selected. Credentials are the AWS SDK's default chain (EKS Pod Identity,
// IRSA, AWS_* variables), never keys here.
type DedupeDynamoDBConfig struct {
	// Table is the shared table; WaveHouse never creates it outside
	// dynamodb-local. Required.
	Table string `yaml:"table" env:"WH_DEDUPE_DYNAMODB_TABLE"`
	// Region overrides the SDK chain's (AWS_REGION).
	Region string `yaml:"region" env:"WH_DEDUPE_DYNAMODB_REGION"`
	// Endpoint points the client at dynamodb-local.
	Endpoint    string        `yaml:"endpoint" env:"WH_DEDUPE_DYNAMODB_ENDPOINT"`
	Timeout     time.Duration `yaml:"timeout" env:"WH_DEDUPE_DYNAMODB_TIMEOUT" env-default:"250ms"`
	MaxAttempts int           `yaml:"max_attempts" env:"WH_DEDUPE_DYNAMODB_MAX_ATTEMPTS" env-default:"3"`
	RetryMode   string        `yaml:"retry_mode" env:"WH_DEDUPE_DYNAMODB_RETRY_MODE" env-default:"standard"`
	// CreateTable creates the table at boot if it is missing. Development
	// only: refused unless Endpoint is set.
	CreateTable bool `yaml:"create_table" env:"WH_DEDUPE_DYNAMODB_CREATE_TABLE" env-default:"false"`
}

func (d Dedupe) validate() error {
	if err := checkBackend("dedupe.backend", "WH_DEDUPE_BACKEND", d.Backend, dedupeBackends); err != nil {
		return err
	}
	if d.Lease < 0 {
		return fmt.Errorf("dedupe.lease (WH_DEDUPE_LEASE) must be >= 0, got %s", d.Lease)
	}
	if d.ReserveConcurrency < 0 {
		return fmt.Errorf("dedupe.reserve_concurrency (WH_DEDUPE_RESERVE_CONCURRENCY) must be >= 0, got %d", d.ReserveConcurrency)
	}
	if d.Backend == DedupeDynamoDB {
		return d.DynamoDB.validate()
	}
	return nil
}

func (d DedupeDynamoDBConfig) validate() error {
	switch {
	case strings.TrimSpace(d.Table) == "":
		return errors.New("dedupe.dynamodb.table (WH_DEDUPE_DYNAMODB_TABLE) is required when dedupe.backend is dynamodb")
	case d.Timeout < 0:
		return fmt.Errorf("dedupe.dynamodb.timeout (WH_DEDUPE_DYNAMODB_TIMEOUT) must be >= 0, got %s", d.Timeout)
	case d.MaxAttempts < 0:
		return fmt.Errorf("dedupe.dynamodb.max_attempts (WH_DEDUPE_DYNAMODB_MAX_ATTEMPTS) must be >= 0, got %d", d.MaxAttempts)
	case d.RetryMode != "" && d.RetryMode != "standard" && d.RetryMode != "adaptive":
		return fmt.Errorf("dedupe.dynamodb.retry_mode (WH_DEDUPE_DYNAMODB_RETRY_MODE) %q: want standard or adaptive", d.RetryMode)
	case d.CreateTable && d.Endpoint == "":
		return errors.New("dedupe.dynamodb.create_table (WH_DEDUPE_DYNAMODB_CREATE_TABLE) is for dynamodb-local only: set dedupe.dynamodb.endpoint, or create the table with your infrastructure code")
	}
	return nil
}

// CoordBackend names where leases for singleton work (the sweeper) are held.
// Nothing reads it yet: the lease layer (#613) wires it.
type CoordBackend string

// CoordLocal holds leases in this process, which is enough while no other
// process shares its queue.
const CoordLocal CoordBackend = "local"

var coordBackends = []CoordBackend{CoordLocal}

// Coord selects the coordination layer.
type Coord struct {
	Backend CoordBackend `yaml:"backend" env:"WH_COORD_BACKEND" env-default:"local"`
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

// embeddedDuplicateWindow mirrors mq.EmbeddedDuplicateWindow, the embedded
// ingest stream's duplicate window (#613 F2). A lease longer than it would let
// the republish of a publish whose outcome was unknown land twice.
const embeddedDuplicateWindow = 2 * time.Minute

// validateBackends checks every layer's backend and its sub-block, then the
// rules that span two layers.
func (c *Config) validateBackends() error {
	for _, check := range []func() error{c.MQ.validate, c.Cache.validate, c.Dedupe.validate, c.Coord.validate} {
		if err := check(); err != nil {
			return err
		}
	}
	if c.MQ.Backend == MQEmbedded && c.Dedupe.Lease > embeddedDuplicateWindow {
		return fmt.Errorf("dedupe.lease (WH_DEDUPE_LEASE) %s exceeds the embedded mq's %s duplicate window: a claim must lapse before the queue forgets the publish it guards", c.Dedupe.Lease, embeddedDuplicateWindow)
	}
	return nil
}

// Distributed reports whether the message queue is shared with other
// processes. The embedded one listens on no port, so while it is selected
// every process is an island: nothing else can reach its queue.
func (c *Config) Distributed() bool { return c.MQ.Backend != MQEmbedded }

// NeedsDataDir reports whether a selected backend keeps state under data_dir,
// and so whether boot must probe it (CheckDataDir).
func (c *Config) NeedsDataDir() bool {
	return c.MQ.Backend == MQEmbedded || c.Dedupe.Backend == DedupePebble
}

// Warnings returns what a valid configuration is still likely to get wrong,
// one line each, for boot to log at WARN. They are not errors because each is
// correct for a single replica, and one process cannot count its replicas.
func (c *Config) Warnings() []string {
	if !c.Distributed() {
		return nil
	}
	var out []string
	if c.Cache.Backend == CacheLocal {
		out = append(out, "cache.backend=local with a shared mq.backend is correct for one replica only: an event ingested on another replica never invalidates this one's cache, so its reads stay stale until the cached entry expires")
	}
	if c.Dedupe.Backend == DedupePebble {
		out = append(out, "dedupe.backend=pebble with a shared mq.backend dedupes per replica only: an id seen by another replica is not seen by this one")
	}
	return out
}
