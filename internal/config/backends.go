package config

import (
	"fmt"
	"slices"
	"strings"
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
	Backend MQBackend `yaml:"backend" env:"WH_MQ_BACKEND"`
}

func (m MQ) validate() error {
	return checkBackend("mq.backend", "WH_MQ_BACKEND", m.Backend, mqBackends)
}

// CacheBackend names the query-result cache implementation.
type CacheBackend string

const (
	// CacheLocal is the in-process Ristretto cache, sized by
	// cache.l1_max_cost.
	CacheLocal CacheBackend = "local"
	// CacheRedis is one Redis-compatible server shared by every process,
	// configured by cache.redis.
	CacheRedis CacheBackend = "redis"
)

var cacheBackends = []CacheBackend{CacheLocal, CacheRedis}

// Cache selects and sizes the query-result cache. The time-range bucket
// structured queries normalize to is a settings-directory key
// (query.timestamp_bucket_seconds) — query shaping, not process memory.
type Cache struct {
	Backend   CacheBackend     `yaml:"backend" env:"WH_CACHE_BACKEND"`
	L1MaxCost int64            `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST"`
	Redis     CacheRedisConfig `yaml:"redis"`
}

func (c Cache) validate() error {
	if err := checkBackend("cache.backend", "WH_CACHE_BACKEND", c.Backend, cacheBackends); err != nil {
		return err
	}
	if c.Backend == CacheRedis {
		return c.Redis.validate()
	}
	return nil
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
// one line each, for boot to log at WARN. The shared-queue ones are not
// errors because each is correct for a single replica, and one process
// cannot count its replicas.
func (c *Config) Warnings() []string {
	// All are the api role's: a process without it opens no cache it reads
	// and no dedupe store, and a split's Deployments differ only in roles, so
	// the API's warnings cover the others'.
	if !c.Has(RoleAPI) {
		return nil
	}
	var out []string
	if c.Cache.Backend == CacheRedis && c.Cache.Redis.TLS.InsecureSkipVerify {
		out = append(out, "cache.redis.tls.insecure_skip_verify is on: the cache accepts any certificate, so whoever can intercept the connection can read and replace cached query results")
	}
	if c.Cache.Backend != CacheRedis && c.Cache.Redis.hasAddrs() {
		out = append(out, "cache.redis.addrs is set but cache.backend is "+string(c.Cache.Backend)+": the redis block is not read; set cache.backend=redis to share the cache")
	}
	if !c.Distributed() {
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
