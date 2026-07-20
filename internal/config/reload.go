package config

import (
	"log/slog"
	"reflect"
	"sync"
)

// ReloadResult reports what one reload applied (hot fields) and which changed
// sections stay at boot values until restart.
type ReloadResult struct {
	Applied         []string `json:"applied"`
	RestartRequired []string `json:"restart_required"`
}

// fieldProbe names one comparable slice of the config for diffing.
type fieldProbe struct {
	name string
	get  func(*Config) any
}

// hotFields whitelists the only values a Reload applies live: pure values read
// via a swappable snapshot, no lifecycle attached (extension point for #222).
var hotFields = []fieldProbe{
	{"dedupe.id_field", func(c *Config) any { return c.Dedupe.IDField }},
	{"dedupe.require_id", func(c *Config) any { return c.Dedupe.RequireID }},
}

// restartSections covers everything else at section granularity, so an edit to
// a non-reloadable value is reported instead of silently ignored.
var restartSections = []fieldProbe{
	{"data_dir", func(c *Config) any { return c.DataDir }},
	{"server", func(c *Config) any { return c.Server }},
	{"clickhouse", func(c *Config) any { return c.ClickHouse }},
	{"mq", func(c *Config) any { return c.MQ }},
	{"dedupe.enabled", func(c *Config) any { return c.Dedupe.Enabled }},
	{"cache", func(c *Config) any { return c.Cache }},
	{"auth", func(c *Config) any { return c.Auth }},
	{"schema", func(c *Config) any { return c.Schema }},
	{"dlq", func(c *Config) any { return c.DLQ }},
	{"policy", func(c *Config) any { return c.Policy }},
	{"pipes", func(c *Config) any { return c.Pipes }},
	{"otel", func(c *Config) any { return c.OTel }},
	{"prometheus", func(c *Config) any { return c.Prometheus }},
	{"query", func(c *Config) any { return c.Query }},
	{"stream", func(c *Config) any { return c.Stream }},
}

// Reloader re-runs Load on demand (SIGHUP or POST /v1/admin/config/reload) and
// applies the hotFields whitelist via registered hooks.
type Reloader struct {
	path   string
	logger *slog.Logger

	mu       sync.Mutex
	current  *Config
	applyFns []func(*Config)
}

// NewReloader tracks the config loaded at boot from path.
func NewReloader(path string, boot *Config, logger *slog.Logger) *Reloader {
	return &Reloader{path: path, current: boot, logger: logger}
}

// OnReload registers fn to run on every successful reload; fn must be
// idempotent and fast, as it runs under the lock even when nothing changed.
func (r *Reloader) OnReload(fn func(*Config)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyFns = append(r.applyFns, fn)
}

// Reload re-runs Load, diffs against the running config, applies hooks, and
// reports the classification; a load that fails changes nothing.
func (r *Reloader) Reload() (ReloadResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	next, err := Load(r.path)
	if err != nil {
		r.logger.Error("config reload failed; previous config still active", "path", r.path, "error", err)
		return ReloadResult{}, err
	}

	res := diffConfigs(r.current, next)
	for _, fn := range r.applyFns {
		fn(next)
	}
	r.current = next

	r.logger.Info("config reloaded", "path", r.path,
		"applied", res.Applied, "restart_required", res.RestartRequired)
	return res, nil
}

// diffConfigs classifies every change as applied (hot) or restart-only, with
// non-nil slices so the JSON renders [] not null.
func diffConfigs(old, next *Config) ReloadResult {
	res := ReloadResult{Applied: []string{}, RestartRequired: []string{}}
	for _, f := range hotFields {
		if !reflect.DeepEqual(f.get(old), f.get(next)) {
			res.Applied = append(res.Applied, f.name)
		}
	}
	for _, s := range restartSections {
		if !reflect.DeepEqual(s.get(old), s.get(next)) {
			res.RestartRequired = append(res.RestartRequired, s.name)
		}
	}
	return res
}
