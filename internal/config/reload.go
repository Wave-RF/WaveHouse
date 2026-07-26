package config

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"
)

// ReloadResult reports what one reload did: Applied lists hot fields whose
// values changed and now apply; RestartRequired lists restart-only sections
// where the file differs from the config the process booted with — exactly
// what a restart would pick up, re-reported on every reload until one happens.
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
// Every entry needs a matching push in applyHotFields
// (cmd/wavehouse/hotfields.go) — an entry without one is reported as applied
// without taking effect.
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
// applies the hotFields whitelist via apply.
type Reloader struct {
	path       string
	logger     *slog.Logger
	fileBacked bool
	// apply pushes a successfully loaded config's hot fields into the running
	// process. It runs under mu on every successful reload — even a no-change
	// one — so it must be idempotent and fast.
	apply func(*Config)

	mu      sync.Mutex
	boot    *Config // as loaded at process start; restart-only diffs compare against this
	current *Config // last successful load; hot-field diffs compare against this
}

// NewReloader tracks the config loaded at boot from path; apply (optional) runs
// on every successful reload. Whether path exists is captured now, so a later
// Reload can refuse the env-only fallback if the file disappears.
func NewReloader(path string, boot *Config, logger *slog.Logger, apply func(*Config)) *Reloader {
	_, statErr := os.Stat(path)
	return &Reloader{path: path, fileBacked: statErr == nil, boot: boot, current: boot, logger: logger, apply: apply}
}

// Reload re-runs Load, diffs, applies, and reports the classification; a load
// that fails changes nothing.
func (r *Reloader) Reload() (ReloadResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// A file-backed process whose file is gone (renamed, deleted, mount lost)
	// must fail here: falling through to Load would "succeed" from env +
	// defaults and revert every live hot field, the opposite of the
	// previous-config-stays-active promise.
	if r.fileBacked {
		if _, err := os.Stat(r.path); err != nil {
			err = fmt.Errorf("stat config file: %w", err)
			r.logger.Error("config reload failed; previous config still active", "path", r.path, "error", err)
			return ReloadResult{}, err
		}
	}

	next, err := Load(r.path)
	if err != nil {
		r.logger.Error("config reload failed; previous config still active", "path", r.path, "error", err)
		return ReloadResult{}, err
	}

	res := diffConfigs(r.current, r.boot, next)
	if r.apply != nil {
		r.apply(next)
	}
	r.current = next

	r.logger.Info("config reloaded", "path", r.path,
		"applied", res.Applied, "restart_required", res.RestartRequired)
	return res, nil
}

// diffConfigs classifies changes: hot fields against the previous load (what
// this reload newly applies), restart-only sections against boot (what a
// restart would change). Non-nil slices so the JSON renders [] not null.
func diffConfigs(current, boot, next *Config) ReloadResult {
	res := ReloadResult{Applied: []string{}, RestartRequired: []string{}}
	for _, f := range hotFields {
		if !reflect.DeepEqual(f.get(current), f.get(next)) {
			res.Applied = append(res.Applied, f.name)
		}
	}
	for _, s := range restartSections {
		if !reflect.DeepEqual(s.get(boot), s.get(next)) {
			res.RestartRequired = append(res.RestartRequired, s.name)
		}
	}
	return res
}
