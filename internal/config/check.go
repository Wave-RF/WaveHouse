package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

// envPrefix is what every WaveHouse environment variable starts with, and the
// only filter that makes an unbound-name check meaningful: the environment
// always carries names that aren't ours.
const envPrefix = "WH_"

// processEnv lists the WH_* names the binary reads outside the Config struct
// (cmd/wavehouse), so UnboundEnv doesn't flag them.
var processEnv = []string{EnvConfig, EnvLogLevel}

// rejectUnboundEnv is the environment half of rejectUnknownKeys: an error
// naming every WH_* variable in environ that nothing reads.
func rejectUnboundEnv(environ []string) error {
	unbound := UnboundEnv(environ)
	if len(unbound) == 0 {
		return nil
	}
	return fmt.Errorf("unbound environment variable(s): %s — a typo, or a key that moved to the settings directory (%s); unset it, or rename it to a key the Config struct declares. On Kubernetes, a Service named wh or wh-* injects WH_SERVICE_HOST, WH_PORT, … into every pod started after it: set enableServiceLinks: false on the pod spec, or rename the Service", strings.Join(unbound, ", "), EnvSettingsDir)
}

// UnboundEnv returns, sorted, every WH_* name in environ (os.Environ() form,
// "KEY=value") that no Config field's env tag binds and the binary doesn't
// read otherwise. Such a name is almost always a typo or a key that moved to
// the settings directory — `WH_DEDUPE_ENABLED=true` left in a compose file
// would otherwise be set, ignored, and believed. Only the WH_ prefix is
// checked, since the environment is shared with whatever launched the process.
func UnboundEnv(environ []string) []string {
	bound := map[string]bool{}
	for _, name := range processEnv {
		bound[name] = true
	}
	collectEnvTags(reflect.TypeFor[Config](), bound)
	var out []string
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, envPrefix) && !bound[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// collectEnvTags records every name in every env tag in t, recursing into
// nested structs. It mirrors cleanenv's tag grammar: an env tag is a
// comma-separated list of names. cleanenv's `env-prefix` tag is NOT
// mirrored — Config must not use it, or a prefixed variable that cleanenv
// reads would be refused here; TestUnboundEnv_MatchesCleanenv pins that.
func collectEnvTags(t reflect.Type, into map[string]bool) {
	for i := range t.NumField() {
		f := t.Field(i)
		for _, name := range strings.Split(f.Tag.Get("env"), ",") {
			if name != "" {
				into[name] = true
			}
		}
		if f.Type.Kind() == reflect.Struct {
			collectEnvTags(f.Type, into)
		}
	}
}

// CheckDataDir reports whether boot can use dir as data_dir: a directory that
// exists must be writable, and one that doesn't (boot creates it — a first
// run, or a missing mount that WarnIfFreshDataDir calls out) must have a
// writable nearest existing ancestor so that creation can succeed. Run before
// anything dials out, so a data_dir the process cannot write to refuses boot
// before ClickHouse discovery rather than after it; a permission denial
// carries the UID-65532 hint, since a bind mount owned by root is the
// typical cause. Writability is probed by creating and removing one temp
// file: the only portable test that exercises the mount's ownership and
// mode. A blank dir — reachable through `WH_DATA_DIR=` — is refused
// outright: the ancestor walk would otherwise probe the working directory
// and pass, and NATS and Pebble state would land under it.
func CheckDataDir(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("data_dir (WH_DATA_DIR) is required: an empty value would scatter NATS and Pebble state under the working directory")
	}
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Boot creates it; check the ancestor below.
	case err != nil:
		return fmt.Errorf("data_dir %s: %w", dir, err)
	case !info.IsDir():
		return fmt.Errorf("data_dir %s is not a directory", dir)
	}

	target := dir
	for {
		if _, err := os.Stat(target); err == nil {
			break
		}
		parent := filepath.Dir(target)
		if parent == target {
			break
		}
		target = parent
	}
	f, err := os.CreateTemp(target, ".wavehouse-datadir-probe-*")
	if err != nil {
		msg := fmt.Sprintf("data_dir %s is not writable", dir)
		if target != dir {
			msg = fmt.Sprintf("data_dir %s does not exist and %s is not writable, so it cannot be created", dir, target)
		}
		if errors.Is(err, fs.ErrPermission) {
			msg += "; " + permissionHint
		}
		return fmt.Errorf("%s: %w", msg, err)
	}
	name := f.Name()
	_ = f.Close()
	return os.Remove(name)
}
