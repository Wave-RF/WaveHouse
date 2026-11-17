package typelayer

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"

	"github.com/wave-rf/chtypes/go/v2/chtypes"
)

// This file is where a tenant's schemas get their library and their server.
//
// Every schema of a tenant compiles on a chtypes server handle built from the
// tenant's discovered server zone (newServer). A schema on a server binds that
// zone, as a table on the server does: its zone-less DateTime and DateTime64
// columns, every DEFAULT, MATERIALIZED, ALIAS and EPHEMERAL expression,
// timezone(), and every parse and filter that names no session_timezone. So
// no call here names one, and tenants in different zones are served side by
// side from one process.
//
// No chtypes.Setup is made: the image zone is the library's default, UTC, and
// nothing compiled here reads it, since every schema is on a server.

// openLine resolves the library for serverVersion's line. A non-empty cause is
// why the tenant cannot be served: no loadable artifact for the line (the
// SDK's own message, with its CHTYPES_ARTIFACT_* code), or a fetch that failed
// or a cache chtypes cannot use, unwritable or unreadable after boot
// (fetchCause). A failed fetch is retried by the tenant's next Bind.
func (e *Engine) openLine(serverVersion string) (*chtypes.Library, string) {
	line := versionLine(serverVersion)
	if lib, ok := e.libs.Load(line); ok {
		return lib.(*chtypes.Library), ""
	}
	missing := installedCause(e.reg, line)
	if missing != "" && !e.autoFetch {
		return nil, missing
	}
	lib, err := e.open(e.ctx, line)
	if err != nil {
		if missing != "" && installedCause(e.reg, line) != "" {
			return nil, fetchCause(line, err)
		}
		return nil, err.Error()
	}
	e.libs.Store(line, lib)
	return lib, ""
}

// newServer describes the tenant's server to lib: its zone, and nothing else.
// The library validates the zone, so a name it cannot load (ClickHouse code
// 36) is the tenant's cause. Settings stay unset, and Macros nil (unknown):
// every table compiles on neutralEngine, so no Replicated engine reads one.
func newServer(lib *chtypes.Library, tz string) (*chtypes.Server, string) {
	srv, err := lib.NewServer(chtypes.ServerProfile{Timezone: tz})
	if err != nil {
		return nil, fmt.Sprintf("ClickHouse reports server timezone %q, which chtypes cannot serve: %s", tz, err)
	}
	return srv, ""
}

// fetchCause is the cause when the open of a line that was not installed
// failed and left it uninstalled: the fetch failed, or chtypes cannot use its
// cache (CHTYPES_CACHE_UNUSABLE), whether it cannot be written or became
// unreadable after boot, which fails before any fetch starts.
func fetchCause(line string, err error) string {
	if errors.Is(err, chtypes.ErrCacheUnusable) {
		return fmt.Sprintf("chtypes cannot use its cache for ClickHouse %s (%s-%s): %s", line, runtime.GOOS, runtime.GOARCH, err)
	}
	return fmt.Sprintf("chtypes could not fetch the artifact for ClickHouse %s (%s-%s): %s",
		line, runtime.GOOS, runtime.GOARCH, err)
}

// installedCause is the artifact-missing cause when no install record on this
// platform answers line, matched as For matches one (a component prefix of its
// version), and "" when one does. It reads install records and opens nothing.
func installedCause(reg *chtypes.Registry, line string) string {
	installed, err := reg.Installed()
	if err != nil {
		return err.Error()
	}
	platform := runtime.GOOS + "-" + runtime.GOARCH
	if slices.ContainsFunc(installed, func(r chtypes.Resolved) bool {
		return r.Platform == platform && (r.Version == line || strings.HasPrefix(r.Version, line+"."))
	}) {
		return ""
	}
	return fmt.Sprintf("chtypes: no installed artifact for ClickHouse %s (%s), and autofetch is off [%s]",
		line, platform, chtypes.CodeArtifactMissing)
}

// versionLine is the ClickHouse line of a server version ("26.8.15.10" is
// "26.8"). The registry is asked for the line, never the patch: every tenant
// on a line shares one library whichever patch its server runs, and the line
// resolves to the same library for the life of the process. A spelling with no
// line is passed through for the SDK to refuse in its own words.
func versionLine(serverVersion string) string {
	v := strings.TrimPrefix(strings.TrimSpace(serverVersion), "v")
	major, rest, ok := strings.Cut(v, ".")
	if !ok {
		return serverVersion
	}
	minor, _, _ := strings.Cut(rest, ".")
	return major + "." + minor
}
