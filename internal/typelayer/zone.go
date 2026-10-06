package typelayer

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wave-rf/chtypes/go/chtypes"

	// knownZone checks names against Go's zone database; embed it so that is
	// deliberate and the host needs no tzdata.
	_ "time/tzdata"
)

// This file is the one place the zone rule lives.
//
// chtypes loads every library under one image zone per process (chtypes.Setup,
// before the first open), and that zone is what a zone-less DateTime column
// and the functions over it read in. A call's own session_timezone governs the
// rest: how a body's rows parse into instants, and a filter's WHERE, which
// answers in the zone the filter was created in.
//
// The image zone is the server zone of the first tenant chtypes serves, so a
// single-zone deployment is exact. A tenant that cannot be served commits
// nothing (a zone name not recognised, no artifact for its line, or a first
// open that fails), so the next servable tenant sets the zone. Every other tenant's calls carry its server zone as
// session_timezone, the same zone on a filter's create as on every parse it
// is evaluated against (Table.zoneOpts): chtypes declines a batch whose
// filter was created in another zone. What session_timezone does not reach is an expression over a
// zone-less DateTime column, so such a table is unavailable for that tenant
// (zoneCause) until chtypes binds a zone per schema (Wave-RF/chtypes#419).

// image is the committed image zone, "" until the first open under it
// succeeds; mu serializes that first open.
var image struct {
	mu   sync.Mutex
	zone string
}

// openLine resolves the library for serverVersion's line. A non-empty cause is
// why the tenant cannot be served: a zone name WaveHouse does not recognise,
// no loadable artifact for the line (the SDK's own message, with its
// CHTYPES_ARTIFACT_* code), a fetch that failed or a cache chtypes cannot use,
// unwritable or unreadable after boot (fetchCause), or a first open that fails
// in the zone (see openFirst). Only the first open asks chtypes about the zone: once an image
// zone is committed, a tenant in a zone chtypes cannot load gets no cause
// here, and chtypes refuses each of its calls instead (see knownZone). A
// failed fetch is retried by the tenant's next Bind.
func (e *Engine) openLine(serverVersion, tz string) (*chtypes.Library, string) {
	if !knownZone(tz) {
		return nil, fmt.Sprintf("ClickHouse reports server timezone %q, which is not a zone name WaveHouse recognises, "+
			"so no row of this tenant can be read in it", tz)
	}
	line := versionLine(serverVersion)
	if lib, ok := e.libs.Load(line); ok {
		return lib.(*chtypes.Library), ""
	}
	if lib, cause, first := e.openFirst(line, tz); first {
		return lib, cause
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

// openFirst is openLine while no image zone is committed (first is false once
// one is). The zone is committed only once a library has opened under it.
// With autofetch off, a tenant with no installed artifact on this platform for
// its line never reaches Setup, and an open that fails leaves the image
// uncommitted, the tenant unavailable. chtypes 1.0.3 unlocks Setup after any
// open that fails before the library loads (the artifact, the fetch or the
// zone), so the next servable tenant sets its own zone. If Setup ever refuses,
// that fails closed: opening anyway would load the library in the held zone.
//
// With autofetch on, a missing line is fetched by the open itself, after
// Setup: chtypes has no call that installs without opening.
func (e *Engine) openFirst(line, tz string) (lib *chtypes.Library, cause string, first bool) {
	image.mu.Lock()
	defer image.mu.Unlock()
	if image.zone != "" {
		return nil, "", false
	}
	missing := installedCause(e.reg, line)
	if missing != "" && !e.autoFetch {
		return nil, missing, true
	}
	if err := chtypes.Setup(chtypes.SetupOptions{Timezone: tz}); err != nil {
		return nil, fmt.Sprintf("ClickHouse reports server timezone %q, which chtypes refused as this process's "+
			"image zone: %s", tz, err), true
	}
	lib, err := e.open(e.ctx, line)
	var artifact *chtypes.ArtifactError
	switch {
	case err != nil && missing != "" && installedCause(e.reg, line) != "":
		return nil, fetchCause(line, err), true
	case errors.As(err, &artifact):
		return nil, err.Error(), true
	case err != nil:
		return nil, fmt.Sprintf("ClickHouse reports server timezone %q, and chtypes could not open a library in it: %s",
			tz, err), true
	}
	image.zone = tz
	e.libs.Store(line, lib)
	slog.Info("chtypes image zone committed", "zone", tz)
	return lib, "", true
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

// knownZone reports whether tz is a zone name Go's time package knows, which
// turns a garbage name away before it reaches chtypes. chtypes carries its own
// zone data and Setup does not validate a name, so a zone Go knows and chtypes
// does not passes here. As the first open, it fails and commits nothing. After
// an image zone is committed, its tenant is bound, and chtypes refuses every
// parse and filter compile in that zone (ClickHouse code 36), so no record of
// it is accepted. Refusing it at bind would take chtypes saying which zones it
// can load.
func knownZone(tz string) bool {
	if tz == "" || tz == "Local" {
		return false
	}
	_, err := time.LoadLocation(tz)
	return err == nil
}

// imageZone is the committed image zone, "" before the first open.
func imageZone() string {
	image.mu.Lock()
	defer image.mu.Unlock()
	return image.zone
}

// sessionZone is the session_timezone a table in server zone tz passes on
// every call: none for the image zone, which every call already reads in, so
// a single-zone deployment never passes one.
func sessionZone(tz string) string {
	if tz == imageZone() {
		return ""
	}
	return tz
}

// zoneCause is why a table whose calls carry session zone session cannot be
// served, "" when it can. With no session zone everything reads in the
// server's zone. With one, an expression over a zone-less DateTime column
// computes in the image zone where the server computes in its own, so a table
// declaring such a column AND an expression is refused. A literal a role
// injects counts only on a zone-less DateTime column, and a type's default
// value is an instant, not a reading. No CHECK constraint is ever declared
// here (discovery carries none).
func zoneCause(session string, cols []colDecl) string {
	if session == "" {
		return ""
	}
	zoneless, expr := "", false
	for _, c := range cols {
		dt := zonelessDateTime(c.Type)
		if dt && zoneless == "" {
			zoneless = c.Name
		}
		switch {
		case c.DefaultExpression == "", c.origin == exprTypeDefault:
		case c.origin == exprLiteral:
			expr = expr || dt
		default:
			expr = true
		}
	}
	if zoneless == "" || !expr {
		return ""
	}
	return fmt.Sprintf(
		"ClickHouse reports server timezone %q, but this process reads zone-less DateTime columns in %q, and this table "+
			"declares one (%q) alongside DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expressions, which would compute in the wrong zone; "+
			"serve this tenant from a process whose first tenant is in %q (Wave-RF/chtypes#419)",
		session, imageZone(), zoneless, session)
}

// zonelessDateTime reports whether a canonical type holds a DateTime or
// DateTime64 with no explicit zone. Anything unclear counts as zone-less.
func zonelessDateTime(typ string) bool {
	rest := typ
	for {
		i := strings.Index(rest, "DateTime")
		if i < 0 {
			return false
		}
		rest = strings.TrimPrefix(rest[i+len("DateTime"):], "64")
		if !strings.HasPrefix(rest, "(") {
			return true
		}
		args, _, ok := strings.Cut(rest[1:], ")")
		if !ok || !strings.Contains(args, "'") {
			return true
		}
	}
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
