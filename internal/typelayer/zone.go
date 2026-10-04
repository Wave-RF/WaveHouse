package typelayer

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// This file is the one place the zone rule lives.
//
// chtypes loads every library under one image zone per process (chtypes.Setup,
// before the first open), and that zone is what a zone-less DateTime column
// and the functions over it read in. A call's own session_timezone governs the
// rest: how a body's rows parse into instants, and a filter's WHERE, which
// answers in the zone the filter was created in.
//
// The image zone is the first bound tenant's server zone, so a single-zone
// deployment is exact; UTC if this host's zoneinfo does not know that zone,
// since chtypes would then refuse every open, for every tenant, for the life
// of the process. Every other tenant's calls carry its server zone as
// session_timezone, the same zone on a filter's create as on every parse it is
// evaluated against (Table.zoneOpts): chtypes declines a batch whose filter
// was created in another zone. What session_timezone does not reach is an
// expression over a zone-less DateTime column, so such a table is unavailable
// for that tenant (zoneCause) until chtypes binds a zone per schema
// (Wave-RF/chtypes#419).

var image struct {
	mu   sync.Mutex
	set  bool
	zone string
	err  error
}

// openLine resolves the library for serverVersion's line, committing tz as the
// process's image zone if no tenant has yet. A non-empty cause is why the
// tenant cannot be served: a zone this host does not know, the setup refused,
// or no loadable artifact for the line (the SDK's own message, with its
// CHTYPES_ARTIFACT_* code).
func openLine(reg *chtypes.Registry, serverVersion, tz string) (*chtypes.Library, string) {
	known := knownZone(tz)
	candidate := tz
	if !known {
		candidate = "UTC"
	}
	zone, err := setupImage(candidate)
	if err != nil {
		return nil, fmt.Sprintf("chtypes refused this process's image zone %q: %s", zone, err)
	}
	if !known {
		return nil, fmt.Sprintf("ClickHouse reports server timezone %q, which this host's zoneinfo does not know, "+
			"so no row of this tenant can be read in it", tz)
	}
	lib, err := reg.For(versionLine(serverVersion))
	if err != nil {
		return nil, err.Error()
	}
	return lib, ""
}

// knownZone reports whether this host's zoneinfo, which chtypes reads zone
// names from, knows tz. Nothing in the binary embeds a zone database
// (time/tzdata), so time's own lookup reads the same files.
func knownZone(tz string) bool {
	if tz == "" || tz == "Local" {
		return false
	}
	_, err := time.LoadLocation(tz)
	return err == nil
}

// setupImage commits the image zone once per process and returns it.
func setupImage(tz string) (string, error) {
	image.mu.Lock()
	defer image.mu.Unlock()
	if !image.set {
		image.set = true
		image.zone = tz
		image.err = chtypes.Setup(chtypes.SetupOptions{Timezone: tz})
		slog.Info("chtypes image zone committed", "zone", tz)
	}
	return image.zone, image.err
}

// imageZone is the committed image zone, "" before the first bind.
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
			"declares one (%q) alongside DEFAULT, MATERIALIZED or ALIAS expressions, which would compute in the wrong zone; "+
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
