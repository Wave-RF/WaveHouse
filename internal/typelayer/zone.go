package typelayer

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// This file is the one place the server-zone rule lives.
//
// chtypes initialises each library in one server timezone, once per process:
// the SDK opens an artifact image (a file, however it is spelled or linked) at
// most once and refuses to open it again in another zone, and a per-call
// session_timezone does not change how a bare DateTime is read. So a zone is a
// fact about this process and one opened library, not about an Engine or a
// tenant: the first tenant to open a line's library fixes its zone, and a
// tenant on that line whose server reports another zone cannot be served by
// this process. Every library open goes through openLine, so the record below
// is complete.
//
// The zone reaches the SDK through its process default (SetDefaultTimezone),
// not WithTimezone: a registry's WithTimezone is fixed when it is built, and
// the Engine's one registry is built at boot, before discovery has reported
// any server's zone, and then opens every line, each in the zone of the first
// tenant on it. The default is set under the same lock as every open, so the
// zone an open reads is the one set for it.

// openedZones is the zone each opened library was initialised in, keyed by the
// path the SDK reports for it (one image per file, shared by every Registry).
var openedZones = struct {
	mu  sync.Mutex
	lib map[string]openedLib
}{lib: map[string]openedLib{}}

type openedLib struct {
	line string
	zone string
}

// openLine resolves the library for serverVersion's line, opening it in tz
// when this process has not opened it yet. A non-empty cause is why the tenant
// cannot be served: no loadable artifact for the line (the SDK's own message,
// which also covers a library path that cannot be stat'ed, refused before any
// dlopen), or a library already opened in another zone.
func openLine(reg *chtypes.Registry, serverVersion, tz string) (*chtypes.Library, string) {
	line := versionLine(serverVersion)

	openedZones.mu.Lock()
	defer openedZones.mu.Unlock()

	chtypes.SetDefaultTimezone(tz) // read only if this call is what opens the library
	lib, err := reg.For(chtypes.Version(line))
	if errors.Is(err, chtypes.ErrInitConflict) {
		// Another Registry in this process (another Engine) opened the image
		// in another zone before this one resolved the line. The record only
		// names that zone; the SDK's words are kept beside ours.
		return nil, zoneCause(tz, line, openedZoneOfLine(line, tz)) + " (" + err.Error() + ")"
	}
	if err != nil {
		return nil, err.Error()
	}
	o, seen := openedZones.lib[lib.Path]
	if !seen {
		openedZones.lib[lib.Path] = openedLib{line: lib.Minor, zone: tz}
		return lib, ""
	}
	if o.zone != tz {
		return nil, zoneCause(tz, lib.Minor, o.zone)
	}
	return lib, ""
}

// openedZoneOfLine is a zone other than tz that a library of line was opened
// in, "" when the record has none. Caller holds openedZones.mu.
func openedZoneOfLine(line, tz string) string {
	var zones []string
	for _, o := range openedZones.lib {
		if o.line == line && o.zone != tz {
			zones = append(zones, o.zone)
		}
	}
	if len(zones) == 0 {
		return ""
	}
	return slices.Min(zones)
}

// zoneCause is the per-tenant zone refusal; opened is "" when the zone the
// line was opened in is not known.
func zoneCause(tz, line, opened string) string {
	in := "another timezone"
	if opened != "" {
		in = strconv.Quote(opened)
	}
	return fmt.Sprintf(
		"ClickHouse reports server timezone %q, but this process opened the chtypes library for ClickHouse %s in %s; "+
			"one process serves one timezone per ClickHouse version line, so serve this tenant from another process or restart",
		tz, line, in)
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

// samePatch reports whether the loaded artifact was built from the server's
// own patch. An artifact's version names its channel ("26.8.15.10-lts"); a
// server's version() does not.
func samePatch(artifact chtypes.Version, serverVersion string) bool {
	a, _, _ := strings.Cut(string(artifact), "-")
	return a == strings.TrimPrefix(strings.TrimSpace(serverVersion), "v")
}
