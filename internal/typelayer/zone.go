package typelayer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// This file is the one place the server-zone rule lives.
//
// chtypes initialises each library in one server timezone, once per process:
// the SDK opens an artifact path at most once and refuses to open it again in
// another zone, and a per-call session_timezone does not change how a bare
// DateTime is read. So a zone is a fact about this process and one opened
// library, not about an Engine or a tenant: the first tenant to open a line's
// library fixes its zone, and a tenant on that line whose server reports
// another zone cannot be served by this process. Every library open goes
// through openLine, so the record below is complete.
//
// The zone reaches the SDK through its process default (SetDefaultTimezone),
// not WithTimezone: a registry's WithTimezone is fixed when it is built, and
// the Engine's one registry is built at boot, before discovery has reported
// any server's zone, and then opens every line, each in the zone of the first
// tenant on it. The default is set under the same lock as every open, so the
// zone an open reads is the one set for it.

// openedZones is the zone each opened library was initialised in, keyed by the
// library's path (the SDK opens one image per path, shared by every Registry).
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
// cannot be served: no loadable artifact for the line (the SDK's own message),
// or a library already opened in another zone.
func openLine(reg *chtypes.Registry, serverVersion, tz string) (*chtypes.Library, string) {
	line := versionLine(serverVersion)

	openedZones.mu.Lock()
	defer openedZones.mu.Unlock()

	chtypes.SetDefaultTimezone(tz) // read only if this call is what opens the library
	lib, err := reg.For(chtypes.Version(line))
	if err != nil {
		// The SDK refuses to open a path already open in another zone, which
		// another Registry in this process (another Engine) reaches before it
		// has resolved the line itself. Its error is untyped, so the case is
		// recognised from the record, and its words are kept beside ours.
		for _, o := range openedZones.lib {
			if o.line == line && o.zone != tz {
				return nil, zoneCause(tz, line, o.zone) + " (" + err.Error() + ")"
			}
		}
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

func zoneCause(tz, line, opened string) string {
	return fmt.Sprintf(
		"ClickHouse reports server timezone %q, but this process opened the chtypes library for ClickHouse %s in %q; "+
			"one process serves one timezone per ClickHouse version line, so serve this tenant from another process or restart",
		tz, line, opened)
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
