package typelayer

import (
	"fmt"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// This file is the one place the server-zone rule lives.
//
// chtypes reads its Timezone package global once per library, when the library
// is first opened, and a library is opened at most once per process (the SDK
// dedupes by path). So a zone is a fact about this process and one ClickHouse
// version line, not about an Engine or a tenant: the first tenant to open a
// line fixes its zone, and a tenant on that line whose server reports another
// zone cannot be served by this process. Every library open goes through
// openLine, so the record below is complete.

// lineZones is the zone each opened library was initialised with, keyed by the
// SDK's own *Library (one per opened artifact, shared by every Registry).
var lineZones = struct {
	mu   sync.Mutex
	zone map[*chtypes.Library]string
}{zone: map[*chtypes.Library]string{}}

// openLine resolves the library for serverVersion, opening it in tz when this
// process has not opened it yet. A non-empty cause is why the tenant cannot be
// served: no loadable artifact for the line (the SDK's own message), or a line
// already opened in another zone.
func openLine(reg *chtypes.Registry, serverVersion, tz string) (*chtypes.Library, string) {
	lineZones.mu.Lock()
	defer lineZones.mu.Unlock()

	chtypes.Timezone = tz // read only if this For is what opens the library
	lib, err := reg.For(chtypes.Version(serverVersion))
	if err != nil {
		return nil, err.Error()
	}
	opened, seen := lineZones.zone[lib]
	if !seen {
		lineZones.zone[lib] = tz
		return lib, ""
	}
	if opened != tz {
		return nil, fmt.Sprintf(
			"ClickHouse reports server timezone %q, but this process opened the chtypes library for ClickHouse %s in %q; "+
				"one process serves one timezone per ClickHouse version line, so serve this tenant from another process or restart",
			tz, lib.Minor, opened)
	}
	return lib, ""
}
