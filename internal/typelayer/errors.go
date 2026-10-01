package typelayer

import (
	"errors"
	"fmt"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Unavailable reports that no compiled schema can answer for a table right
// now. It is never a verdict about data: callers map it to HTTP 503 on ingest
// and to "withhold" on the stream, so it must stay distinguishable from a
// ClickHouse rejection.
//
// It is always about one tenant: a missing artifact for the tenant's server
// line, a server zone this process cannot adopt, or a table that did not
// compile leaves every other tenant answering.
type Unavailable struct {
	Tenant tenant.ID
	// Table is "" when the cause covers the tenant's every table.
	Table string
	// Cause is the operator-facing reason, already carrying the SDK's own
	// wording where there is one (an artifact-missing error lists the
	// directories it searched, which is the whole diagnostic).
	Cause string
}

func (e *Unavailable) Error() string {
	if e.Table == "" {
		return fmt.Sprintf("chtypes unavailable for tenant %q: %s", e.Tenant, e.Cause)
	}
	return fmt.Sprintf("chtypes unavailable for tenant %q table %q: %s", e.Tenant, e.Table, e.Cause)
}

// IsUnavailable reports whether err is an *Unavailable anywhere in its chain.
func IsUnavailable(err error) bool {
	var u *Unavailable
	return errors.As(err, &u)
}

// ErrColumnsDrift is returned by ParseRow when the envelope's column list is
// not an INSERT column list the current compiled handle accepts: it names a
// column the handle does not export, or names one twice. The event predates a
// schema change (or was not written by this gateway) and must be withheld
// rather than read under guessed positions.
var ErrColumnsDrift = errors.New("row columns do not match the table's wire columns")
