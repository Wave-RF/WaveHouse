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
// It is about one tenant, or one of its tables: a tenant not bound yet, a
// missing artifact for the tenant's server line, a server zone chtypes cannot
// serve, a table that did not compile, or a table whose zone-less DateTime
// expressions this process cannot compute in its server's zone (that table
// alone) leaves every other tenant answering. The one exception is a first
// open whose artifact does not load: chtypes then refuses every tenant in
// another zone until one in that open's zone is served (see openFirst).
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

// RoleRefused reports that a role's projection of a table does not compile,
// while the table itself does. Only Engine.RoleTable returns it.
//
// Unlike Unavailable it is a standing condition: it follows from the role's
// insert policy and the table's schema, and the refusal is cached for the
// schema generation, so the same request fails the same way until an operator
// changes one of them. A caller must not answer it with a retry hint.
type RoleRefused struct {
	Tenant tenant.ID
	Table  string
	// Cause is the operator-facing reason: ClickHouse's own compile refusal,
	// or the shape the role asked for that cannot be expressed.
	Cause string
}

func (e *RoleRefused) Error() string {
	return fmt.Sprintf("chtypes cannot compile the role's schema for tenant %q table %q: %s", e.Tenant, e.Table, e.Cause)
}

// ErrColumnsDrift is returned by ParseRow when the envelope's column list is
// not an INSERT column list the current compiled handle accepts: it names a
// column the handle does not export, or names one twice. The event predates a
// schema change (or was not written by this gateway) and must be withheld
// rather than read under guessed positions.
var ErrColumnsDrift = errors.New("row columns do not match the table's wire columns")
