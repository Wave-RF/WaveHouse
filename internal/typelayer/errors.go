package typelayer

import (
	"errors"
	"fmt"
)

// Unavailable reports that no compiled schema can answer for a table right
// now. It is never a verdict about data: callers map it to HTTP 503 on ingest
// and to "withhold" on the stream, so it must stay distinguishable from a
// ClickHouse rejection.
//
// Table is "" when the cause is process-wide (no artifact for the server's
// version line, or a timezone the process cannot adopt).
type Unavailable struct {
	Table string
	// Cause is the operator-facing reason, already carrying the SDK's own
	// wording where there is one (an artifact-missing error lists the
	// directories it searched, which is the whole diagnostic).
	Cause string
}

func (e *Unavailable) Error() string {
	if e.Table == "" {
		return "chtypes unavailable: " + e.Cause
	}
	return fmt.Sprintf("chtypes unavailable for table %q: %s", e.Table, e.Cause)
}

// IsUnavailable reports whether err is an *Unavailable anywhere in its chain.
func IsUnavailable(err error) bool {
	var u *Unavailable
	return errors.As(err, &u)
}

// ErrColumnsDrift is returned by ParseRow when the envelope's column list is
// not the one the current compiled handle exports. A positional row is only
// interpretable against the generation that produced it, so a mismatch means
// the event predates a schema change and must be withheld rather than guessed.
var ErrColumnsDrift = errors.New("row columns do not match the table's wire columns")
