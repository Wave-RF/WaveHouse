package stream

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// Reasons a row was withheld, the "reason" label on
// wavehouse_sse_rows_withheld_total. The first three are the type layer's own
// verdict classes, aliased so the two packages cannot drift on the spelling; the
// last two are this package's, for the cases where no verdict was ever reached.
const (
	// ReasonFilter: the role's predicate answered false — the ordinary case, and
	// the only one that is not a symptom of something being wrong. It is also
	// the answer for a predicate whose claim was unresolvable, which matches no
	// row without the type layer compiling anything (the query path's `1 = 0`).
	ReasonFilter = typelayer.ReasonFilter
	// ReasonError: ClickHouse evaluated the predicate over this row and raised.
	// Either side of the comparison can cause it, and both are the policy
	// author's to fix: a stored value the expression cannot read, or a filter
	// constant the column's type cannot read (a claim rendering as "abc"
	// against a Decimal column answers code 53 per row, against a Float one
	// 72; an integer column's strict cast answers false instead). It is also
	// classifyPrepare's label for a Prepare failure that is neither
	// ReasonUnavailable nor ReasonDrift: the type layer refusing the parse call
	// as a whole.
	ReasonError = typelayer.ReasonError
	// ReasonDecline: no verdict was reached. The expression would not compile
	// for this generation, chtypes would not answer for the row (one that does
	// not parse lands here, not in Prepare's error: see
	// TestEngineEvaluator_UnreadableRowIsDeclined), or the predicate reads a
	// column the event does not carry (see engineRowView.Visible). Withheld,
	// like every answer that is not a definite true.
	ReasonDecline = typelayer.ReasonDecline
	// ReasonUnavailable: no compiled schema can answer for this tenant's table
	// — the tenant is not bound yet, its server line has no artifact, its server
	// zone differs from the one this process opened that line with, the table's
	// schema did not compile, or no engine is wired at all. Nothing about the
	// row; every row-filtered subscriber of the table is affected until it is
	// fixed.
	ReasonUnavailable = "unavailable"
	// ReasonDrift: the event's column list names a column the table's current
	// generation does not export (dropped or renamed since), or names one twice,
	// so its positional row cannot be read at all. Expected briefly after a
	// schema change, alarming if it persists.
	ReasonDrift = "drift"
)

// RowEvaluator is the one place a row's visibility under a role's row-filter is
// decided. Prepare is called ONCE per event — parsing the positional row is the
// expensive half — and the returned view answers for each subscriber's resolved
// permissions. The interface exists because that ratio (one parse, K visibility
// questions) is the whole shape of the hot path, and because it lets the tests
// drive admission from a stub instead of a compiled schema; the production
// implementation is engineEvaluator below.
//
// A nil RowEvaluator on the Hub means the fail-closed default (see
// Hub.rowEvaluator), never "everything visible".
type RowEvaluator interface {
	// Prepare reads one event's row for tenant id's table. columns is the
	// envelope's column list — the inserting role's, which may be narrower than
	// the table — and row the positional JSON array as published. An error
	// means no subscriber of a row-filtered role may see this row; the Hub
	// labels the withhold with WithheldReason(err).
	Prepare(id tenant.ID, table string, columns []string, row json.RawMessage) (RowView, error)
}

// RowView is one prepared event row, reusable across every subscriber of every
// row-filtered role on that event. Close must be called once the fan-out is
// done: the parsed row holds native memory and a schema handle.
type RowView interface {
	// Visible reports whether perms admit this row, and when they do not, the
	// Reason* label the withheld metric wants. reason is "" when visible.
	Visible(perms *policy.ResolvedPermissions) (visible bool, reason string)
	Close()
}

// NewRowEvaluator builds the production evaluator over the process's type
// layer: the row is parsed by ClickHouse's own parser, under the column list it
// was published with, and the role's predicate is evaluated by ClickHouse's own
// expression engine, so the stream's verdict is the one the query path's WHERE
// reaches over the stored row.
//
// A nil engine is a valid argument and withholds every row-filtered row — the
// same answer the Hub's unwired default gives — so a boot path that has no
// Engine fails closed rather than silently unfiltered.
func NewRowEvaluator(eng *typelayer.Engine) RowEvaluator {
	return &engineEvaluator{types: eng}
}

// engineEvaluator is the RowEvaluator every production path uses: a
// typelayer.Engine resolving the tenant's table, parsing the row and evaluating
// the role's predicates on it.
type engineEvaluator struct {
	types *typelayer.Engine
	// unwired reports the missing engine once rather than once per event. The
	// condition is a boot-time wiring mistake, so the first line says everything
	// the next million would.
	unwired sync.Once
}

// errNoEngine is the cause behind an unwired evaluator's withholds. It is not a
// verdict about the row: no row of any row-filtered role can be evaluated.
var errNoEngine = errors.New("no type engine is wired into the stream hub")

func (e *engineEvaluator) Prepare(id tenant.ID, table string, columns []string, row json.RawMessage) (RowView, error) {
	if e.types == nil {
		e.unwired.Do(func() {
			slog.Error("row-level security cannot be evaluated: no type engine is wired into the stream hub; "+
				"every row is withheld from every subscriber of a row-filtered role until one is",
				"tenant", id, "table", table)
		})
		return nil, &withheldError{reason: ReasonUnavailable, err: errNoEngine}
	}
	tbl, err := e.types.Table(id, table)
	if err != nil {
		return nil, classifyPrepare(err)
	}
	parsed, err := tbl.ParseRow(columns, row)
	if err != nil {
		tbl.Release()
		return nil, classifyPrepare(err)
	}
	carried := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		carried[c] = struct{}{}
	}
	return &engineRowView{tbl: tbl, row: parsed, carried: carried}, nil
}

// engineRowView holds the table handle for as long as the parsed row lives: the
// row is owned by the compiled schema, so releasing the handle first would leave
// a rebind free of memory the fan-out is still reading.
type engineRowView struct {
	tbl *typelayer.Table
	row *typelayer.Row
	// carried is the envelope's column list: the values the inserting role
	// actually supplied.
	carried map[string]struct{}
}

// Visible evaluates perms' row filter over the parsed row.
//
// A predicate over a column the event does not carry withholds the row from
// this subscriber, labelled ReasonDecline: the stream declines to judge a value
// the row never supplied. The parse does hold a value there — the DEFAULT, or
// the MATERIALIZED expression, computed when the stream parsed the row — but
// the stored row's value is computed again when the worker inserts, and a
// DEFAULT such as now(), rand() or generateUUIDv4() lands differently, so a
// verdict on the stream's copy could admit a row the query path's WHERE
// excludes. Withholding costs availability for that one role/column pairing,
// never confidentiality. Decline rather than filter because it is not a policy
// verdict: the reading role filters on a column the inserting role does not
// write, which an operator should be able to see.
func (v *engineRowView) Visible(perms *policy.ResolvedPermissions) (bool, string) {
	preds, ok := perms.Predicates()
	if !ok {
		// A denied grant, or one resolved for INSERT: no row is admissible. Same
		// answer the query path gives by never running the SELECT at all.
		return false, ReasonFilter
	}
	if len(preds) == 0 {
		return true, ""
	}
	for _, p := range preds {
		if _, carried := v.carried[p.Column]; !carried {
			return false, ReasonDecline
		}
	}
	return v.row.VisibleWithReason(preds)
}

func (v *engineRowView) Close() {
	v.row.Close()
	v.tbl.Release()
}

// withheldError carries the metric label alongside the cause, so the Hub does
// not have to re-derive from an error string what the evaluator already knew.
type withheldError struct {
	reason string
	err    error
}

func (e *withheldError) Error() string { return e.err.Error() }
func (e *withheldError) Unwrap() error { return e.err }

// classifyPrepare names WHY no view could be prepared. The causes are
// operationally different — an unavailable table (see ReasonUnavailable) is an
// estate problem, drift is a schema change in flight — and they are
// indistinguishable in the metric without the label.
func classifyPrepare(err error) error {
	switch {
	case typelayer.IsUnavailable(err):
		return &withheldError{reason: ReasonUnavailable, err: err}
	case errors.Is(err, typelayer.ErrColumnsDrift):
		return &withheldError{reason: ReasonDrift, err: err}
	default:
		return &withheldError{reason: ReasonError, err: err}
	}
}

// WithheldReason maps a Prepare error to its metric label. An evaluator that
// returns a plain error (a test double, a future implementation) reads as
// "error" rather than losing the withhold.
func WithheldReason(err error) string {
	var w *withheldError
	if errors.As(err, &w) {
		return w.reason
	}
	return ReasonError
}
