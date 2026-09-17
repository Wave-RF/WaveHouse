package stream

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/policy"
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
	// author's to fix: a stored value the expression cannot read, and a filter
	// CONSTANT the column's type cannot read — a claim rendering as "abc", "-1",
	// "1.5" or "007" against a numeric column now compiles and answers code 53
	// per row, where before the String-parameter binding it failed to compile
	// and counted as ReasonDecline. Nothing hidden became visible; the label
	// moved.
	ReasonError = typelayer.ReasonError
	// ReasonDecline: no verdict was reached at all — the expression would not
	// compile for this generation, or chtypes would not answer for it. Withheld,
	// like every answer that is not a definite true.
	ReasonDecline = typelayer.ReasonDecline
	// ReasonUnavailable: no compiled schema can answer for this table — no
	// artifact for the server's version line, a compile refusal, or no engine
	// wired at all. Nothing about the row; every row-filtered subscriber of the
	// table is affected until it is fixed.
	ReasonUnavailable = "unavailable"
	// ReasonDrift: the event's column list is not the one the current compiled
	// generation exports, so its positional row cannot be read at all. Expected
	// briefly after a schema change, alarming if it persists.
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
	// Prepare reads one event's row. columns is the envelope's column list, row
	// the positional JSON array as published. An error means no subscriber of a
	// row-filtered role may see this row; the Hub labels the withhold with
	// WithheldReason(err).
	Prepare(table string, columns []string, row []byte) (RowView, error)
}

// RowView is one prepared event row, reusable across every subscriber of every
// row-filtered role on that event. Close must be called once the fan-out is
// done: the parsed row holds native memory that is not reference-counted.
type RowView interface {
	// Visible reports whether perms admit this row, and when they do not, the
	// Reason* label the withheld metric wants. reason is "" when visible.
	Visible(perms *policy.ResolvedPermissions) (visible bool, reason string)
	Close()
}

// NewRowEvaluator builds the production evaluator: the row is parsed by
// ClickHouse's own parser and the role's predicate is evaluated by ClickHouse's
// own expression engine, so the stream's verdict is the one the query path's
// WHERE would reach over the stored row.
//
// A nil engine is a valid argument and withholds every row-filtered row (see
// engineEvaluator.Prepare) — the same answer the Hub's unwired default gives,
// so a boot path that forgets to build an Engine fails closed rather than
// silently unfiltered.
func NewRowEvaluator(types *typelayer.Engine, logger *slog.Logger) RowEvaluator {
	return &engineEvaluator{types: types, logger: logger}
}

// engineEvaluator is the RowEvaluator every production path uses: a
// typelayer.Engine parsing the row and evaluating the role's predicates on it.
type engineEvaluator struct {
	types  *typelayer.Engine
	logger *slog.Logger
	// unwired reports the missing engine once rather than once per event. The
	// condition is a boot-time wiring mistake, so the first line says everything
	// the next million would.
	unwired sync.Once
}

func (e *engineEvaluator) log() *slog.Logger {
	if e.logger == nil {
		return slog.Default()
	}
	return e.logger
}

// errNoEngine is the cause behind an unwired evaluator's withholds. It is not a
// verdict about the row: no row of any row-filtered role can be evaluated.
var errNoEngine = errors.New("no type engine is wired into the stream hub")

func (e *engineEvaluator) Prepare(table string, columns []string, row []byte) (RowView, error) {
	if e.types == nil {
		e.unwired.Do(func() {
			e.log().Error("row-level security cannot be evaluated: no type engine is wired into the stream hub; "+
				"every row is withheld from every subscriber of a row-filtered role until one is",
				"table", table)
		})
		return nil, &withheldError{reason: ReasonUnavailable, err: errNoEngine}
	}
	tbl, err := e.types.Table(table)
	if err != nil {
		return nil, classifyPrepare(err)
	}
	parsed, err := tbl.ParseRow(columns, row)
	if err != nil {
		tbl.Release()
		return nil, classifyPrepare(err)
	}
	return &engineRowView{tbl: tbl, row: parsed}, nil
}

// engineRowView holds the table handle for as long as the parsed row lives: the
// row is owned by the compiled schema, so releasing the handle first would leave
// a rebind free of memory the fan-out is still reading.
type engineRowView struct {
	tbl *typelayer.Table
	row *typelayer.Row
}

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

// classifyPrepare names WHY no view could be prepared. The three causes are
// operationally different — a missing artifact is an estate problem, drift is a
// schema change in flight, a parse failure is one bad event — and they are
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
