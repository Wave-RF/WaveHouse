package api

import (
	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// This file holds the per-record decision points a native type layer will take
// over: schema validation, timestamp canonicalization, and the insert-check
// comparison. Each is an interface with a default implementation that delegates
// to today's code unchanged, so the replacement is a wiring change rather than a
// rewrite of the ingest handler. Nothing here decides anything itself.

// RecordValidator covers the two schema-driven steps of the record pipeline.
// Validate rejects a record the table's schema cannot accept; CanonicalizeTimestamps
// rewrites DateTime/DateTime64 values to the canonical wire form in place.
//
// CanonicalizeTimestamps returns nothing, matching discovery's function: it is
// deliberately fail-open (#372) — a value it cannot read is passed through for
// ClickHouse to judge, and the row filter is what enforces. Giving it an error
// return would invite a caller to change that.
//
// The two are one interface because they are one contract — "what this schema
// says about this record" — evaluated at two points in processRecord that must
// stay apart: the insert-check block sits between them deliberately, so checks
// keep pre-#372 semantics.
type RecordValidator interface {
	Validate(schema *discovery.TableSchema, record map[string]any) error
	CanonicalizeTimestamps(schema *discovery.TableSchema, record map[string]any)
}

// discoveryValidator is the default RecordValidator, delegating to
// internal/discovery.
type discoveryValidator struct{}

func (discoveryValidator) Validate(schema *discovery.TableSchema, record map[string]any) error {
	return discovery.Validate(schema, record)
}

func (discoveryValidator) CanonicalizeTimestamps(schema *discovery.TableSchema, record map[string]any) {
	discovery.CanonicalizeTimestamps(schema, record)
}

// validator returns the handler's RecordValidator, or the default when none is
// wired. Nil-safe rather than constructor-enforced because IngestHandler is
// built by struct literal in places.
func (h *IngestHandler) validator() RecordValidator {
	if h.Validator != nil {
		return h.Validator
	}
	return discoveryValidator{}
}

// InsertChecker decides whether a record's value satisfies a policy check
// clause. Matches answers the scalar `_eq` form (the required value), InSet the
// `_in` form (set membership). It never sees a record as a whole: the
// auto-injection of a missing check value stays in processRecord, where the
// ordering against validation and canonicalization is load-bearing.
type InsertChecker interface {
	Matches(actual, required any) bool
	InSet(v any, set []any) bool
}

// canonicalChecker is the default InsertChecker: the canonical-scalar
// comparison in ingest.go, unchanged.
type canonicalChecker struct{}

func (canonicalChecker) Matches(actual, required any) bool {
	return checkValueMatches(actual, required)
}

func (canonicalChecker) InSet(v any, set []any) bool { return valueInSet(v, set) }

// checker returns the handler's InsertChecker, or the default when none is
// wired. Nil-safe for the same reason as validator.
func (h *IngestHandler) checker() InsertChecker {
	if h.Checker != nil {
		return h.Checker
	}
	return canonicalChecker{}
}
