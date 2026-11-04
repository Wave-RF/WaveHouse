package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// dedupeIDStoresNothing reports whether col stores NULL or "" when a record
// omits it, sends null, or may not write it, which eventIDAt reads as missing.
// Any other column lets the parser fill a value the record never sent (a
// DEFAULT, a nil UUID, an integer's 0, a FixedString's zero bytes), and dedupe
// would key on it and drop every later such record as a duplicate. An empty
// string sent for a Nullable column of any other type is stored as that
// type's zero value, not NULL, so the rule is a string column and nothing
// else, with no per-value check: String or LowCardinality of it, either
// Nullable, with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression.
func dedupeIDStoresNothing(col discovery.Column) bool {
	if col.HasDefault || col.DefaultKind != "" {
		return false
	}
	switch col.Type {
	case "String", "Nullable(String)", "LowCardinality(String)", "LowCardinality(Nullable(String))":
		return true
	}
	return false
}

// dedupeIDReady answers whether table's dedupe id column can key dedupe, with
// the error every ingest into it gets when it cannot: dedupe never silently
// falls back to publishing undeduped, which would hide a misconfigured table.
// It is a 500 marked not retryable, like roleRefusedAbort, since only a change
// to the settings or the table clears it. A table without dedupe, or whose
// id_field names no column, is left to the per-record missing-id path. It
// returns the settings it judged, which the request remembers (ingestRun.idChecked)
// so a reload during the body's processing cannot slip another column past it.
func (h *IngestHandler) dedupeIDReady(ctx context.Context, store *settings.Store, table string, schema *discovery.TableSchema) (settings.Dedupe, *requestAbort) {
	if h.Dedup == nil || h.DedupeSettings == nil {
		return settings.Dedupe{}, nil
	}
	dd := h.DedupeSettings(store, table)
	if !dd.Enabled {
		return dd, nil
	}
	return dd, h.dedupeIDColumn(ctx, store, table, schema, dd.IDField)
}

// dedupeIDColumn is the column half of dedupeIDReady, for an id_field that
// came from a settings snapshot read later in the request. nil means idField
// can key dedupe or names no column.
func (h *IngestHandler) dedupeIDColumn(ctx context.Context, store *settings.Store, table string, schema *discovery.TableSchema, idField string) *requestAbort {
	col, ok := schema.Lookup(idField)
	if !ok || dedupeIDStoresNothing(col) {
		return nil
	}
	id := store.Tenant()
	if h.noticeDue("dedupe-id:" + id.String() + "/" + table + "/" + idField) {
		slog.ErrorContext(ctx, "ingest refused: the dedupe id column stores a value the record did not send",
			"tenant", id, "table", table, "id_field", idField, "type", col.Type, "default_kind", col.DefaultKind)
	}
	retryable := false
	return &requestAbort{
		Status: http.StatusInternalServerError,
		Message: fmt.Sprintf("dedupe id column %q must be String or Nullable(String) with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression",
			idField),
		Retryable: &retryable,
		// Only counted when a reload moves dedupe to this column mid-request;
		// a refusal before the body is read counts nothing.
		Result: resultDedupeUnavailable,
	}
}
