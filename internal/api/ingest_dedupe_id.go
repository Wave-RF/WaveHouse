package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// dedupeIDStoresNothing reports whether col stores NULL or "" when a record
// omits it, sends null, or may not write it, which eventIDAt reads as missing.
// Any other column lets the parser fill a value the record never sent (a
// DEFAULT, a nil UUID, an integer's 0, a FixedString's zero bytes), and dedupe
// would key on it and drop every later such record as a duplicate. So the
// column must be Nullable(T) for any T, String, or LowCardinality of either,
// with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression.
func dedupeIDStoresNothing(col discovery.Column) bool {
	if col.HasDefault || col.DefaultKind != "" {
		return false
	}
	typ := col.Type
	if inner, ok := strings.CutPrefix(typ, "LowCardinality("); ok {
		if typ, ok = strings.CutSuffix(inner, ")"); !ok {
			return false
		}
	}
	return typ == "String" || (strings.HasPrefix(typ, "Nullable(") && strings.HasSuffix(typ, ")"))
}

// dedupeIDReady answers whether table's dedupe id column can key dedupe, with
// the error every ingest into it gets when it cannot: dedupe never silently
// falls back to publishing undeduped, which would hide a misconfigured table.
// It is a 500 marked not retryable, like roleRefusedAbort, since only a change
// to the settings or the table clears it. A table without dedupe, or whose
// id_field names no column, is left to the per-record missing-id path.
func (h *IngestHandler) dedupeIDReady(ctx context.Context, store *settings.Store, table string, schema *discovery.TableSchema) *requestAbort {
	if h.Dedup == nil || h.DedupeSettings == nil {
		return nil
	}
	dd := h.DedupeSettings(store, table)
	if !dd.Enabled || dd.IDField == "" {
		return nil
	}
	col, ok := schema.Lookup(dd.IDField)
	if !ok || dedupeIDStoresNothing(col) {
		return nil
	}
	id := store.Tenant()
	if h.noticeDue("dedupe-id:" + id.String() + "/" + table + "/" + dd.IDField) {
		slog.ErrorContext(ctx, "ingest refused: the dedupe id column stores a value the record did not send",
			"tenant", id, "table", table, "id_field", dd.IDField, "type", col.Type, "default_kind", col.DefaultKind)
	}
	retryable := false
	return &requestAbort{
		Status: http.StatusInternalServerError,
		Message: fmt.Sprintf("dedupe id column %q must be Nullable or String with no DEFAULT, MATERIALIZED, ALIAS or EPHEMERAL expression",
			dd.IDField),
		Retryable: &retryable,
	}
}
