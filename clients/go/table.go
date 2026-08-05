package wavehouse

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
)

// TableRef is a reference to a table. Use it for queries, inserts, schema, and
// streams. NOT safe to use concurrently from multiple goroutines for mutations;
// reads (Fetch, Select, etc.) are safe.
type TableRef struct {
	ctx          httpContext
	table        string
	createStream func(table string, opts *StreamOptions) *StreamController
}

// Fetch is a SELECT * shortcut with a default limit of 1000.
func (t *TableRef) Fetch(ctx context.Context) (*Page[map[string]any], error) {
	return t.SelectAll().Limit(DefaultLimit).FetchUntyped(ctx)
}

// Select starts building a typed query with the given column projection.
func (t *TableRef) Select(columns ...string) *QueryBuilder {
	return &QueryBuilder{
		ctx:          t.ctx,
		createStream: t.createStream,
		state: queryState{
			table:   t.table,
			columns: columns,
		},
	}
}

// SelectAll starts a query that selects every column the caller's role may read.
func (t *TableRef) SelectAll() *QueryBuilder {
	return t.Select().SelectAll()
}

// Insert inserts one or more rows into this table. A single map or struct is
// sent as JSON; any slice — []map[string]any, a generated/user-defined row
// type such as []ClickRow, etc. — is serialized to NDJSON for batch ingest.
func (t *TableRef) Insert(ctx context.Context, data any) (*InsertResult, error) {
	if rows, ok := data.([]map[string]any); ok {
		return t.insertBatch(ctx, rows)
	}
	if rv, ok := sliceValue(data); ok {
		return t.insertBatchReflect(ctx, rv)
	}
	return t.insertSingle(ctx, data)
}

// sliceValue reports whether data is a slice type, returning its
// reflect.Value for iteration. []byte is excluded and treated as an opaque
// single value (matching encoding/json's special-cased handling of byte
// slices) rather than a batch of numbers.
func sliceValue(data any) (reflect.Value, bool) {
	if data == nil {
		return reflect.Value{}, false
	}
	if _, isBytes := data.([]byte); isBytes {
		return reflect.Value{}, false
	}
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}
	return v, true
}

// InsertNDJSON inserts pre-formatted NDJSON (one record per line).
func (t *TableRef) InsertNDJSON(ctx context.Context, ndjson string) (*InsertResult, error) {
	return t.sendNDJSON(ctx, ndjson)
}

// Schema returns the table's column definitions from ClickHouse. Admin-only.
func (t *TableRef) Schema(ctx context.Context) (*TableSchema, error) {
	var schema TableSchema
	if err := doRequest(t.ctx, ctx, requestOptions{
		method: "GET",
		path:   "/v1/schema",
		params: url.Values{"table": {t.table}},
	}, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// Stream opens a live SSE event stream for this table.
func (t *TableRef) Stream(opts *StreamOptions) *StreamController {
	return t.createStream(t.table, opts)
}

func (t *TableRef) insertSingle(ctx context.Context, data any) (*InsertResult, error) {
	var res struct {
		OK        *bool `json:"ok"`
		Duplicate *bool `json:"duplicate"`
	}
	if err := doRequest(t.ctx, ctx, requestOptions{
		method: "POST",
		path:   "/v1/ingest",
		params: url.Values{"table": {t.table}},
		body:   data,
	}, &res); err != nil {
		return nil, err
	}
	ok := true
	if res.OK != nil {
		ok = *res.OK
	}
	result := &InsertResult{OK: ok}
	if res.Duplicate != nil {
		result.Duplicate = res.Duplicate
	}
	return result, nil
}

func (t *TableRef) insertBatch(ctx context.Context, rows []map[string]any) (*InsertResult, error) {
	if len(rows) == 0 {
		zero := 0
		return &InsertResult{
			OK:         true,
			Total:      &zero,
			Succeeded:  &zero,
			Failed:     &zero,
			Duplicates: &zero,
		}, nil
	}
	var sb strings.Builder
	for i, row := range rows {
		if i > 0 {
			sb.WriteByte('\n')
		}
		raw, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		sb.Write(raw)
	}
	return t.sendNDJSON(ctx, sb.String())
}

// insertBatchReflect is the fallback batch path for any slice type other
// than []map[string]any (the fast path in insertBatch above) — e.g. a
// generated or user-defined row type such as []ClickRow. Each element is
// marshaled to JSON individually and joined as NDJSON, exactly like
// insertBatch, so the server's per-record batch summary (failed, results,
// etc.) is preserved instead of being silently dropped by insertSingle.
func (t *TableRef) insertBatchReflect(ctx context.Context, rows reflect.Value) (*InsertResult, error) {
	n := rows.Len()
	if n == 0 {
		zero := 0
		return &InsertResult{
			OK:         true,
			Total:      &zero,
			Succeeded:  &zero,
			Failed:     &zero,
			Duplicates: &zero,
		}, nil
	}
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte('\n')
		}
		raw, err := json.Marshal(rows.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		sb.Write(raw)
	}
	return t.sendNDJSON(ctx, sb.String())
}

func (t *TableRef) sendNDJSON(ctx context.Context, ndjson string) (*InsertResult, error) {
	var res struct {
		Total      int                  `json:"total"`
		Succeeded  int                  `json:"succeeded"`
		Failed     int                  `json:"failed"`
		Duplicates int                  `json:"duplicates"`
		Results    []InsertRecordResult `json:"results"`
	}
	if err := doRequest(t.ctx, ctx, requestOptions{
		method:      "POST",
		path:        "/v1/ingest",
		params:      url.Values{"table": {t.table}},
		rawBody:     ndjson,
		contentType: "application/x-ndjson",
	}, &res); err != nil {
		return nil, err
	}
	result := &InsertResult{
		OK:         res.Failed == 0,
		Total:      &res.Total,
		Succeeded:  &res.Succeeded,
		Failed:     &res.Failed,
		Duplicates: &res.Duplicates,
	}
	if len(res.Results) > 0 {
		result.Results = res.Results
	}
	return result, nil
}
