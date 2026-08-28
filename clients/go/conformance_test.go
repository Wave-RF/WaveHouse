package wavehouse

import (
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// wireCasesJSON embeds the shared wire-format fixture — the same file the TS
// runner replays (tests/conformance/conformance_ts.mjs) — so the test binary
// is self-contained: it works from a module archive or a standalone checkout
// without depending on paths outside the Go module.
//
//go:embed testdata/wire_cases.json
var wireCasesJSON []byte

// wireCase is one entry in the shared wire_cases.json fixture.
type wireCase struct {
	Name                string          `json:"name"`
	Endpoint            string          `json:"endpoint"`
	Table               string          `json:"table"`
	Operations          []wireOp        `json:"operations"`
	PipeName            string          `json:"pipe_name"`
	PipeParams          map[string]any  `json:"pipe_params"`
	PipeDefBody         json.RawMessage `json:"pipe_def"`
	PolicyBody          json.RawMessage `json:"policy_body"`
	SQL                 string          `json:"sql"`
	ExpectedPath        string          `json:"expected_path"`
	ExpectedMethod      string          `json:"expected_method"`
	ExpectedContentType string          `json:"expected_content_type"`
	ExpectedBody        json.RawMessage `json:"expected_body"`
	ExpectedRawBody     *string         `json:"expected_raw_body"`
}

type wireOp struct {
	Method string `json:"method"`
	Args   []any  `json:"args"`
}

// captured holds the HTTP request details from a single SDK call.
type captured struct {
	method      string
	path        string // path + query string
	contentType string
	body        string
}

// logErr surfaces SDK-call errors the assertions otherwise ignore: they only
// inspect the captured request, but a call that fails before sending should
// name the real cause.
func logErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Logf("SDK call returned error (request may still be valid): %v", err)
	}
}

// errOf drops the value of a two-result SDK call, keeping only the error.
func errOf[T any](_ T, err error) error { return err }

// jsonArg decodes a fixture field into the SDK request type it stands for.
func jsonArg[T any](t *testing.T, field string, raw json.RawMessage) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse %s: %v", field, err)
	}
	return v
}

func TestConformance_WireFormat(t *testing.T) {
	var cases []wireCase
	if err := json.Unmarshal(wireCasesJSON, &cases); err != nil {
		t.Fatalf("parse wire_cases.json: %v", err)
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			// capt is written on the server goroutine and read on the test
			// goroutine; the mutex is what makes that visible under -race.
			var mu sync.Mutex
			var capt captured
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				capt.method = r.Method
				capt.path = r.URL.RequestURI()
				capt.contentType = r.Header.Get("Content-Type")
				raw, _ := io.ReadAll(r.Body)
				capt.body = string(raw)
				mu.Unlock()

				// Return valid JSON so the SDK doesn't error on decode.
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasPrefix(r.URL.Path, "/v1/ops/dlq"):
					_ = json.NewEncoder(w).Encode(DLQStats{Tables: map[string]int{}, Total: 0})
				case strings.HasPrefix(r.URL.Path, "/v1/ops/schema") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode([]TableSchema{})
				case r.URL.Path == "/v1/ops/policy/validate" && r.Method == "POST":
					_ = json.NewEncoder(w).Encode(ValidationResult{Valid: true})
				case strings.HasPrefix(r.URL.Path, "/v1/ops/policy") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode(Policy{Tables: map[string]TablePolicy{}})
				case strings.HasPrefix(r.URL.Path, "/v1/ops/pipes/") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode(Pipe{Name: "test", SQL: "SELECT 1"})
				case r.URL.Path == "/v1/ops/pipes" && r.Method == "GET":
					_ = json.NewEncoder(w).Encode([]Pipe{})
				case strings.HasPrefix(r.URL.Path, "/v1/ingest"):
					if r.Header.Get("Content-Type") == "application/x-ndjson" {
						_ = json.NewEncoder(w).Encode(InsertResult{})
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
					}
				case r.URL.Path == "/v1/health":
					_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
				default:
					_ = json.NewEncoder(w).Encode([]map[string]any{})
				}
			}))
			defer srv.Close()

			c := NewClient(Config{
				BaseURL:    srv.URL,
				HTTPClient: srv.Client(),
				Options:    &ClientOptions{MaxRetries: Ptr(0)},
			})
			ctx := context.Background()

			switch tc.Endpoint {
			case "query":
				logErr(t, errOf(applyOps(t, tc.Table, c, tc.Operations).FetchUntyped(ctx)))
			case "ingest", "ingest_batch":
				logErr(t, errOf(c.From(tc.Table).Insert(ctx, insertArg(t, tc))))
			case "pipe":
				logErr(t, errOf(c.Pipe(tc.PipeName, tc.PipeParams).FetchUntyped(ctx)))
			case "sql":
				logErr(t, errOf(SQL[map[string]any](ctx, c, tc.SQL)))
			case "health":
				logErr(t, c.Sys.Health(ctx))
			case "schema_list":
				logErr(t, errOf(c.Schema.List(ctx)))
			case "schema_refresh":
				logErr(t, c.Schema.Refresh(ctx))
			case "policy_get":
				logErr(t, errOf(c.Policy.Get(ctx)))
			case "policy_set":
				logErr(t, c.Policy.Set(ctx, jsonArg[*Policy](t, "policy_body", tc.PolicyBody)))
			case "policy_validate":
				logErr(t, errOf(c.Policy.Validate(ctx, jsonArg[*Policy](t, "policy_body", tc.PolicyBody))))
			case "dlq_list":
				logErr(t, errOf(c.DLQ.List(ctx)))
			case "dlq_table":
				logErr(t, errOf(c.DLQ.Table(ctx, tc.Table)))
			case "pipes_list":
				logErr(t, errOf(c.Pipes.List(ctx)))
			case "pipes_get":
				logErr(t, errOf(c.Pipes.Get(ctx, tc.PipeName)))
			case "pipes_set":
				logErr(t, c.Pipes.Set(ctx, tc.PipeName, jsonArg[PipeDef](t, "pipe_def", tc.PipeDefBody)))
			case "pipes_delete":
				logErr(t, c.Pipes.Delete(ctx, tc.PipeName))
			default:
				// Hard failure, matching the TS runner: skipped cases break
				// cross-SDK parity.
				t.Fatalf("unhandled endpoint %q — wire it up in the dispatch switch", tc.Endpoint)
			}

			mu.Lock()
			defer mu.Unlock()

			wantEq := func(what, want, got string) {
				t.Helper()
				if want != "" && want != got {
					t.Errorf("%s: want %s, got %s", what, want, got)
				}
			}
			wantEq("method", tc.ExpectedMethod, capt.method)
			wantEq("content-type", tc.ExpectedContentType, capt.contentType)
			// Paths compare by meaning (see normalizePath) but report as written.
			if tc.ExpectedPath != "" && normalizePath(tc.ExpectedPath) != normalizePath(capt.path) {
				t.Errorf("path: want %s, got %s", tc.ExpectedPath, capt.path)
			}

			// Raw body: the NDJSON cases, where the byte layout is the point.
			if tc.ExpectedRawBody != nil {
				if capt.body != *tc.ExpectedRawBody {
					t.Errorf("raw body:\n  want: %s\n  got:  %s", *tc.ExpectedRawBody, capt.body)
				}
				return
			}
			if tc.ExpectedBody == nil || string(tc.ExpectedBody) == "null" {
				return
			}
			// Both sides decode into any, so every number is a float64 on both
			// sides and map key order is irrelevant to DeepEqual.
			var want, got any
			if err := json.Unmarshal(tc.ExpectedBody, &want); err != nil {
				t.Fatalf("parse expected_body: %v", err)
			}
			if err := json.Unmarshal([]byte(capt.body), &got); err != nil {
				t.Fatalf("parse captured body: %v (body: %s)", err, capt.body)
			}
			if !reflect.DeepEqual(want, got) {
				wantJSON, _ := json.MarshalIndent(want, "", "  ")
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				t.Errorf("body mismatch:\n  want: %s\n  got:  %s", wantJSON, gotJSON)
			}
		})
	}
}

// aggOps are the fixture ops that map one-to-one onto a (column, alias)
// aggregation method. Empty args fall through to each method's own defaults.
var aggOps = map[string]func(*QueryBuilder, string, string) *QueryBuilder{
	"count":         (*QueryBuilder).Count,
	"sum":           (*QueryBuilder).Sum,
	"avg":           (*QueryBuilder).Avg,
	"min":           (*QueryBuilder).Min,
	"max":           (*QueryBuilder).Max,
	"countDistinct": (*QueryBuilder).CountDistinct,
}

// applyOps replays the operation chain from the fixture onto a QueryBuilder.
// Fixtures always put select first (mirroring real usage), so rebuilding on
// select is safe and keeps this simple.
func applyOps(t *testing.T, table string, c *Client, ops []wireOp) *QueryBuilder {
	t.Helper()
	q := c.From(table).Select()

	for _, op := range ops {
		if agg, ok := aggOps[op.Method]; ok {
			q = agg(q, stringArg(op.Args, 0, ""), stringArg(op.Args, 1, ""))
			continue
		}
		switch op.Method {
		case "select":
			q = c.From(table).Select(toStringSlice(op.Args)...)
		case "selectAll":
			q = q.SelectAll()
		case "where":
			if len(op.Args) != 3 {
				t.Fatalf("where needs 3 args, got %d", len(op.Args))
			}
			col, colOK := op.Args[0].(string)
			rawOp, opOK := op.Args[1].(string)
			if !colOK || !opOK {
				t.Fatalf("where: want (string, string, any) args, got (%T, %T)", op.Args[0], op.Args[1])
			}
			q = q.Where(col, FilterOp(rawOp), op.Args[2])
		case "aggregate":
			q = q.Aggregate(stringArg(op.Args, 0, ""), stringArg(op.Args, 1, ""), stringArg(op.Args, 2, ""))
		case "groupBy":
			q = q.GroupBy(toStringSlice(op.Args)...)
		case "orderBy":
			q = q.OrderBy(stringArg(op.Args, 0, ""), stringArg(op.Args, 1, "asc"))
		case "limit":
			q = q.Limit(intArg(op.Args, 0))
		case "timeRange":
			q = q.TimeRange(stringArg(op.Args, 0, ""), stringArg(op.Args, 1, ""), stringArg(op.Args, 2, ""))
		case "cacheTTL":
			q = q.CacheTTL(intArg(op.Args, 0))
		}
	}
	return q
}

// insertArg returns the payload for an ingest case. Batch rows arrive from
// JSON as []any; hand Insert the []map[string]any a caller would pass so it
// takes the batch (NDJSON) path a typed slice takes.
func insertArg(t *testing.T, tc wireCase) any {
	t.Helper()
	if len(tc.Operations) == 0 || tc.Operations[0].Method != "insert" || len(tc.Operations[0].Args) == 0 {
		t.Fatalf("ingest case %q needs an insert operation with one arg", tc.Name)
	}
	arg := tc.Operations[0].Args[0]
	batch, ok := arg.([]any)
	if !ok {
		return arg
	}
	rows := make([]map[string]any, len(batch))
	for i, r := range batch {
		row, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("fixture row is not an object: %T", r)
		}
		rows[i] = row
	}
	return rows
}

func stringArg(args []any, i int, fallback string) string {
	if i >= len(args) {
		return fallback
	}
	s, ok := args[i].(string)
	if !ok {
		return fallback
	}
	return s
}

func intArg(args []any, i int) int {
	if i >= len(args) {
		return 0
	}
	switch v := args[i].(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}

func toStringSlice(args []any) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i], _ = a.(string)
	}
	return out
}

// normalizePath compares request URIs by meaning: same path, same decoded
// query values regardless of + vs %20 spelling or parameter order. A raw
// string replace would also rewrite literal + characters and stop asserting
// the encoding at all.
func normalizePath(p string) string {
	u, err := url.ParseRequestURI(p)
	if err != nil {
		return p
	}
	return u.Path + "?" + u.Query().Encode()
}
