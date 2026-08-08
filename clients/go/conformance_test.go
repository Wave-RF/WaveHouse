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

// logCallErr surfaces SDK-call errors that the conformance harness otherwise
// ignores — the assertions only inspect the captured request, but when a call
// fails before sending, the failure message should name the real cause.
func logCallErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Logf("SDK call returned error (request may still be valid): %v", err)
	}
}

// wireCasesJSON embeds the shared wire-format conformance fixture so the
// test binary is self-contained: it works from a module archive or a
// standalone checkout without depending on paths outside the Go module.
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

func loadWireCases(t *testing.T) []wireCase {
	t.Helper()
	var cases []wireCase
	if err := json.Unmarshal(wireCasesJSON, &cases); err != nil {
		t.Fatalf("parse wire_cases.json: %v", err)
	}
	return cases
}

// captured holds the HTTP request details from a single SDK call.
type captured struct {
	method      string
	path        string // path + query string
	contentType string
	body        string
}

func TestConformance_WireFormat(t *testing.T) {
	cases := loadWireCases(t)

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
				case strings.HasPrefix(r.URL.Path, "/v1/dlq"):
					_ = json.NewEncoder(w).Encode(DLQStats{Tables: map[string]int{}, Total: 0})
				case strings.HasPrefix(r.URL.Path, "/v1/schema") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode([]TableSchema{})
				case r.URL.Path == "/v1/admin/policy/validate" && r.Method == "POST":
					_ = json.NewEncoder(w).Encode(ValidationResult{Valid: true})
				case strings.HasPrefix(r.URL.Path, "/v1/admin/policy") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode(Policy{Tables: map[string]TablePolicy{}})
				case strings.HasPrefix(r.URL.Path, "/v1/admin/pipes/") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode(Pipe{Name: "test", SQL: "SELECT 1"})
				case r.URL.Path == "/v1/admin/pipes" && r.Method == "GET":
					_ = json.NewEncoder(w).Encode([]Pipe{})
				default:
					_ = json.NewEncoder(w).Encode([]map[string]any{})
				}
			}))
			defer srv.Close()

			c := NewClient(Config{
				BaseURL:    srv.URL,
				HTTPClient: srv.Client(),
				Options:    &ClientOptions{MaxRetries: 0},
			})
			ctx := context.Background()

			// Execute the case.
			switch tc.Endpoint {
			case "query":
				q := applyOps(t, tc.Table, c, tc.Operations)
				_, err := q.FetchUntyped(ctx)
				logCallErr(t, err)

			case "ingest":
				if len(tc.Operations) == 0 || tc.Operations[0].Method != "insert" {
					t.Fatalf("ingest case %q has no insert operation", tc.Name)
				}
				if len(tc.Operations[0].Args) == 0 {
					t.Fatal("insert needs 1 arg, got 0")
				}
				data := tc.Operations[0].Args[0]
				_, err := c.From(tc.Table).Insert(ctx, data)
				logCallErr(t, err)

			case "ingest_batch":
				if len(tc.Operations) == 0 || tc.Operations[0].Method != "insert" {
					t.Fatalf("ingest_batch case %q has no insert operation", tc.Name)
				}
				if len(tc.Operations[0].Args) == 0 {
					t.Fatal("insert needs 1 arg, got 0")
				}
				rawArr, ok := tc.Operations[0].Args[0].([]any)
				if !ok {
					t.Fatalf("batch insert args[0] is not an array")
				}
				rows := make([]map[string]any, len(rawArr))
				for i, r := range rawArr {
					rows[i] = toStringMap(t, r)
				}
				_, batchErr := c.From(tc.Table).Insert(ctx, rows)
				logCallErr(t, batchErr)

			case "pipe":
				p := c.Pipe(tc.PipeName, tc.PipeParams)
				_, err := p.FetchUntyped(ctx)
				logCallErr(t, err)

			case "sql":
				_, err := SQL[map[string]any](ctx, c, tc.SQL)
				logCallErr(t, err)

			case "health":
				logCallErr(t, c.Sys.Health(ctx))

			case "schema_list":
				_, err := c.Schema.List(ctx)
				logCallErr(t, err)

			case "schema_refresh":
				logCallErr(t, c.Schema.Refresh(ctx))

			case "policy_get":
				_, err := c.Policy.Get(ctx)
				logCallErr(t, err)

			case "policy_set":
				var pol Policy
				if err := json.Unmarshal(tc.PolicyBody, &pol); err != nil {
					t.Fatalf("parse policy_body: %v", err)
				}
				logCallErr(t, c.Policy.Set(ctx, &pol))

			case "policy_validate":
				var pol Policy
				if err := json.Unmarshal(tc.PolicyBody, &pol); err != nil {
					t.Fatalf("parse policy_body: %v", err)
				}
				_, err := c.Policy.Validate(ctx, &pol)
				logCallErr(t, err)

			case "dlq_list":
				_, err := c.DLQ.List(ctx)
				logCallErr(t, err)

			case "dlq_table":
				_, err := c.DLQ.Table(ctx, tc.Table)
				logCallErr(t, err)

			case "pipes_list":
				_, err := c.Pipes.List(ctx)
				logCallErr(t, err)

			case "pipes_get":
				_, err := c.Pipes.Get(ctx, tc.PipeName)
				logCallErr(t, err)

			case "pipes_set":
				var def PipeDef
				if err := json.Unmarshal(tc.PipeDefBody, &def); err != nil {
					t.Fatalf("parse pipe_def: %v", err)
				}
				logCallErr(t, c.Pipes.Set(ctx, tc.PipeName, def))

			case "pipes_delete":
				logCallErr(t, c.Pipes.Delete(ctx, tc.PipeName))

			default:
				// Hard failure, matching the TS runner: skipped cases break
				// cross-SDK parity.
				t.Fatalf("unhandled endpoint %q — wire it up in the dispatch switch", tc.Endpoint)
			}

			// Verify method.
			mu.Lock()
			defer mu.Unlock()
			if tc.ExpectedMethod != "" && capt.method != tc.ExpectedMethod {
				t.Errorf("method: want %s, got %s", tc.ExpectedMethod, capt.method)
			}

			// Verify path.
			if tc.ExpectedPath != "" {
				// Normalize: the SDK may use different encoding (+ vs %20).
				wantPath := normalizePath(tc.ExpectedPath)
				gotPath := normalizePath(capt.path)
				if wantPath != gotPath {
					t.Errorf("path: want %s, got %s", tc.ExpectedPath, capt.path)
				}
			}

			// Verify content type.
			if tc.ExpectedContentType != "" && capt.contentType != tc.ExpectedContentType {
				t.Errorf("content-type: want %s, got %s", tc.ExpectedContentType, capt.contentType)
			}

			// Verify raw body (for NDJSON).
			if tc.ExpectedRawBody != nil {
				if capt.body != *tc.ExpectedRawBody {
					t.Errorf("raw body:\n  want: %s\n  got:  %s", *tc.ExpectedRawBody, capt.body)
				}
				return
			}

			// Verify JSON body.
			if tc.ExpectedBody != nil && string(tc.ExpectedBody) != "null" {
				var want, got any
				if err := json.Unmarshal(tc.ExpectedBody, &want); err != nil {
					t.Fatalf("parse expected_body: %v", err)
				}
				if err := json.Unmarshal([]byte(capt.body), &got); err != nil {
					t.Fatalf("parse captured body: %v (body: %s)", err, capt.body)
				}
				if !deepEqualJSON(want, got) {
					wantJSON, _ := json.MarshalIndent(want, "", "  ")
					gotJSON, _ := json.MarshalIndent(got, "", "  ")
					t.Errorf("body mismatch:\n  want: %s\n  got:  %s", wantJSON, gotJSON)
				}
			}
		})
	}
}

// applyOps replays the operation chain from the fixture onto a QueryBuilder.
// Fixtures always put select first (mirroring real usage), so rebuilding on
// select is safe and keeps this simple.
func applyOps(t *testing.T, table string, c *Client, ops []wireOp) *QueryBuilder {
	t.Helper()
	q := c.From(table).Select()

	for _, op := range ops {
		switch op.Method {
		case "select":
			q = c.From(table).Select(toStringSlice(op.Args)...)
		case "selectAll":
			q = q.SelectAll()
		case "where":
			if len(op.Args) != 3 {
				t.Fatalf("where needs 3 args, got %d", len(op.Args))
			}
			col, ok := op.Args[0].(string)
			if !ok {
				t.Fatalf("where: column arg is %T, want string", op.Args[0])
			}
			rawOp, ok := op.Args[1].(string)
			if !ok {
				t.Fatalf("where: operator arg is %T, want string", op.Args[1])
			}
			opStr := FilterOp(rawOp)
			val := op.Args[2]
			q = q.Where(col, opStr, val)
		case "count":
			col, alias := stringArg(op.Args, 0, "*"), stringArg(op.Args, 1, "count")
			q = q.Count(col, alias)
		case "sum":
			col, alias := stringArg(op.Args, 0, ""), stringArg(op.Args, 1, "")
			q = q.Sum(col, alias)
		case "avg":
			col, alias := stringArg(op.Args, 0, ""), stringArg(op.Args, 1, "")
			q = q.Avg(col, alias)
		case "min":
			col, alias := stringArg(op.Args, 0, ""), stringArg(op.Args, 1, "")
			q = q.Min(col, alias)
		case "max":
			col, alias := stringArg(op.Args, 0, ""), stringArg(op.Args, 1, "")
			q = q.Max(col, alias)
		case "countDistinct":
			col, alias := stringArg(op.Args, 0, ""), stringArg(op.Args, 1, "")
			q = q.CountDistinct(col, alias)
		case "aggregate":
			fn := stringArg(op.Args, 0, "")
			col := stringArg(op.Args, 1, "")
			alias := stringArg(op.Args, 2, "")
			q = q.Aggregate(fn, col, alias)
		case "groupBy":
			cols := toStringSlice(op.Args)
			q = q.GroupBy(cols...)
		case "orderBy":
			col := stringArg(op.Args, 0, "")
			dir := stringArg(op.Args, 1, "asc")
			q = q.OrderBy(col, dir)
		case "limit":
			n := intArg(op.Args, 0)
			q = q.Limit(n)
		case "timeRange":
			col := stringArg(op.Args, 0, "")
			since := stringArg(op.Args, 1, "")
			until := stringArg(op.Args, 2, "")
			q = q.TimeRange(col, since, until)
		case "cacheTTL":
			n := intArg(op.Args, 0)
			q = q.CacheTTL(n)
		}
	}
	return q
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

func toStringMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("fixture row is not an object: %T", v)
	}
	return m
}

// deepEqualJSON compares two JSON-decoded values, treating float64 ints as equal
// to ints (JSON numbers decode as float64 in Go).
func deepEqualJSON(a, b any) bool {
	return reflect.DeepEqual(normalizeJSON(a), normalizeJSON(b))
}

func normalizeJSON(v any) any {
	switch val := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(val))
		for k, v := range val {
			m[k] = normalizeJSON(v)
		}
		return m
	case []any:
		s := make([]any, len(val))
		for i, v := range val {
			s[i] = normalizeJSON(v)
		}
		return s
	case float64:
		// Normalize integer-valued floats to int for comparison.
		if val == float64(int64(val)) {
			return int64(val)
		}
		return val
	default:
		return val
	}
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
