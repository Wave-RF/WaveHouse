package main

import (
	"context"
	"encoding/json"
	"errors"
	"go/format"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func TestChTypeToGo(t *testing.T) {
	tests := []struct {
		ch   string
		want string
	}{
		{"String", "string"},
		{"FixedString(16)", "string"},
		{"UUID", "string"},
		{"DateTime64(3, 'UTC')", "string"},
		{"Date", "string"},
		{"Time64(3)", "string"},
		{"Enum8('a' = 1)", "string"},
		{"IPv4", "string"},
		{"Bool", "bool"},
		{"Boolean", "bool"},
		{"UInt8", "uint8"},
		{"UInt16", "uint16"},
		{"UInt32", "uint32"},
		{"Int8", "int8"},
		{"Int32", "int32"},
		{"Float32", "float32"},
		{"BFloat16", "float32"},
		{"Float64", "float64"},
		// /v1/query re-marshals server-side: 64-bit ints arrive unquoted.
		{"UInt64", "uint64"},
		{"Int64", "int64"},
		{"UInt128", "json.Number"},
		{"Int256", "json.Number"},
		{"Decimal(18, 4)", "string"},
		{"Nullable(Int32)", "*int32"},
		{"Nullable(Int64)", "*int64"},
		{"LowCardinality(String)", "string"},
		{"LowCardinality(Nullable(String))", "*string"},
		{"SimpleAggregateFunction(sum, UInt32)", "uint32"},
		{"SimpleAggregateFunction(any)", "any"},
		{"Array(String)", "[]string"},
		{"Array(Nullable(Int32))", "[]*int32"},
		// []uint8 is []byte → base64 on marshal; RawMessage round-trips both
		// the ingest array form and the (currently base64) query response.
		{"Array(UInt8)", "json.RawMessage"},
		{"Map(String, UInt32)", "map[string]uint32"},
		{"Map(String, Map(UInt32, String))", "map[string]map[uint32]string"},
		{"Tuple(String, UInt8)", "any"},
		{"SomethingNew", "any"},
	}
	for _, tt := range tests {
		if got := chTypeToGo(tt.ch); got != tt.want {
			t.Errorf("chTypeToGo(%q) = %q, want %q", tt.ch, got, tt.want)
		}
	}
}

func TestPascalCase(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"clicks", "Clicks"},
		{"user_id", "UserId"},
		{"received_timestamp", "ReceivedTimestamp"},
		{"multi-part.name here", "MultiPartNameHere"},
		{"2fa_events", "X2faEvents"}, // leading digit gets the X prefix
		{"", ""},
	}
	for _, tt := range tests {
		if got := pascalCase(tt.in); got != tt.want {
			t.Errorf("pascalCase(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestFindTopLevelComma(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{"String, UInt32", 6},
		{"Map(String, String), UInt8", 19},
		{"NoComma", -1},
	}
	for _, tt := range tests {
		if got := findTopLevelComma(tt.in); got != tt.want {
			t.Errorf("findTopLevelComma(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestGenerate_Basic(t *testing.T) {
	out, err := generate(map[string]tableSchema{
		"clicks": {Name: "clicks", Columns: []column{
			{Name: "page", Type: "String"},
			{Name: "score", Type: "Float64"},
			{Name: "big", Type: "UInt128"},
			{Name: "received_timestamp", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		}},
	}, "myapp")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"package myapp",
		`import "encoding/json"`, // dragged in by the json.Number field below
		"type ClicksRow struct {",
		"Page string `json:\"page\"`",
		"Score float64 `json:\"score\"`",
		"Big json.Number `json:\"big\"`",
		// Defaulted column: pointer + omitempty so an explicit zero still sends.
		"ReceivedTimestamp *string `json:\"received_timestamp,omitempty\"`",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("generated output missing %q:\n%s", want, out)
		}
	}
	if _, err := format.Source([]byte(out)); err != nil {
		t.Fatalf("generated output is not valid Go: %v", err)
	}
}

func TestGenerate_FieldCollisionFails(t *testing.T) {
	_, err := generate(map[string]tableSchema{
		"t": {Name: "t", Columns: []column{
			{Name: "user_id", Type: "String"},
			{Name: "userId", Type: "String"},
		}},
	}, "main")
	if err == nil || !strings.Contains(err.Error(), "UserId") {
		t.Fatalf("want field-collision error naming UserId, got %v", err)
	}
}

func TestGenerate_TypeCollisionFails(t *testing.T) {
	_, err := generate(map[string]tableSchema{
		"2fa":  {Name: "2fa", Columns: []column{{Name: "a", Type: "String"}}},
		"x2fa": {Name: "x2fa", Columns: []column{{Name: "a", Type: "String"}}},
	}, "main")
	if err == nil || !strings.Contains(err.Error(), "X2faRow") {
		t.Fatalf("want type-collision error naming X2faRow, got %v", err)
	}
}

// TestGeneratedShapeDecodesStructuredQueryPayload asserts the mapping choices
// actually decode what /v1/query emits: the server scans ClickHouse values
// into Go types and re-marshals, so 64-bit ints are unquoted numbers,
// 128/256-bit ints are unquoted arbitrary-width numbers, and Decimals are
// quoted strings.
func TestGeneratedShapeDecodesStructuredQueryPayload(t *testing.T) {
	type row struct {
		ID    uint64      `json:"id"`
		Delta int64       `json:"delta"`
		Big   json.Number `json:"big"`
		Price string      `json:"price"`
	}
	payload := `[{"id":18446744073709551615,"delta":-9007199254740993,"big":170141183460469231731687303715884105727,"price":"12.3400"}]`
	var rows []row
	if err := json.Unmarshal([]byte(payload), &rows); err != nil {
		t.Fatalf("generated shape failed to decode /v1/query payload: %v", err)
	}
	if rows[0].ID != 18446744073709551615 || rows[0].Delta != -9007199254740993 {
		t.Fatalf("64-bit values corrupted: %+v", rows[0])
	}
	if rows[0].Big.String() != "170141183460469231731687303715884105727" {
		t.Fatalf("128-bit value corrupted: %s", rows[0].Big)
	}
}

// withArgs points os.Args at args for the duration of the test. parseArgs and
// flagValue read the global directly, so tests driving them must not run in
// parallel.
func withArgs(t *testing.T, args []string) {
	t.Helper()
	saved := os.Args
	t.Cleanup(func() { os.Args = saved })
	os.Args = args
}

func TestFlagValue(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		want  string
		wantI int
	}{
		{name: "value follows the flag", args: []string{"cg", "--url", "http://h:9000"}, want: "http://h:9000", wantI: 2},
		// There is no lookahead: a flag-shaped value is consumed as the value.
		{name: "flag-shaped value", args: []string{"cg", "--out", "--package"}, want: "--package", wantI: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withArgs(t, tt.args)
			i := 1
			if got := flagValue(&i); got != tt.want {
				t.Errorf("flagValue() = %q, want %q", got, tt.want)
			}
			if i != tt.wantI {
				t.Errorf("index advanced to %d, want %d", i, tt.wantI)
			}
		})
	}
}

func TestParseArgs(t *testing.T) {
	// Every case differs from the defaults parseArgs starts with by a field or
	// two, so deriving the wants keeps each row about what it changes.
	defaults := cliArgs{url: "http://localhost:8080", out: "./wavehouse_types.go", pkg: "main"}
	allFlags := cliArgs{url: "http://h:9000", out: "./types.go", auth: "argv-token", pkg: "db"}
	secondURL, envAuth, argvAuth := defaults, defaults, defaults
	secondURL.url = "http://second"
	envAuth.auth = "env-token"
	argvAuth.auth = "argv-token"
	tests := []struct {
		name string
		args []string
		env  string // WAVEHOUSE_AUTH
		want cliArgs
	}{
		{name: "no arguments uses defaults", args: []string{"cg"}, want: defaults},
		{
			name: "long flags",
			args: []string{"cg", "--url", "http://h:9000", "--out", "./types.go", "--auth", "argv-token", "--package", "db"},
			want: allFlags,
		},
		{
			name: "short flags",
			args: []string{"cg", "-u", "http://h:9000", "-o", "./types.go", "-a", "argv-token", "-p", "db"},
			want: allFlags,
		},
		{name: "later flag wins", args: []string{"cg", "-u", "http://first", "--url", "http://second"}, want: secondURL},
		{name: "WAVEHOUSE_AUTH fills an unset --auth", args: []string{"cg"}, env: "env-token", want: envAuth},
		{name: "--auth beats WAVEHOUSE_AUTH", args: []string{"cg", "--auth", "argv-token"}, env: "env-token", want: argvAuth},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("WAVEHOUSE_AUTH", tt.env) // empty reads back the same as unset
			withArgs(t, tt.args)
			if got := parseArgs(); got != tt.want {
				t.Errorf("parseArgs() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// parseArgsChildEnv carries the argv under test to the re-executed child.
const parseArgsChildEnv = "WAVEHOUSE_CODEGEN_TEST_ARGS"

// TestParseArgsExitPaths covers the branches that end in os.Exit, which can
// only be observed from outside the process: each case re-runs this test
// binary with the argv under test and asserts on the child's exit code and
// output.
func TestParseArgsExitPaths(t *testing.T) {
	if raw, ok := os.LookupEnv(parseArgsChildEnv); ok {
		withArgs(t, append([]string{"wavehouse-codegen"}, strings.Fields(raw)...))
		parseArgs()
		t.Fatal("parseArgs returned instead of exiting")
	}
	tests := []struct {
		name     string
		args     string
		wantCode int
		wantOut  string
	}{
		{name: "missing value for a long flag", args: "--url", wantCode: 2, wantOut: "missing value for --url"},
		{name: "missing value for a short flag", args: "-o", wantCode: 2, wantOut: "missing value for -o"},
		{name: "missing value after a satisfied flag", args: "--url http://h:9000 --package", wantCode: 2, wantOut: "missing value for --package"},
		{name: "unknown argument", args: "--nope", wantCode: 2, wantOut: `unknown argument "--nope"`},
		{name: "bare value with no flag", args: "stray", wantCode: 2, wantOut: `unknown argument "stray"`},
		{name: "help exits cleanly", args: "--help", wantCode: 0, wantOut: "Generate Go types from WaveHouse schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Re-executing this test binary is the standard way to observe
			// an os.Exit path; the argv is fixed by the table above.
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestParseArgsExitPaths$") //nolint:gosec // the command is this test binary, not user input
			cmd.Env = append(os.Environ(), parseArgsChildEnv+"="+tt.args)
			out, err := cmd.CombinedOutput()
			code := 0
			var exitErr *exec.ExitError
			switch {
			case errors.As(err, &exitErr):
				code = exitErr.ExitCode()
			case err != nil:
				t.Fatalf("run child: %v", err)
			}
			if code != tt.wantCode {
				t.Errorf("child exit code = %d, want %d\n%s", code, tt.wantCode, out)
			}
			if !strings.Contains(string(out), tt.wantOut) {
				t.Errorf("child output missing %q:\n%s", tt.wantOut, out)
			}
		})
	}
}

// schemaRequest is what the stub server saw. It travels over a buffered
// channel rather than a shared variable so -race sees the edge between the
// server goroutine and the test.
type schemaRequest struct {
	path string
	auth string
}

// schemaServer answers every request with body under status (0 means 200).
func schemaServer(t *testing.T, status int, body string) (string, <-chan schemaRequest) {
	t.Helper()
	seen := make(chan schemaRequest, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- schemaRequest{path: r.URL.Path, auth: r.Header.Get("Authorization")}
		if status != 0 {
			w.WriteHeader(status)
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, seen
}

func TestFetchSchemas(t *testing.T) {
	const clicksJSON = `{"name":"clicks","columns":[{"name":"page","type":"String"},{"name":"ts","type":"DateTime","has_default":true}]}`
	clicks := tableSchema{Name: "clicks", Columns: []column{
		{Name: "page", Type: "String"},
		{Name: "ts", Type: "DateTime", HasDefault: true},
	}}
	tests := []struct {
		name     string
		suffix   string // appended to the stub server's base URL
		auth     string
		status   int
		body     string
		want     map[string]tableSchema
		wantAuth string
		wantErr  string
	}{
		{
			name: "array response with bearer auth", auth: "tok-123", body: "[" + clicksJSON + "]",
			want: map[string]tableSchema{"clicks": clicks}, wantAuth: "Bearer tok-123",
		},
		{
			name: "map response without auth", body: `{"clicks":` + clicksJSON + `}`,
			want: map[string]tableSchema{"clicks": clicks},
		},
		{name: "trailing slash trimmed from base URL", suffix: "/", body: "[]", want: map[string]tableSchema{}},
		{name: "malformed JSON", body: `[{"name":`, wantErr: "read schema response"},
		{name: "JSON that is neither array nor map", body: `"nope"`, wantErr: "decode schema JSON"},
		{name: "non-200 status", status: http.StatusInternalServerError, body: "boom", wantErr: "schema fetch failed: HTTP 500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, seen := schemaServer(t, tt.status, tt.body)
			got, err := fetchSchemas(t.Context(), base+tt.suffix, tt.auth)
			switch {
			case tt.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("fetchSchemas() error = %v, want one containing %q", err, tt.wantErr)
				}
			case err != nil:
				t.Fatalf("fetchSchemas() error = %v", err)
			case !reflect.DeepEqual(got, tt.want):
				t.Errorf("fetchSchemas() = %+v, want %+v", got, tt.want)
			}
			select {
			case req := <-seen:
				if req.path != "/v1/ops/schema" {
					t.Errorf("requested path = %q, want /v1/ops/schema", req.path)
				}
				if req.auth != tt.wantAuth {
					t.Errorf("Authorization header = %q, want %q", req.auth, tt.wantAuth)
				}
			default:
				t.Error("stub server never saw a request")
			}
		})
	}
}

func TestFetchSchemasRequestErrors(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		baseURL string
		wantErr string
	}{
		{name: "unparseable base URL", ctx: t.Context(), baseURL: "http://%zz", wantErr: "build schema request"},
		{name: "transport failure", ctx: canceled, baseURL: "http://127.0.0.1:1", wantErr: "fetch schema from"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := fetchSchemas(tt.ctx, tt.baseURL, ""); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("fetchSchemas() error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestSortedKeys(t *testing.T) {
	got := sortedKeys(map[string]tableSchema{"clicks": {}, "acks": {}, "views": {}})
	if want := []string{"acks", "clicks", "views"}; !reflect.DeepEqual(got, want) {
		t.Errorf("sortedKeys() = %v, want %v", got, want)
	}
}
