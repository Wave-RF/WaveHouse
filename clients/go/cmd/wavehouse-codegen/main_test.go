package main

import (
	"encoding/json"
	"go/format"
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
			{Name: "received_timestamp", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		}},
	}, "myapp")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"package myapp",
		"type ClicksRow struct {",
		"Page string `json:\"page\"`",
		"Score float64 `json:\"score\"`",
		// Defaulted column: pointer + omitempty so an explicit zero still sends.
		"ReceivedTimestamp *string `json:\"received_timestamp,omitempty\"`",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("generated output missing %q:\n%s", want, out)
		}
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

func TestGenerate_JSONNumberImport(t *testing.T) {
	out, err := generate(map[string]tableSchema{
		"t": {Name: "t", Columns: []column{{Name: "big", Type: "UInt128"}}},
	}, "main")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `import "encoding/json"`) {
		t.Fatalf("json.Number field without encoding/json import:\n%s", out)
	}
	if _, err := format.Source([]byte(out)); err != nil {
		t.Fatalf("generated output is not valid Go: %v", err)
	}
}
