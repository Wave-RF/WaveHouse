package main

import (
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
		// 64-bit and wider integers are quoted in ClickHouse JSON output.
		{"UInt64", "string"},
		{"Int64", "string"},
		{"UInt128", "string"},
		{"Int256", "string"},
		{"Decimal(18, 4)", "string"},
		{"Nullable(Int32)", "*int32"},
		{"Nullable(Int64)", "*string"},
		{"LowCardinality(String)", "string"},
		{"LowCardinality(Nullable(String))", "*string"},
		{"SimpleAggregateFunction(sum, UInt32)", "uint32"},
		{"SimpleAggregateFunction(any)", "any"},
		{"Array(String)", "[]string"},
		{"Array(Nullable(Int32))", "[]*int32"},
		// []uint8 is []byte → base64 on marshal; must widen.
		{"Array(UInt8)", "[]uint16"},
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
