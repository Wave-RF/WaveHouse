// Command wavehouse-codegen reads a WaveHouse server's /v1/schema endpoint
// and generates Go struct definitions for use with the wavehouse SDK.
//
// Usage:
//
//	wavehouse-codegen --url http://localhost:8080 --out ./db.go --auth <jwt>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go/format"
	"net/http"
	"os"
	"slices"
	"strings"
	"unicode"
)

type cliArgs struct {
	url  string
	out  string
	auth string
	pkg  string
}

func parseArgs() cliArgs {
	args := cliArgs{url: "http://localhost:8080", out: "./wavehouse_types.go", pkg: "main"}
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--url", "-u":
			i++
			if i < len(os.Args) {
				args.url = os.Args[i]
			}
		case "--out", "-o":
			i++
			if i < len(os.Args) {
				args.out = os.Args[i]
			}
		case "--auth", "-a":
			i++
			if i < len(os.Args) {
				args.auth = os.Args[i]
			}
		case "--package", "-p":
			i++
			if i < len(os.Args) {
				args.pkg = os.Args[i]
			}
		case "--help", "-h":
			fmt.Println(`wavehouse-codegen — Generate Go types from WaveHouse schema

Options:
  --url, -u       WaveHouse base URL (default: http://localhost:8080)
  --out, -o       Output .go file path (default: ./wavehouse_types.go)
  --auth, -a      Bearer token for authenticated /v1/schema endpoint
  --package, -p   Go package name (default: main)
  --help, -h      Show this help`)
			os.Exit(0)
		}
	}
	return args
}

type column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	IsNullable bool   `json:"is_nullable"`
	HasDefault bool   `json:"has_default"`
}

type tableSchema struct {
	Name    string   `json:"name"`
	Columns []column `json:"columns"`
}

func fetchSchemas(ctx context.Context, baseURL, auth string) (map[string]tableSchema, error) {
	url := strings.TrimRight(baseURL, "/") + "/v1/schema"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("schema fetch failed: HTTP %d", resp.StatusCode)
	}

	// Server returns either []tableSchema or map[string]tableSchema.
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	// Try array first.
	var arr []tableSchema
	if err := json.Unmarshal(raw, &arr); err == nil {
		m := make(map[string]tableSchema, len(arr))
		for _, t := range arr {
			m[t.Name] = t
		}
		return m, nil
	}
	var m map[string]tableSchema
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// chTypeToGo maps a ClickHouse type string (as reported by /v1/schema) to a
// Go type name suitable for a JSON struct field.
//
// We deliberately don't import clickhouse-go's type catalog
// (github.com/ClickHouse/clickhouse-go/v2/lib/column) for this. It's public
// and does expose a real ClickHouse-type-string parser —
// column.Type(chType).Column(name, sc).ScanType() — but it answers a
// different question than the one we're asking. That catalog maps to the Go
// types the *driver* scans query results into over the native protocol
// (time.Time for Date/DateTime*, uuid.UUID for UUID, decimal.Decimal for
// Decimal, net.IP for IPv4/IPv6, *big.Int for [U]Int128/256), not the types
// that round-trip cleanly through the JSON the /v1/schema and query
// endpoints actually speak. ClickHouse's JSON output renders DateTime as
// "2024-01-15 10:30:00" (no "T", no offset), which fails Go's default
// time.Time JSON unmarshaling; big integers and decimals are similarly
// rendered as JSON strings, not driver-native types. Adopting the driver's
// ScanType() as-is would produce generated structs that don't unmarshal the
// server's actual JSON, and would drag uuid/decimal/orb/net imports into
// generated output that today has zero non-stdlib dependencies. So we keep
// the hand-rolled JSON-oriented mapping below, informed by (but not bound
// to) the type set clickhouse-go's lib/column recognizes.
func chTypeToGo(chType string) string {
	// Unwrap Nullable → pointer.
	if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
		inner := chType[9 : len(chType)-1]
		return "*" + chTypeToGo(inner)
	}
	// Unwrap LowCardinality.
	if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
		return chTypeToGo(chType[15 : len(chType)-1])
	}
	// Unwrap SimpleAggregateFunction(func, InnerType) — readable columns in
	// AggregatingMergeTree/SummingMergeTree rollup tables. The value on the
	// wire is just InnerType; the aggregate function name only describes how
	// merges combine rows.
	if strings.HasPrefix(chType, "SimpleAggregateFunction(") && strings.HasSuffix(chType, ")") {
		inner := chType[len("SimpleAggregateFunction(") : len(chType)-1]
		if comma := findTopLevelComma(inner); comma != -1 {
			return chTypeToGo(strings.TrimSpace(inner[comma+1:]))
		}
		return "any"
	}
	// String-like.
	switch {
	case chType == "String",
		strings.HasPrefix(chType, "FixedString("),
		chType == "UUID",
		strings.HasPrefix(chType, "DateTime"),
		strings.HasPrefix(chType, "Date"),
		// Time/Time64 are ClickHouse's newer time-of-day types (distinct
		// from DateTime); same JSON-string-not-RFC3339 story applies.
		strings.HasPrefix(chType, "Time"),
		strings.HasPrefix(chType, "Enum8("),
		strings.HasPrefix(chType, "Enum16("),
		chType == "IPv4",
		chType == "IPv6":
		return "string"
	case chType == "Bool", chType == "Boolean":
		return "bool"
	}
	// Numeric — map widths honestly.
	switch {
	case chType == "UInt8":
		return "uint8"
	case chType == "UInt16":
		return "uint16"
	case chType == "UInt32":
		return "uint32"
	case chType == "UInt64":
		return "uint64"
	case chType == "Int8":
		return "int8"
	case chType == "Int16":
		return "int16"
	case chType == "Int32":
		return "int32"
	case chType == "Int64":
		return "int64"
	case chType == "Float32":
		return "float32"
	case chType == "Float64":
		return "float64"
	case chType == "BFloat16":
		return "float32"
	case strings.HasPrefix(chType, "Decimal"),
		strings.HasPrefix(chType, "UInt128"),
		strings.HasPrefix(chType, "UInt256"),
		strings.HasPrefix(chType, "Int128"),
		strings.HasPrefix(chType, "Int256"):
		return "string" // big numbers are strings in JSON
	}
	// Array.
	if strings.HasPrefix(chType, "Array(") && strings.HasSuffix(chType, ")") {
		inner := chType[6 : len(chType)-1]
		return "[]" + chTypeToGo(inner)
	}
	// Map.
	if strings.HasPrefix(chType, "Map(") && strings.HasSuffix(chType, ")") {
		inner := chType[4 : len(chType)-1]
		comma := findTopLevelComma(inner)
		if comma != -1 {
			k := chTypeToGo(strings.TrimSpace(inner[:comma]))
			v := chTypeToGo(strings.TrimSpace(inner[comma+1:]))
			return "map[" + k + "]" + v
		}
		return "map[string]any"
	}
	return "any"
}

func findTopLevelComma(s string) int {
	depth := 0
	for i := range len(s) {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func pascalCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == ' ' || r == '.'
	})
	var sb strings.Builder
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		runes := []rune(p)
		runes[0] = unicode.ToUpper(runes[0])
		sb.WriteString(string(runes))
	}
	result := sb.String()
	if result == "" {
		return result
	}
	// Go identifiers can't start with a digit (e.g. a table named
	// "2fa_events" would otherwise produce the invalid identifier
	// "2faEvents"). Prefix with "X" to keep it a valid, exported name.
	if unicode.IsDigit([]rune(result)[0]) {
		result = "X" + result
	}
	return result
}

func generate(schemas map[string]tableSchema, pkg string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "// Code generated by wavehouse-codegen. DO NOT EDIT.\n\npackage %s\n\n", pkg)

	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		schema := schemas[name]
		typeName := pascalCase(name) + "Row"
		fmt.Fprintf(&sb, "// %s represents a row in the %q table.\ntype %s struct {\n", typeName, name, typeName)
		for _, col := range schema.Columns {
			goType := chTypeToGo(col.Type)
			fieldName := pascalCase(col.Name)
			jsonTag := col.Name
			if col.HasDefault {
				jsonTag += ",omitempty"
			}
			fmt.Fprintf(&sb, "\t%s %s `json:%q`\n", fieldName, goType, jsonTag)
		}
		sb.WriteString("}\n\n")
	}

	return sb.String()
}

func main() {
	args := parseArgs()
	fmt.Printf("Fetching schema from %s...\n", args.url)

	schemas, err := fetchSchemas(context.Background(), args.url, args.auth)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if len(schemas) == 0 {
		fmt.Fprintln(os.Stderr, "No tables found. Is WaveHouse running with tables in ClickHouse?")
		os.Exit(1)
	}

	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	slices.Sort(names)
	fmt.Printf("Found %d table(s): %s\n", len(schemas), strings.Join(names, ", "))

	output := generate(schemas, args.pkg)

	// gofmt the output. A failure here means the generated source is not
	// valid Go (e.g. a table/column name produced an invalid identifier);
	// don't write unusable output and claim success.
	formatted, err := format.Source([]byte(output))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: generated code is not valid Go: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(args.out, formatted, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", args.out, err)
		os.Exit(1)
	}

	fmt.Printf("✓ Types written to %s\n", args.out)
}
