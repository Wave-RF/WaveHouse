// Command wavehouse-codegen reads a WaveHouse server's /v1/ops/schema endpoint
// and generates Go struct definitions for use with the wavehouse SDK.
//
// Usage:
//
//	WAVEHOUSE_AUTH=<jwt> wavehouse-codegen --url http://localhost:8080 --out ./db.go
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
	"time"
	"unicode"
)

type cliArgs struct {
	url  string
	out  string
	auth string
	pkg  string
}

// flagValue consumes and returns the value following os.Args[*i], exiting
// rather than silently falling back to the default when it is missing.
func flagValue(i *int) string {
	flag := os.Args[*i]
	*i++
	if *i >= len(os.Args) {
		fmt.Fprintf(os.Stderr, "Error: missing value for %s (use --help)\n", flag)
		os.Exit(2)
	}
	return os.Args[*i]
}

func parseArgs() cliArgs {
	args := cliArgs{url: "http://localhost:8080", out: "./wavehouse_types.go", pkg: "main"}
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--url", "-u":
			args.url = flagValue(&i)
		case "--out", "-o":
			args.out = flagValue(&i)
		case "--auth", "-a":
			args.auth = flagValue(&i)
		case "--package", "-p":
			args.pkg = flagValue(&i)
		case "--help", "-h":
			fmt.Println(`wavehouse-codegen — Generate Go types from WaveHouse schema

Options:
  --url, -u       WaveHouse base URL (default: http://localhost:8080)
  --out, -o       Output .go file path (default: ./wavehouse_types.go)
  --auth, -a      Bearer token for authenticated /v1/ops/schema endpoint
                  (prefer the WAVEHOUSE_AUTH env var — argv leaks into
                  shell history and process listings)
  --package, -p   Go package name (default: main)
  --help, -h      Show this help`)
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown argument %q (use --help)\n", os.Args[i])
			os.Exit(2)
		}
	}
	if args.auth == "" {
		args.auth = os.Getenv("WAVEHOUSE_AUTH")
	}
	return args
}

type column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	HasDefault bool   `json:"has_default"`
}

type tableSchema struct {
	Name    string   `json:"name"`
	Columns []column `json:"columns"`
}

func fetchSchemas(ctx context.Context, baseURL, auth string) (map[string]tableSchema, error) {
	url := strings.TrimRight(baseURL, "/") + "/v1/ops/schema"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("build schema request for %s: %w", url, err)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch schema from %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("schema fetch failed: HTTP %d", resp.StatusCode)
	}

	// The server returns either []tableSchema or map[string]tableSchema.
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("read schema response: %w", err)
	}
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
		return nil, fmt.Errorf("decode schema JSON: %w", err)
	}
	return m, nil
}

// chTypeToGo maps a ClickHouse type string (as reported by /v1/ops/schema) to
// a Go type name suitable for a JSON struct field. The mapping targets what
// round-trips through the server's JSON, not clickhouse-go's native scan
// types, and keeps generated output free of non-stdlib imports.
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
	// Unwrap SimpleAggregateFunction(func, InnerType): the wire value is just
	// InnerType, the function name only describes how merges combine rows.
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
		strings.HasPrefix(chType, "Time"), // Time/Time64 time-of-day types
		strings.HasPrefix(chType, "Enum8("),
		strings.HasPrefix(chType, "Enum16("),
		chType == "IPv4",
		chType == "IPv6":
		return "string"
	case chType == "Bool", chType == "Boolean":
		return "bool"
	}
	// Generated structs target /v1/query and /v1/pipes/*, where the server
	// re-marshals values so 64-bit integers arrive as unquoted JSON numbers.
	// /v1/ops/query forwards ClickHouse's own JSON, which quotes them — use
	// SQL[map[string]any] there.
	if mapped, ok := map[string]string{
		"UInt8": "uint8", "UInt16": "uint16", "UInt32": "uint32", "UInt64": "uint64",
		"Int8": "int8", "Int16": "int16", "Int32": "int32", "Int64": "int64",
		"Float32": "float32", "Float64": "float64", "BFloat16": "float32",
	}[chType]; ok {
		return mapped
	}
	switch {
	case strings.HasPrefix(chType, "Decimal"):
		return "string" // marshaled as a quoted string on the structured path
	case strings.HasPrefix(chType, "UInt128"),
		strings.HasPrefix(chType, "UInt256"),
		strings.HasPrefix(chType, "Int128"),
		strings.HasPrefix(chType, "Int256"):
		// Arbitrary-width unquoted JSON numbers; int64/uint64 would overflow.
		return "json.Number"
	}
	if strings.HasPrefix(chType, "Array(") && strings.HasSuffix(chType, ")") {
		inner := chTypeToGo(chType[6 : len(chType)-1])
		// Array(UInt8) is asymmetric on the wire — ingest takes a JSON array,
		// /v1/query returns base64 (#436) — and only json.RawMessage
		// round-trips both without a decode error.
		if inner == "uint8" {
			return "json.RawMessage"
		}
		return "[]" + inner
	}
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
	// Go identifiers can't start with a digit, so "2fa_events" needs a prefix
	// to stay a valid exported name.
	if unicode.IsDigit(rune(result[0])) { // digits are ASCII; no rune-slice needed
		result = "X" + result
	}
	return result
}

func sortedKeys(m map[string]tableSchema) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func generate(schemas map[string]tableSchema, pkg string) (string, error) {
	var sb strings.Builder

	names := sortedKeys(schemas)

	// json.Number fields (128/256-bit integer columns) need the import.
	needsJSON := false
	for _, name := range names {
		for _, col := range schemas[name].Columns {
			if strings.Contains(chTypeToGo(col.Type), "json.") {
				needsJSON = true
			}
		}
	}
	fmt.Fprintf(&sb, "// Code generated by wavehouse-codegen. DO NOT EDIT.\n\npackage %s\n\n", pkg)
	if needsJSON {
		sb.WriteString("import \"encoding/json\"\n\n")
	}

	// pascalCase is not injective and format.Source only parses, so a
	// collision would otherwise be written out as a non-compiling file.
	seenTypes := make(map[string]string, len(names))
	for _, name := range names {
		schema := schemas[name]
		typeName := pascalCase(name) + "Row"
		if prev, dup := seenTypes[typeName]; dup {
			return "", fmt.Errorf("tables %q and %q both map to type %q; rename one or generate separately", prev, name, typeName)
		}
		seenTypes[typeName] = name
		fmt.Fprintf(&sb, "// %s represents a row in the %q table.\ntype %s struct {\n", typeName, name, typeName)
		seenFields := make(map[string]string, len(schema.Columns))
		for _, col := range schema.Columns {
			goType := chTypeToGo(col.Type)
			fieldName := pascalCase(col.Name)
			if prev, dup := seenFields[fieldName]; dup {
				return "", fmt.Errorf("table %q: columns %q and %q both map to field %q", name, prev, col.Name, fieldName)
			}
			seenFields[fieldName] = col.Name
			jsonTag := col.Name
			if col.HasDefault {
				// Pointer + omitempty means nil omits the field so the server
				// default applies, while a pointer to the zero value still
				// sends an explicit 0/false/"".
				jsonTag += ",omitempty"
				if !strings.HasPrefix(goType, "*") {
					goType = "*" + goType
				}
			}
			fmt.Fprintf(&sb, "\t%s %s `json:%q`\n", fieldName, goType, jsonTag)
		}
		sb.WriteString("}\n\n")
	}

	return sb.String(), nil
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

	names := sortedKeys(schemas)
	fmt.Printf("Found %d table(s): %s\n", len(schemas), strings.Join(names, ", "))

	output, err := generate(schemas, args.pkg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// A format failure means the generated source is not valid Go; don't write
	// unusable output and claim success.
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
