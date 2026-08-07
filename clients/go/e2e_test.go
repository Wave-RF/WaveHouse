//go:build e2e

package wavehouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// e2eClient builds a Client pointing at the live WaveHouse instance.
// It reads WAVEHOUSE_URL (default http://localhost:8080) and the optional
// WAVEHOUSE_AUTH bearer token. The test is skipped when the server is
// unreachable — so `go test -tags e2e` degrades gracefully on a dev
// machine that isn't running the stack.
func e2eClient(t *testing.T) *Client {
	t.Helper()

	base := os.Getenv("WAVEHOUSE_URL")
	if base == "" {
		base = "http://localhost:8080"
	}

	cfg := Config{
		BaseURL: base,
		Options: &ClientOptions{MaxRetries: 1},
	}
	if tok := os.Getenv("WAVEHOUSE_AUTH"); tok != "" {
		cfg.Auth = StaticToken(tok)
	}

	// Probe the server before committing to the test. Bounded so a host that
	// accepts the connection but never responds still yields the graceful skip.
	probeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	probe, err := http.NewRequestWithContext(probeCtx, "GET", base+"/v1/health", nil)
	if err != nil {
		t.Skipf("e2e: bad WAVEHOUSE_URL %q: %v", base, err)
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(probe)
	if err != nil {
		t.Skipf("e2e: server unreachable at %s: %v", base, err)
	}
	resp.Body.Close()

	return NewClient(cfg)
}

// marker returns a unique string for the running test, useful for
// inserting distinguishable rows that won't collide across parallel runs.
func marker(t *testing.T) string {
	t.Helper()
	// Replace slashes in subtest names so it's a clean string value.
	safe := strings.ReplaceAll(t.Name(), "/", "_")
	return fmt.Sprintf("%s_%d", safe, time.Now().UnixNano())
}

// firstTable discovers a usable table from the schema list and returns its
// schema alongside the name. Many E2E tests need a real table to insert/query
// — this avoids hardcoding a name, and returning the schema avoids a second
// Schema.List whose result set might no longer contain the chosen table.
// Sorted so every run picks the same table (map iteration order is random).
func firstTable(t *testing.T, c *Client) (string, TableSchema) {
	t.Helper()
	schemas, err := c.Schema.List(context.Background())
	if err != nil {
		t.Skipf("e2e: cannot list schemas (auth?): %v", err)
	}
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	if len(names) == 0 {
		t.Skip("e2e: no tables found — server has an empty schema")
	}
	slices.Sort(names)
	return names[0], schemas[names[0]]
}

// waitForRows polls the marker query until at least want rows are visible or
// the deadline expires. Ingestion is asynchronous — a fixed sleep fails on a
// loaded runner without any real defect.
func waitForRows(t *testing.T, c *Client, table, markerCol, mk string, want int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		page, err := c.From(table).Select(markerCol).
			Where(markerCol, OpEq, mk).
			Limit(max(want, 1)).
			FetchUntyped(context.Background())
		if err != nil {
			t.Fatalf("query for marker %q: %v", mk, err)
		}
		if len(page.Data) >= want || time.Now().After(deadline) {
			return page.Data
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestE2E_HealthCheck(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()

	if err := c.Sys.Health(ctx); err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
}

func TestE2E_SchemaList(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()

	schemas, err := c.Schema.List(ctx)
	if err != nil {
		t.Fatalf("Schema.List failed: %v", err)
	}
	if len(schemas) == 0 {
		t.Fatal("Schema.List returned zero tables — expected at least one")
	}
	// Quick sanity: every table should have columns.
	for name, ts := range schemas {
		if len(ts.Columns) == 0 {
			t.Errorf("table %q has no columns", name)
		}
	}
}

func TestE2E_InsertAndQuery(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()
	table, ts := firstTable(t, c)
	mk := marker(t)

	row := buildMarkerRow(t, ts, mk)
	markerCol := markerColumn(t, ts)

	res, err := c.From(table).Insert(ctx, row)
	if err != nil {
		t.Fatalf("Insert into %s failed: %v", table, err)
	}
	if !res.OK {
		t.Fatalf("Insert into %s: OK=false", table)
	}

	rows := waitForRows(t, c, table, markerCol, mk, 1)
	if len(rows) == 0 {
		t.Fatal("Query returned zero rows — expected the inserted marker row")
	}
	got, _ := rows[0][markerCol].(string)
	if got != mk {
		t.Errorf("marker mismatch: want %q, got %q", mk, got)
	}
}

func TestE2E_BatchInsert(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()
	table, ts := firstTable(t, c)

	mk := marker(t)
	markerCol := markerColumn(t, ts)

	// Build 3 rows, each with the same marker so we can count them.
	rows := make([]map[string]any, 3)
	for i := range rows {
		rows[i] = buildMarkerRow(t, ts, mk)
	}

	res, err := c.From(table).Insert(ctx, rows)
	if err != nil {
		t.Fatalf("Batch insert failed: %v", err)
	}
	if !res.OK {
		t.Fatalf("Batch insert: OK=false")
	}

	got := waitForRows(t, c, table, markerCol, mk, 3)
	if len(got) < 3 {
		t.Fatalf("expected >= 3 rows for marker %q, got %d", mk, len(got))
	}
}

func TestE2E_QueryBuilder(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()
	table, ts := firstTable(t, c)

	// Pick two columns for a minimal projection.
	var cols []string
	for _, col := range ts.Columns {
		cols = append(cols, col.Name)
		if len(cols) >= 2 {
			break
		}
	}
	if len(cols) == 0 {
		t.Skipf("e2e: table %q has no columns", table)
	}

	page, err := c.From(table).
		Select(cols...).
		OrderBy(cols[0], "asc").
		Limit(5).
		FetchUntyped(ctx)
	if err != nil {
		t.Fatalf("QueryBuilder chain failed: %v", err)
	}
	// We can't assert exact data, but the chain should execute without error
	// and return at most 5 rows.
	if len(page.Data) > 5 {
		t.Errorf("Limit(5) returned %d rows", len(page.Data))
	}
}

func TestE2E_TypedFetch(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()
	table, _ := firstTable(t, c)

	q := c.From(table).SelectAll().Limit(3)
	page, err := FetchTyped[map[string]any](ctx, q)
	if err != nil {
		t.Fatalf("FetchTyped failed: %v", err)
	}
	// If the table has data we should get rows; if it's empty that's still
	// a valid result. The important thing is no error and correct type.
	for i, row := range page.Data {
		if row == nil {
			t.Errorf("row %d is nil", i)
		}
	}
}

func TestE2E_SQLQuery(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()

	rows, err := SQL[map[string]any](ctx, c, "SELECT 1 AS n")
	if err != nil {
		skipIfUnauthorized(t, err, "SQL query")
		t.Fatalf("SQL query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// ClickHouse returns numbers as strings or floats depending on format;
	// accept either.
	n := rows[0]["n"]
	switch v := n.(type) {
	case float64:
		if v != 1 {
			t.Errorf("expected n=1, got %v", v)
		}
	case string:
		if v != "1" {
			t.Errorf("expected n=1, got %q", v)
		}
	default:
		t.Errorf("unexpected type for n: %T = %v", n, n)
	}
}

func TestE2E_PolicyGetSet(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()

	pol, err := c.Policy.Get(ctx)
	if err != nil {
		skipIfUnauthorized(t, err, "Policy.Get")
		t.Fatalf("Policy.Get failed: %v", err)
	}

	// Round-trip: set the same policy back.
	if err := c.Policy.Set(ctx, pol); err != nil {
		t.Fatalf("Policy.Set (round-trip) failed: %v", err)
	}

	// Read again and verify tables still match.
	pol2, err := c.Policy.Get(ctx)
	if err != nil {
		t.Fatalf("Policy.Get (after set) failed: %v", err)
	}
	if len(pol2.Tables) != len(pol.Tables) {
		t.Errorf("policy table count changed: %d -> %d", len(pol.Tables), len(pol2.Tables))
	}
}

func TestE2E_PipesCRUD(t *testing.T) {
	c := e2eClient(t)
	ctx := context.Background()

	pipeName := fmt.Sprintf("e2e_test_%d", time.Now().UnixNano())

	// Create
	def := PipeDef{
		SQL:         "SELECT 1 AS ok",
		Description: "E2E test pipe — safe to delete",
	}
	if err := c.Pipes.Set(ctx, pipeName, def); err != nil {
		skipIfUnauthorized(t, err, "Pipes.Set")
		t.Fatalf("Pipes.Set (create) failed: %v", err)
	}

	// Cleanup: always attempt delete so we don't litter.
	t.Cleanup(func() {
		_ = c.Pipes.Delete(context.Background(), pipeName)
	})

	// Get
	pipe, err := c.Pipes.Get(ctx, pipeName)
	if err != nil {
		t.Fatalf("Pipes.Get failed: %v", err)
	}
	if pipe.SQL != def.SQL {
		t.Errorf("pipe SQL mismatch: want %q, got %q", def.SQL, pipe.SQL)
	}

	// List — verify it appears
	pipes, err := c.Pipes.List(ctx)
	if err != nil {
		t.Fatalf("Pipes.List failed: %v", err)
	}
	found := false
	for _, p := range pipes {
		if p.Name == pipeName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Pipes.List: created pipe %q not found in list of %d pipes", pipeName, len(pipes))
	}

	// Delete
	if err := c.Pipes.Delete(ctx, pipeName); err != nil {
		t.Fatalf("Pipes.Delete failed: %v", err)
	}

	// Verify gone — Get should fail.
	_, err = c.Pipes.Get(ctx, pipeName)
	if err == nil {
		t.Error("Pipes.Get after delete: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// markerColumn finds the first String/LowCardinality(String) column in the
// schema that we can use to inject a test marker value.
func markerColumn(t *testing.T, ts TableSchema) string {
	t.Helper()
	for _, col := range ts.Columns {
		ct := strings.ToLower(col.Type)
		if ct == "string" || strings.Contains(ct, "string") {
			return col.Name
		}
	}
	t.Skipf("e2e: table %q has no string column for marker injection", ts.Name)
	return ""
}

// buildMarkerRow constructs a minimal valid row for the table, injecting the
// marker into the first string column and using sensible defaults for other
// required columns.
func buildMarkerRow(t *testing.T, ts TableSchema, mk string) map[string]any {
	t.Helper()
	row := make(map[string]any)
	markerSet := false
	for _, col := range ts.Columns {
		if col.HasDefault {
			continue // let the server fill defaults
		}
		ct := strings.ToLower(col.Type)
		switch {
		case !markerSet && strings.Contains(ct, "string"):
			row[col.Name] = mk
			markerSet = true
		case strings.Contains(ct, "string"):
			row[col.Name] = "e2e"
		case strings.Contains(ct, "int"):
			row[col.Name] = 0
		case strings.Contains(ct, "float") || strings.Contains(ct, "decimal"):
			row[col.Name] = 0.0
		case strings.Contains(ct, "date") || strings.Contains(ct, "datetime"):
			row[col.Name] = time.Now().UTC().Format(time.RFC3339)
		case strings.Contains(ct, "bool"):
			row[col.Name] = false
		default:
			// No safe synthetic value for this type (Array, Map, Tuple, UUID,
			// ...) — an empty string would make the insert fail with a type
			// error that looks like an SDK defect.
			t.Skipf("e2e: table %q requires column %q of unsupported type %q", ts.Name, col.Name, col.Type)
		}
	}
	if !markerSet {
		t.Skipf("e2e: table %q has no non-default string column for marker", ts.Name)
	}
	return row
}

// skipIfUnauthorized skips the test when err indicates a 401 or 403,
// meaning the operation requires admin auth the current token lacks.
func skipIfUnauthorized(t *testing.T, err error, op string) {
	t.Helper()
	if isHTTPStatus(err, 401) || isHTTPStatus(err, 403) {
		t.Skipf("%s requires admin auth, skipping", op)
	}
}

// isHTTPStatus checks whether err wraps a wavehouse.Error with the given status.
func isHTTPStatus(err error, status int) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Status == status
	}
	return false
}
