package api

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/testutil/mutationtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubConn records how many times Exec or Query was called and returns
// canned results. The embedded nil driver.Conn keeps every method we don't
// override undefined-method-call-panic'd, which is what we want — the test
// fails loudly if executeCHQuery starts touching new surface area.
type stubConn struct {
	driver.Conn
	execCount  int
	queryCount int
	execErr    error
	// queryRows, when non-nil, is returned by Query in place of the default
	// empty rows. Tests that exercise row-scan / transformRow paths use this.
	queryRows driver.Rows
}

func (c *stubConn) Exec(_ context.Context, _ string, _ ...any) error {
	c.execCount++
	return c.execErr
}

func (c *stubConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.queryCount++
	if c.queryRows != nil {
		return c.queryRows, nil
	}
	return &chainEmptyRows{}, nil
}

// TestIsMutation runs the shared cases; the integration suite checks the
// same cases against ClickHouse's parser.
func TestIsMutation(t *testing.T) {
	t.Parallel()
	for _, tc := range mutationtest.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.Mutation, IsMutation(tc.SQL))
		})
	}
}

// TestIsMutation_ClickHouseWhitespace pins every character ClickHouse 26.6's
// lexer accepts as whitespace, each checked against a live server: ahead of a
// write it must not hide the verb, and ahead of a read it must not make one.
func TestIsMutation_ClickHouseWhitespace(t *testing.T) {
	t.Parallel()
	spaces := []rune{' ', '\t', '\n', '\v', '\f', '\r', 0x85, 0xA0, 0x180E, 0x2028, 0x2029, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF}
	for r := rune(0x2000); r <= 0x200D; r++ {
		spaces = append(spaces, r)
	}
	for _, r := range spaces {
		ws := string(r)
		assert.True(t, IsMutation(ws+"INSERT INTO t VALUES (1)"), "U+%04X before INSERT", r)
		assert.False(t, IsMutation(ws+"SELECT 1"), "U+%04X before SELECT", r)
		assert.True(t, IsMutation("WITH x AS (SELECT 1)"+ws+"INSERT INTO t SELECT * FROM x"), "U+%04X before a WITH's INSERT", r)
		assert.True(t, IsMutation("WITH 1 AS x INSERT"+ws+"INTO t SELECT x"), "U+%04X between a WITH's INSERT and INTO", r)
	}
	// Not whitespace to ClickHouse (it rejects the statement), so not skipped.
	assert.False(t, IsMutation("\u1680INSERT INTO t VALUES (1)"))
}

func TestExecuteCHQuery_MutationRoutesToExec(t *testing.T) {
	t.Parallel()
	// Mutations route through driver.Exec because clickhouse-go's
	// driver.Query() errors on statements that return no result set.
	// executeCHQuery marshals the no-rows case to `[]` so the response shape
	// stays "always an array" regardless of whether the SQL was a read or
	// a mutation. Used by structured_query and pipes handlers; the raw-SQL
	// handler bypasses this entirely (HTTP proxy).
	for _, sql := range []string{
		"TRUNCATE TABLE clicks",
		"DROP TABLE clicks",
		"DELETE FROM clicks WHERE id = 1",
		"ALTER TABLE clicks ADD COLUMN c String",
		"INSERT INTO clicks VALUES (1)",
		"  -- audit log\n  UPDATE clicks SET v = 1 WHERE id = 2",
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			conn := &stubConn{}
			rows, err := executeCHQuery(context.Background(), conn, sql, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, conn.execCount, "Exec must be used for mutations")
			assert.Zero(t, conn.queryCount, "Query must not be used for mutations")
			assert.Equal(t, []map[string]any{}, rows, "mutation result must marshal to [] not null")
		})
	}
}

func TestExecuteCHQuery_SelectRoutesToQuery(t *testing.T) {
	t.Parallel()
	conn := &stubConn{}
	rows, err := executeCHQuery(context.Background(), conn, "SELECT 1", nil)
	require.NoError(t, err)
	assert.Zero(t, conn.execCount, "Exec must not be used for SELECT")
	assert.Equal(t, 1, conn.queryCount, "Query must be used for SELECT")
	assert.Equal(t, []map[string]any{}, rows, "zero-row SELECT must marshal to [] not null")
}

// TestExecuteCHQuery_TransformsClickHouseTypes pins transformRow's contract
// at the unit level: UUIDs become canonical strings, time.Time — including the
// *time.Time a Nullable(DateTime…) column scans as — becomes RFC3339Nano UTC
// (NULL stays JSON null), and other scalars pass through unchanged. The
// integration suite exercises the same path against a real ClickHouse, but
// this unit test catches regressions in the type-conversion branches without
// standing up testcontainers.
func TestExecuteCHQuery_TransformsClickHouseTypes(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	ts := time.Date(2026, 5, 19, 12, 30, 45, 123456789, time.FixedZone("EST", -5*3600))
	conn := &stubConn{queryRows: &chainOneRow{
		columns: []chainColumnType{
			{name: "id", scanType: reflect.TypeFor[uuid.UUID]()},
			{name: "received_at", scanType: reflect.TypeFor[time.Time]()},
			{name: "updated_at", scanType: reflect.TypeFor[*time.Time]()},
			{name: "deleted_at", scanType: reflect.TypeFor[*time.Time]()},
			{name: "n", scanType: reflect.TypeFor[int64]()},
		},
		values: []any{id, ts, &ts, (*time.Time)(nil), int64(42)},
	}}

	rows, err := executeCHQuery(context.Background(), conn, "SELECT id, received_at, updated_at, deleted_at, n FROM t", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, id.String(), rows[0]["id"], "UUID must be stringified")
	assert.Equal(t, ts.UTC().Format(time.RFC3339Nano), rows[0]["received_at"], "time must be RFC3339Nano in UTC")
	assert.Equal(t, ts.UTC().Format(time.RFC3339Nano), rows[0]["updated_at"], "nullable time must be RFC3339Nano in UTC")
	assert.Nil(t, rows[0]["deleted_at"], "NULL nullable time must stay nil")
	assert.Equal(t, int64(42), rows[0]["n"], "scalar must pass through unchanged")
}

// chainOneRow implements driver.Rows for a single canned row. Scan
// reflect-writes values[i] into the i-th destination pointer that
// executeCHQuery allocates from ColumnTypes()[i].ScanType().
type chainOneRow struct {
	driver.Rows
	columns []chainColumnType
	values  []any
	yielded bool
}

func (r *chainOneRow) Next() bool {
	if r.yielded {
		return false
	}
	r.yielded = true
	return true
}

func (r *chainOneRow) Scan(dest ...any) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.values[i]))
	}
	return nil
}

func (*chainOneRow) Close() error { return nil }
func (*chainOneRow) Err() error   { return nil }

func (r *chainOneRow) ColumnTypes() []driver.ColumnType {
	out := make([]driver.ColumnType, len(r.columns))
	for i := range r.columns {
		out[i] = &r.columns[i]
	}
	return out
}

type chainColumnType struct {
	driver.ColumnType
	name     string
	scanType reflect.Type
}

func (c *chainColumnType) Name() string           { return c.name }
func (c *chainColumnType) ScanType() reflect.Type { return c.scanType }
