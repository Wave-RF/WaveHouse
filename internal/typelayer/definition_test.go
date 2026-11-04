package typelayer

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// checkedTable is a table with a CHECK on id, declared by ddl.
func checkedTable(ddl string) *discovery.TableSchema {
	return &discovery.TableSchema{Name: "ck", DDL: ddl, Columns: []discovery.Column{
		{Name: "id", Type: "UInt64", Position: 1},
		{Name: "v", Type: "String", Position: 2},
	}}
}

// ingestTwo runs one record that breaks the CHECK and one that keeps it.
func ingestTwo(t *testing.T, eng *Engine) Batch {
	t.Helper()
	tbl, err := eng.Table(tenant.Default, "ck")
	require.NoError(t, err)
	defer tbl.Release()
	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte("{\"id\":0,\"v\":\"bad\"}\n{\"id\":5,\"v\":\"ok\"}\n"))
	require.NoError(t, err)
	return b
}

// TestCompileSchema_OwnDefinition: a table compiles from its own definition,
// so its CHECK refuses a body the way the server's INSERT does (code 469,
// the whole body); the same columns on the neutral engine carry no CHECK.
func TestCompileSchema_OwnDefinition(t *testing.T) {
	t.Parallel()
	own := ingestTwo(t, testEngine(t, checkedTable(
		"CREATE TABLE db.ck (`id` UInt64, `v` String, CONSTRAINT c CHECK id > 0) ENGINE = MergeTree ORDER BY id")))
	require.NotNil(t, own.Refused, "the CHECK refuses the body")
	assert.Equal(t, 469, own.Refused.Code)
	assert.Equal(t, 1, own.Refused.Record)
	assert.Empty(t, own.Rows)

	columns := ingestTwo(t, testEngine(t, checkedTable("")))
	assert.Nil(t, columns.Refused, "no definition: the neutral engine has no CHECK")
	assert.Len(t, columns.Rows, 2)
}

// TestCompileSchema_FallsBackToColumns: a definition chtypes does not model
// (a Distributed engine) is served from its columns on the neutral engine,
// with a WARN naming the decline; a definition ClickHouse refuses is named by
// its code alone, since its message can quote the statement, and an external
// engine's statement carries its wiring. Not t.Parallel: it captures the
// process-wide default logger.
func TestCompileSchema_FallsBackToColumns(t *testing.T) {
	const msg = "chtypes could not compile the table's own definition; compiling its columns on a neutral engine, without its engine, keys, partition, indices, CHECKs or TTL"
	tests := []struct {
		name, ddl, cause string
	}{
		{"engine not modelled", "CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = Distributed('cluster', 'db', 'ck_local', rand())", "chtypes declined: engine not modelled: Distributed"},
		// ClickHouse's message for this one quotes the statement from the
		// error on, the endpoint, key and secret with it (measured).
		{"refused by ClickHouse", "CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = S3((, 'https://bucket.example/x.csv', 'AKIAEXAMPLE', 'hunter2', 'CSV')", "compile refused with ClickHouse code 62"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := logtest.Capture(t, slog.LevelWarn)
			b := ingestTwo(t, testEngine(t, checkedTable(tt.ddl)))
			assert.Nil(t, b.Refused)
			assert.Len(t, b.Rows, 2, "the table is served from its columns")

			recs := withMsg(logRecords(t, logs), msg)
			require.Len(t, recs, 1)
			assert.Equal(t, "ck", recs[0]["table"])
			assert.Equal(t, tt.cause, recs[0]["cause"])
			assert.NotContains(t, logs.String(), "hunter2")
		})
	}
}

// TestBind_DefinitionChangeRecompiles: a change to the definition alone, here
// the sorting key, recompiles the table as a column change does.
func TestBind_DefinitionChangeRecompiles(t *testing.T) {
	t.Parallel()
	before := checkedTable("CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = MergeTree ORDER BY id")
	eng := testEngine(t, before)
	gen := func() uint64 {
		tbl, err := eng.Table(tenant.Default, "ck")
		require.NoError(t, err)
		defer tbl.Release()
		return tbl.Generation
	}
	first := gen()
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{before})
	assert.Equal(t, first, gen(), "an unchanged definition keeps the handle")
	after := checkedTable(strings.Replace(before.DDL, "ORDER BY id", "ORDER BY (v, id)", 1))
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{after})
	assert.Greater(t, gen(), first, "a new sorting key recompiles")
}
