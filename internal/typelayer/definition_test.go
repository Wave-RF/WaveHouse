package typelayer

import (
	"fmt"
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

// ingestTwo runs one record that keeps the CHECK, then one that breaks it.
func ingestTwo(t *testing.T, eng *Engine) Batch {
	t.Helper()
	tbl, err := eng.Table(tenant.Default, "ck")
	require.NoError(t, err)
	defer tbl.Release()
	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte("{\"id\":5,\"v\":\"ok\"}\n{\"id\":0,\"v\":\"bad\"}\n"))
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
	assert.Equal(t, 2, own.Refused.Record, "the record that breaks it")
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
		{"engine not modelled", "CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = Distributed('cluster', 'db', 'ck_local', rand())", "chtypes declined: engine-not-modeled"},
		// These two declines quote the definition in their message (measured).
		{"table setting not modelled", "CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'hunter2policy'", "chtypes declined: table-setting-not-modeled"},
		{"CHECK reads a dictionary", "CREATE TABLE db.ck (`id` UInt64, `v` String, CONSTRAINT c CHECK dictHas('hunter2dict', id)) ENGINE = MergeTree ORDER BY id", "chtypes declined: expression-reads-storage"},
		// ClickHouse's message for this one quotes the statement from the
		// error on, the endpoint, key and secret with it (measured).
		{"refused by ClickHouse", "CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = S3((, 'https://bucket.example/x.csv', 'AKIAEXAMPLE', 'hunter2', 'CSV')", "compile refused with ClickHouse code 62 (SYNTAX_ERROR)"},
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

// walkOnce runs one ingestNext over body in windows of a few records, so a
// test can read what the walk did to the cursor.
func walkOnce(t *testing.T, eng *Engine, table, body string) (Batch, *cursor) {
	t.Helper()
	tbl, err := eng.Table(tenant.Default, table)
	require.NoError(t, err)
	defer tbl.Release()
	c, err := newCursor(FormatJSONEachRow, IngestOptions{Window: Limits{Bytes: 1 << 20, Records: 3}}, []byte(body))
	require.NoError(t, err)
	b, err := tbl.ingestNext(c, nil)
	require.NoError(t, err)
	return b, c
}

// TestIngest_BlockRefusalDoesNotGrow: an insert step over the block (here a
// CollapsingMergeTree sign) refuses a window with every record read and no
// record to blame. That refuses the body at once: growing the window, which
// holds the same rows and more, parsed the whole body in one call, gigabytes
// for a large one.
func TestIngest_BlockRefusalDoesNotGrow(t *testing.T) {
	t.Parallel()
	eng := testEngine(t, &discovery.TableSchema{
		Name:    "cl",
		DDL:     "CREATE TABLE db.cl (`id` UInt64, `sign` Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY id",
		Columns: []discovery.Column{{Name: "id", Type: "UInt64", Position: 1}, {Name: "sign", Type: "Int8", Position: 2}},
	})
	var body strings.Builder
	for i := range 200 {
		sign := 1
		if i == 1 {
			sign = 5
		}
		fmt.Fprintf(&body, "{\"id\":%d,\"sign\":%d}\n", i, sign)
	}
	b, c := walkOnce(t, eng, "cl", body.String())
	require.NotNil(t, b.Refused)
	assert.Equal(t, 117, b.Refused.Code)
	assert.True(t, c.done)
	assert.Equal(t, 1, c.grow, "the window never grew")
}

// TestIngest_PartitionCapLifted: a body spanning more partitions than one
// INSERT block may (100 by default) is accepted, as the worker stores it,
// splitting a block the cap refuses.
func TestIngest_PartitionCapLifted(t *testing.T) {
	t.Parallel()
	eng := testEngine(t, &discovery.TableSchema{
		Name:    "pt",
		DDL:     "CREATE TABLE db.pt (`id` UInt64) ENGINE = MergeTree PARTITION BY id ORDER BY id",
		Columns: []discovery.Column{{Name: "id", Type: "UInt64", Position: 1}},
	})
	tbl, err := eng.Table(tenant.Default, "pt")
	require.NoError(t, err)
	defer tbl.Release()
	var body strings.Builder
	for i := range 150 {
		fmt.Fprintf(&body, "{\"id\":%d}\n", i)
	}
	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte(body.String()))
	require.NoError(t, err)
	assert.Nil(t, b.Refused)
	assert.Len(t, b.Rows, 150)
}

// TestIngest_CheckRowCountsFromTheBody: a CHECK that refuses a record in a
// later window names its row counted from the body's first record, as the
// refusal's record number does.
func TestIngest_CheckRowCountsFromTheBody(t *testing.T) {
	t.Parallel()
	eng := testEngine(t, checkedTable("CREATE TABLE db.ck (`id` UInt64, `v` String, CONSTRAINT c CHECK id > 0) ENGINE = MergeTree ORDER BY id"))
	tbl, err := eng.Table(tenant.Default, "ck")
	require.NoError(t, err)
	defer tbl.Release()
	var body strings.Builder
	for i := 1; i <= 10; i++ {
		id := i
		if i == 7 {
			id = 0
		}
		fmt.Fprintf(&body, "{\"id\":%d,\"v\":\"x\"}\n", id)
	}
	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{Window: Limits{Bytes: 1 << 20, Records: 3}}, []byte(body.String()))
	require.NoError(t, err)
	require.NotNil(t, b.Refused)
	assert.Equal(t, 7, b.Refused.Record)
	assert.Contains(t, b.Refused.Message, "violated at row 7.")
}

// TestIngest_BlockStepOverACutRecord: a window cut at its byte limit inside a
// CSV record still parses its shortened last record, which a step over the
// block (a partition key over a parsed date, a Collapsing sign) can then
// refuse. The walk reads the window again without that record, so the body is
// answered as one call answers it.
func TestIngest_BlockStepOverACutRecord(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, ddl, body string
		cols            []discovery.Column
		window          int
	}{
		{
			"partition key over a date", "CREATE TABLE db.bs (`id` UInt64, `s` String) ENGINE = MergeTree PARTITION BY toYYYYMM(toDate(s)) ORDER BY id",
			"1,2024-01-01\n2,2024-01-02\n3,2024-01-03\n",
			[]discovery.Column{{Name: "id", Type: "UInt64", Position: 1}, {Name: "s", Type: "String", Position: 2}},
			22,
		},
		{
			"collapsing sign", "CREATE TABLE db.bs (`id` UInt64, `sign` Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY id",
			"1,1\n2,1\n3,1\n",
			[]discovery.Column{{Name: "id", Type: "UInt64", Position: 1}, {Name: "sign", Type: "Int8", Position: 2}},
			6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eng := testEngine(t, &discovery.TableSchema{Name: "bs", DDL: tt.ddl, Columns: tt.cols})
			tbl, err := eng.Table(tenant.Default, "bs")
			require.NoError(t, err)
			defer tbl.Release()
			one, err := tbl.IngestWith(FormatCSV, IngestOptions{StrictPositional: true}, []byte(tt.body))
			require.NoError(t, err)
			require.Nil(t, one.Refused, "one call accepts the body")
			windowed, err := tbl.IngestWith(FormatCSV, IngestOptions{StrictPositional: true, Window: Limits{Bytes: tt.window, Records: 100}}, []byte(tt.body))
			require.NoError(t, err)
			assert.Nil(t, windowed.Refused, "the windows answer as one call")
			assert.Len(t, windowed.Rows, len(one.Rows))
		})
	}
}

// hmacTable is a table whose s column feeds an HMAC under a key the server
// masks, as system.columns and create_table_query report it (measured on
// 26.8.34.13: the key alone becomes '[HIDDEN]', three arguments kept).
func hmacTable(ddl string, cols ...discovery.Column) *discovery.TableSchema {
	return &discovery.TableSchema{Name: "hm", DDL: ddl, Columns: append([]discovery.Column{
		{Name: "s", Type: "String", Position: 1},
	}, cols...)}
}

// TestCompileSchema_MaskedColumnIsUnavailable: an expression whose key the
// server masks would be computed with the literal '[HIDDEN]', a wrong hash,
// so the table is unavailable instead, with a cause that names the column
// and not its expression. Every kind counts: a DEFAULT reading a MATERIALIZED
// or an EPHEMERAL column exports what chtypes computed for it.
func TestCompileSchema_MaskedColumnIsUnavailable(t *testing.T) {
	t.Parallel()
	const hmac = "hex(HMAC('SHA256', s, '[HIDDEN]'))"
	tests := []struct {
		name   string
		cols   []discovery.Column
		masked string
	}{
		{"DEFAULT", []discovery.Column{
			{Name: "d", Type: "String", Position: 2, DefaultKind: "DEFAULT", DefaultExpression: hmac},
		}, "d"},
		{"MATERIALIZED read by a DEFAULT", []discovery.Column{
			{Name: "m", Type: "String", Position: 2, DefaultKind: "MATERIALIZED", DefaultExpression: hmac},
			{Name: "d", Type: "String", Position: 3, DefaultKind: "DEFAULT", DefaultExpression: "concat(m, '!')"},
		}, "m"},
		{"EPHEMERAL read by a DEFAULT", []discovery.Column{
			{Name: "e", Type: "String", Position: 2, DefaultKind: "EPHEMERAL", DefaultExpression: hmac},
			{Name: "d", Type: "String", Position: 3, DefaultKind: "DEFAULT", DefaultExpression: "e"},
		}, "e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eng := testEngine(t, eventsTable(), hmacTable("", tt.cols...))
			tbl, err := eng.Table(tenant.Default, "hm")
			if err == nil {
				tbl.Release()
			}
			var u *Unavailable
			require.ErrorAs(t, err, &u)
			assert.Equal(t, "hm", u.Table)
			assert.Contains(t, u.Cause, "column \""+tt.masked+"\"")
			assert.NotContains(t, u.Cause, "HMAC")
			answers(t, eng, tenant.Default)
		})
	}
}

// TestCompileSchema_MaskedDefinitionFallsBack: a definition holding a masked
// value (a CHECK over a key the server hides) is not compiled, so no CHECK is
// judged with the wrong key; the table is served from its columns. Not
// t.Parallel: it captures the process-wide default logger.
func TestCompileSchema_MaskedDefinitionFallsBack(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelWarn)
	eng := testEngine(t, hmacTable("CREATE TABLE db.hm (`s` String, `sig` String, CONSTRAINT c CHECK sig = hex(HMAC('SHA256', s, '[HIDDEN]'))) ENGINE = MergeTree ORDER BY s",
		discovery.Column{Name: "sig", Type: "String", Position: 2}))
	tbl, err := eng.Table(tenant.Default, "hm")
	require.NoError(t, err)
	defer tbl.Release()
	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte("{\"s\":\"x\",\"sig\":\"A12979808451EB3DFECBD2ECD098F1B174B342EAB7AFF45C613DBECA67C836ED\"}\n"))
	require.NoError(t, err)
	assert.Nil(t, b.Refused, "the CHECK is the server's to judge, with its key")
	assert.Len(t, b.Rows, 1)
	recs := withMsg(logRecords(t, logs), "chtypes could not compile the table's own definition; compiling its columns on a neutral engine, without its engine, keys, partition, indices, CHECKs or TTL")
	require.Len(t, recs, 1)
	assert.Equal(t, "the definition holds a value the server masks as '[HIDDEN]'", recs[0]["cause"])
}
