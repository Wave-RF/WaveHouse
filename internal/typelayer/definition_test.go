package typelayer

import (
	"fmt"
	"log/slog"
	"strconv"
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
	assert.Equal(t, "Constraint `c` is violated at row 2.", own.Refused.Message, "no expression or values quoted")
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
		// A key the server did not mask (a user allowed to see secrets) in an
		// expression chtypes evaluates at parse: a refusal there quotes it.
		{"a key in the partition expression", "CREATE TABLE db.ck (`id` UInt64, `v` String) ENGINE = MergeTree PARTITION BY decrypt('aes-256-ofb', v, 'hunter2hunter2hunter2hunter2abcd') ORDER BY id", "the definition calls a function that takes a key"},
		// A keyed hash's key, which the server never masks, in a CHECK: a
		// violation quotes the expression, key included (measured).
		{"a keyed hash in a CHECK", "CREATE TABLE db.ck (`id` UInt64, `v` String, CONSTRAINT c CHECK sipHash64Keyed((toUInt64(1111), toUInt64(2222)), v) % 2 = 0) ENGINE = MergeTree ORDER BY id", "the definition calls a function that takes a key"},
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

// TestWithoutDefinitionText: a CHECK violation keeps the sentence naming the
// constraint (and the row, when it has one) and drops its quote of the
// expression and values; a message raised while a definition expression was
// evaluated or analyzed is withheld, and so is a CHECK's in a shape not known
// and any message of a code the reader does not raise; a record's own parse
// error, client text that only looks like a CHECK's included, is untouched.
// Messages measured on build 20261104.224808.
func TestWithoutDefinitionText(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		code     int
		in, want string
	}{
		{469, "Constraint `c` for table db.t1 is violated at row 1. Expression: (v != 'constvalue'). Column values: v = 'constvalue'", "Constraint `c` is violated at row 1."},
		{469, "Constraint `c` for table db.t is violated. Expression: (v != 'constvalue').Constraint expression returns nullable column that contains null value", "Constraint `c` is violated."},
		{469, "Constraint `c` for table db.t refused 'constvalue'", definitionWithheld},
		{6, "Cannot parse date here: zzconstpar: Cannot parse Date from String: while executing 'FUNCTION toDate(concat(s, 'constpart') :: 2) -> toDate(concat(s, 'constpart')) Date : 1'", definitionWithheld},
		{6, "Cannot parse string 'notanumberconst' as UInt64: syntax error at begin of string: In scope _CAST(toUInt64('notanumberconst'), 'UInt64') AS d", definitionWithheld},
		{46, "Function with name `futureFunction` does not exist. In scope _CAST('constvalue', 'String') AS secret, _CAST(concat('constvalue', futureFunction(dummy.dummy.v)), 'String') AS dx", definitionWithheld},
		{47, "Unknown expression or function identifier `constvalue_col` in scope _CAST(toString(constvalue_col), 'String') AS dx", definitionWithheld},
		{6, "Cannot parse string 'constvalue' as UInt64: syntax error at begin of string", definitionWithheld},
		{190, "Arrays passed to arrayMap must have equal size. Argument 3 has size 1 which differs with the size of another argument, 2", definitionWithheld},
		{38, "Cannot parse date: value is too short: in scope _CAST(toDate('constvalue'), 'Date') AS d", definitionWithheld},
		{38, "Cannot parse date: value is too short: (while reading the value of key v)", "Cannot parse date: value is too short: (while reading the value of key v)"},
		{676, "Cannot parse IPv6 zz\"}", "Cannot parse IPv6 zz\"}"},
		{691, "Unknown element 'internal_tier_g' for enum, maybe you meant: ['internal_tier_gold']: (while reading the value of key v)", "Unknown element 'internal_tier_g' for enum: (while reading the value of key v)"},
		{691, "Unknown element 'a]' for enum, maybe you meant: ['a]b', 'a]c']", "Unknown element 'a]' for enum"},
		{27, "Cannot parse input: expected '\"' before: 'x\"}\\n': (while reading the value of key id)", "Cannot parse input: expected '\"' before: 'x\"}\\n': (while reading the value of key id)"},
		{27, "Cannot parse input: expected '\\t' before: 'abc. Expression: xyz': (at row 3)", "Cannot parse input: expected '\\t' before: 'abc. Expression: xyz': (at row 3)"},
		{27, "Cannot parse input: expected '\\t' before: 'Constraint x is violated. Expression: y': (at row 3)", "Cannot parse input: expected '\\t' before: 'Constraint x is violated. Expression: y': (at row 3)"},
		{117, "Unknown field found while parsing JSONEachRow format: nope", "Unknown field found while parsing JSONEachRow format: nope"},
		{117, "Duplicate field found while parsing format header: id: (while reading header)", "Duplicate field found while parsing format header: id: (while reading header)"},
		{117, "Expected end of line", "Expected end of line"},
		{117, "Cannot parse JSON value of type Variant(Enum8('constvalue' = 1), UInt8) here: \"zz\": (while reading the value of key v)", definitionWithheld},
		{27, "Cannot parse quoted value of type Variant(Enum8('constvalue' = 1), UInt8) here: zz", definitionWithheld},
		{117, "A shape not measured 'constvalue'", definitionWithheld},
		{117, "Incorrect data: Sign = 5 (must be 1 or -1).", "Incorrect data: Sign = 5 (must be 1 or -1)."},
	} {
		assert.Equal(t, tt.want, withoutDefinitionText(tt.code, tt.in), "%q", tt.in)
	}
}

// TestRefusal_CheckQuotesNoDefinition: a CHECK refusal reaches the client as
// the sentence naming the constraint, whether the CHECK came out false or NULL
// and whether the column it read is one the role may write, and withheld when
// a conversion inside it failed; no constant of the definition survives.
func TestRefusal_CheckQuotesNoDefinition(t *testing.T) {
	t.Parallel()
	nullable := &discovery.TableSchema{
		Name: "cq", DDL: "CREATE TABLE db.cq (`id` UInt64, `n` Nullable(String), CONSTRAINT c CHECK n != 'topsecretconst') ENGINE = MergeTree ORDER BY id",
		Columns: []discovery.Column{{Name: "id", Type: "UInt64", Position: 1}, {Name: "n", Type: "Nullable(String)", Position: 2}},
	}
	hidden := func(check string) *discovery.TableSchema {
		return &discovery.TableSchema{
			Name: "cq", DDL: "CREATE TABLE db.cq (`id` UInt64, `secret` String DEFAULT 'topsecretconst', CONSTRAINT c CHECK " + check + ") ENGINE = MergeTree ORDER BY id",
			Columns: []discovery.Column{
				{Name: "id", Type: "UInt64", Position: 1},
				{Name: "secret", Type: "String", Position: 2, DefaultKind: "DEFAULT", DefaultExpression: "'topsecretconst'"},
			},
		}
	}
	tests := []struct {
		name  string
		table *discovery.TableSchema
		shape RoleShape
		body  string
		want  string
	}{
		{"NULL", nullable, RoleShape{}, `{"id":1}`, "Constraint `c` is violated."},
		{"false", nullable, RoleShape{}, `{"id":1,"n":"topsecretconst"}`, "Constraint `c` is violated at row 1."},
		{"unwritable column, false", hidden("secret != 'topsecretconst'"), RoleShape{Columns: []string{"id"}}, `{"id":1}`, "Constraint `c` is violated at row 1."},
		{"unwritable column, conversion", hidden("toUInt64(secret) > 0"), RoleShape{Columns: []string{"id"}}, `{"id":1}`, definitionWithheld},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eng := testEngine(t, tt.table)
			tbl, err := eng.RoleTable(tenant.Default, "cq", tt.shape)
			require.NoError(t, err)
			defer tbl.Release()
			b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte(tt.body+"\n"))
			require.NoError(t, err)
			require.NotNil(t, b.Refused)
			assert.NotZero(t, b.Refused.Code)
			assert.Equal(t, 1, b.Refused.Record)
			assert.Equal(t, tt.want, b.Refused.Message)
			assert.NotContains(t, b.Refused.Message, "topsecretconst")
		})
	}
}

// TestRefusal_WithholdsADefinitionConstant: a conversion that fails inside a
// DEFAULT or a partition expression names the value it failed on, a constant
// of the definition among it, in the head of ClickHouse's message, and a
// DEFAULT the library cannot analyze lists every DEFAULT of the table; the
// refusal keeps its code and record and withholds the message.
func TestRefusal_WithholdsADefinitionConstant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, ddl, body string
		cols            []discovery.Column
	}{
		{
			"DEFAULT", "", `{"id":1}`,
			[]discovery.Column{
				{Name: "id", Type: "UInt64", Position: 1},
				{Name: "d", Type: "UInt64", Position: 2, DefaultKind: "DEFAULT", DefaultExpression: "toUInt64('notanumberconst')"},
			},
		},
		{
			"partition expression", "CREATE TABLE db.wd (`id` UInt64, `v` String) ENGINE = MergeTree PARTITION BY toUInt64(concat(v, 'notanumberconst')) ORDER BY id", `{"id":1,"v":"zz"}`,
			[]discovery.Column{{Name: "id", Type: "UInt64", Position: 1}, {Name: "v", Type: "String", Position: 2}},
		},
		{
			"unknown function", "CREATE TABLE db.wd (`id` UInt64, `v` String, `s` String DEFAULT 'notanumberconst', `d` String DEFAULT concat('notanumberconst', futureFunction(v))) ENGINE = MergeTree ORDER BY id", `{"id":1,"v":"zz"}`,
			[]discovery.Column{
				{Name: "id", Type: "UInt64", Position: 1},
				{Name: "v", Type: "String", Position: 2},
				{Name: "s", Type: "String", Position: 3, DefaultKind: "DEFAULT", DefaultExpression: "'notanumberconst'"},
				{Name: "d", Type: "String", Position: 4, DefaultKind: "DEFAULT", DefaultExpression: "concat('notanumberconst', futureFunction(v))"},
			},
		},
		{
			"unknown identifier", "CREATE TABLE db.wd (`id` UInt64, `v` String, `d` String DEFAULT toString(notanumberconst_col)) ENGINE = MergeTree ORDER BY id", `{"id":1,"v":"zz"}`,
			[]discovery.Column{
				{Name: "id", Type: "UInt64", Position: 1},
				{Name: "v", Type: "String", Position: 2},
				{Name: "d", Type: "String", Position: 3, DefaultKind: "DEFAULT", DefaultExpression: "toString(notanumberconst_col)"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			eng := testEngine(t, &discovery.TableSchema{Name: "wd", DDL: tt.ddl, Columns: tt.cols})
			tbl, err := eng.Table(tenant.Default, "wd")
			require.NoError(t, err)
			defer tbl.Release()
			b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte(tt.body+"\n"))
			require.NoError(t, err)
			require.NotNil(t, b.Refused)
			assert.NotZero(t, b.Refused.Code)
			assert.Equal(t, definitionWithheld, b.Refused.Message)
			assert.NotContains(t, b.Refused.Message, "notanumberconst")
		})
	}
}

// TestRefusal_KeepsTheReaderMessage: a record's own bad bytes, on a table with
// no definition expression, are refused with ClickHouse's message kept, one
// case per code readerCodes lists, so a code the reader raises that the list
// lacks shows up here rather than as a withheld message to a client.
func TestRefusal_KeepsTheReaderMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code   int
		typ    string
		format Format
		body   string
	}{
		{25, "String", FormatJSONEachRow, `{"id":1,"v":"\ud800"}`},
		{26, "String", FormatJSONEachRow, `[{"id":1,"v":"ab`},
		{27, "UInt64", FormatJSONEachRow, `{"id":1,"v":"abc"}`},
		{32, "String", FormatJSONEachRow, `{"id":`},
		{33, "String", FormatJSONEachRow, `{"id":1`},
		{38, "Date", FormatJSONEachRow, `{"id":1,"v":"zz"}`},
		{41, "DateTime", FormatJSONEachRow, `{"id":1,"v":"zz"}`},
		{69, "Decimal(5, 2)", FormatJSONEachRow, `{"id":1,"v":12345678.9}`},
		{72, "UInt64", FormatJSONEachRow, `{"id":1,"v":-1}`},
		{117, "String", FormatJSONEachRow, `{"id":1,"zz":1}`},
		{130, "Array(UInt8)", FormatJSONEachRow, `{"id":1,"v":"x"}`},
		{131, "FixedString(2)", FormatJSONEachRow, `{"id":1,"v":"abcd"}`},
		{376, "UUID", FormatJSONEachRow, `{"id":1,"v":"zz"}`},
		{407, "DateTime64(9)", FormatJSONEachRow, `{"id":1,"v":"9999-12-31T23:59:59Z"}`},
		{467, "Bool", FormatJSONEachRow, `{"id":1,"v":"maybe"}`},
		{563, "Map(String, UInt8)", FormatCSV, `1,"{'a':'x'}"`},
		{632, "DateTime", FormatCSV, `1,1704103200abc`},
		{675, "IPv4", FormatJSONEachRow, `{"id":1,"v":"zz"}`},
		{676, "IPv6", FormatJSONEachRow, `{"id":1,"v":"zz"}`},
		{691, "Enum8('alpha' = 1)", FormatJSONEachRow, `{"id":1,"v":"alph"}`},
	}
	covered := map[int]bool{}
	for _, tt := range tests {
		covered[tt.code] = true
		t.Run(strconv.Itoa(tt.code), func(t *testing.T) {
			t.Parallel()
			eng := testEngine(t, &discovery.TableSchema{Name: "rd", Columns: []discovery.Column{
				{Name: "id", Type: "UInt64", Position: 1}, {Name: "v", Type: tt.typ, Position: 2},
			}})
			tbl, err := eng.Table(tenant.Default, "rd")
			require.NoError(t, err)
			defer tbl.Release()
			b, err := tbl.IngestWith(tt.format, IngestOptions{}, []byte(tt.body+"\n"))
			require.NoError(t, err)
			require.NotNil(t, b.Refused)
			assert.Equal(t, tt.code, b.Refused.Code, "%s", b.Refused.Message)
			assert.NotEqual(t, definitionWithheld, b.Refused.Message)
			assert.NotEmpty(t, b.Refused.Message)
			assert.NotContains(t, b.Refused.Message, "alpha", "an Enum's labels are the definition's")
		})
	}
	for code := range readerCodes {
		assert.True(t, covered[code], "reader code %d has no case", code)
	}
}

// TestRefusal_KeepsNoDeclaredString: a record's bad value for a column whose
// type declares strings (Enum labels, Tuple element names, a Variant's
// members) is refused in every format with none of them in the message, so a
// writer who cannot read the schema learns none from a refusal.
func TestRefusal_KeepsNoDeclaredString(t *testing.T) {
	t.Parallel()
	types := []string{
		"Variant(Enum8('qxlab' = 1, 'qxlabtwo' = 2), UInt8)", "Enum8('qxlab' = 1, 'qxlabtwo' = 2)", "Array(Enum8('qxlab' = 1))",
		"Nullable(Enum16('qxlab' = 1))", "Array(Variant(Enum8('qxlab' = 1), UInt8))", "Tuple(qxnamea UInt8, qxnameb String)",
		"Map(String, Variant(Tuple(qxnamea UInt8, qxnameb String), UInt64))",
	}
	bodies := []struct {
		format Format
		body   string
	}{
		{FormatJSONEachRow, `{"id":1,"v":"zz"}`},
		{FormatJSONEachRow, `{"id":1,"v":"qxla"}`},
		{FormatJSONEachRow, `{"id":1,"v":[1,"zz"]}`},
		{FormatJSONEachRow, `{"id":1,"v":{"a":"zz"}}`},
		{FormatJSONEachRow, `{"id":1,"v":[["zz"]]}`},
		{FormatCSV, "1,zz"},
		{FormatCSV, "1,qxla"},
		{FormatTSV, "1\tzz"},
		{FormatTSV, "1\tqxla"},
	}
	refused := 0
	for _, typ := range types {
		eng := testEngine(t, &discovery.TableSchema{Name: "ds", Columns: []discovery.Column{
			{Name: "id", Type: "UInt64", Position: 1}, {Name: "v", Type: typ, Position: 2},
		}})
		tbl, err := eng.Table(tenant.Default, "ds")
		require.NoError(t, err, typ)
		for _, b := range bodies {
			r, err := tbl.IngestWith(b.format, IngestOptions{}, []byte(b.body+"\n"))
			require.NoError(t, err)
			if r.Refused == nil {
				continue
			}
			refused++
			assert.NotContains(t, r.Refused.Message, "qxlab", "%s %q", typ, b.body)
			assert.NotContains(t, r.Refused.Message, "qxname", "%s %q", typ, b.body)
		}
		tbl.Release()
	}
	assert.Greater(t, refused, 40, "the bodies are bad for these types")
}
