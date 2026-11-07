package typelayer

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// ordersTable is the per-role fixture: an ordinary column a role may be denied
// (secret), one a check clause injects into (tenant), and a numeric column
// whose reader refuses a text value (amount).
func ordersTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "orders",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "tenant", Type: "String", Position: 2},
			{Name: "secret", Type: "String", Position: 3},
			{Name: "amount", Type: "UInt64", Position: 4},
		},
	}
}

func roleTableFor(t *testing.T, eng *Engine, shape RoleShape) *Table {
	t.Helper()
	tbl, err := eng.RoleTable(tenant.Default, "orders", shape)
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	return tbl
}

// TestRoleTable_IdentityShapeIsTheBaseTable: a role that may write everything
// and injects nothing is answered by the base table itself.
func TestRoleTable_IdentityShapeIsTheBaseTable(t *testing.T) {
	eng := testEngine(t, ordersTable())

	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	base.Release()

	role, err := eng.RoleTable(tenant.Default, "orders", RoleShape{})
	require.NoError(t, err)
	defer role.Release()

	assert.Same(t, base, role)
}

// TestRoleTable_IsAViewOfTheBaseTable: a role's table compiles nothing. It
// runs on the base table's handle and generation, and holds the base table's
// read lock until Release, so a rebind waits for it; the next view is of the
// new generation.
func TestRoleTable_IsAViewOfTheBaseTable(t *testing.T) {
	eng := testEngine(t, ordersTable())
	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	baseHandle := base.c
	base.Release()

	view, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Columns: []string{"id", "amount"}})
	require.NoError(t, err)
	assert.NotSame(t, base, view)
	assert.Same(t, baseHandle, view.c)
	assert.Equal(t, base.Generation, view.Generation)
	assert.Equal(t, base.WireColumns, view.WireColumns)

	changed := ordersTable()
	changed.Columns[3].Type = "UInt32"
	bound := make(chan struct{})
	go func() {
		defer close(bound)
		eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})
	}()
	waitWriterPending(t, base)
	select {
	case <-bound:
		t.Error("the rebind did not wait for the view's Release")
	default:
	}
	view.Release()
	<-bound

	next := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "amount"}})
	assert.Equal(t, uint64(2), next.Generation)
	assert.NotSame(t, baseHandle, next.c)
}

// TestRoleTable_DeniedColumnIsUnnamableAndCarriesTheTableDefault: a column
// the role may not write is left out of the INSERT, so a record naming it
// refuses the body with ClickHouse's own code 117 rather than a Go key walk's
// 403 — and the exported row still carries it, holding the table's default.
func TestRoleTable_DeniedColumnIsUnnamableAndCarriesTheTableDefault(t *testing.T) {
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "tenant", "amount"}})

	assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, tbl.WireColumns)

	good := `{"id":1,"tenant":"acme","amount":5}` + "\n"
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(good))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	assert.True(t, batch.Rows[0].Accepted)
	assert.Equal(t, `[1, "acme", "", 5]`, string(batch.Rows[0].Line))
	assert.Equal(t, tbl.WireColumns, batch.Columns)

	batch, err = tbl.Ingest(FormatJSONEachRow, []byte(good+`{"id":2,"tenant":"acme","secret":"x","amount":5}`+"\n"))
	require.NoError(t, err)
	assert.Contains(t, requireRefused(t, batch, 117, 2).Message, "secret", "a denied column is a verdict about the data, not a decline")
}

// TestRoleTable_GeneratorDefaultOnADeniedColumn (#816): a denied column whose
// DEFAULT is a generator draws per record, as the server would, rather than
// refusing every record of the role; the export carries the drawn values.
func TestRoleTable_GeneratorDefaultOnADeniedColumn(t *testing.T) {
	ts := &discovery.TableSchema{Name: "events", Columns: []discovery.Column{
		{Name: "id", Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()", Position: 1},
		{Name: "tenant", Type: "String", Position: 2},
		{Name: "v", Type: "UInt32", Position: 3},
		{Name: "ts", Type: "DateTime64(3)", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "now64(3)", Position: 4},
	}}
	eng := testEngine(t, ts)
	for _, shape := range []RoleShape{
		{Columns: []string{"tenant", "v"}},
		{Columns: []string{"tenant", "v"}, Defaults: map[string]string{"tenant": "acme"}},
	} {
		tbl, err := eng.RoleTable(tenant.Default, "events", shape)
		require.NoError(t, err)
		batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"tenant":"acme","v":1}`+"\n"+`{"v":2}`+"\n"))
		require.NoError(t, err, "%+v", shape)
		require.Empty(t, batch.Declined, "%+v", shape)
		require.Len(t, batch.Rows, 2, "%+v", shape)
		assert.Equal(t, []string{"id", "tenant", "v", "ts"}, batch.Columns)

		ids := map[string]bool{}
		for i, r := range batch.Rows {
			require.True(t, r.Accepted, "%+v record %d: %s", shape, i, r.Message)
			var row []any
			require.NoError(t, json.Unmarshal(r.Line, &row))
			require.Len(t, row, 4, string(r.Line))
			id, _ := row[0].(string)
			assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$`, id, "a drawn v4 UUID")
			ids[id] = true
			assert.Regexp(t, `^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$`, row[3], "the evaluated now64(3)")
		}
		assert.Len(t, ids, 2, "one draw per record")
		if shape.Defaults != nil {
			assert.Contains(t, string(batch.Rows[1].Line), `"acme"`)
		}

		batch, err = tbl.Ingest(FormatJSONEachRow, []byte(`{"id":"00000000-0000-4000-8000-000000000001","tenant":"x","v":3}`+"\n"))
		tbl.Release()
		require.NoError(t, err)
		assert.Contains(t, requireRefused(t, batch, 117, 1).Message, "id")
	}
}

// TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue: a Defaults
// value fills a column the record omits, or sends as null where the column is
// not Nullable, and a value the record DOES supply still wins.
func TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue(t *testing.T) {
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": "acme"}})

	assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, tbl.WireColumns)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(
		`{"id":1,"amount":5}`+"\n"+
			`{"id":2,"tenant":"other","amount":5}`+"\n"+
			`{"id":3,"tenant":null,"amount":5}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 3)

	for i, want := range []string{`[1, "acme", "", 5]`, `[2, "other", "", 5]`, `[3, "acme", "", 5]`} {
		require.True(t, batch.Rows[i].Accepted, batch.Rows[i].Message)
		assert.Equal(t, want, string(batch.Rows[i].Line))
	}
}

// TestRoleTable_DefaultValueEscaping: the injected value is spelled as a JSON
// field (fieldIn), so every spelling that could end the field early has to
// survive as data. The breakout attempts are the cases that matter — none may
// fill another column.
func TestRoleTable_DefaultValueEscaping(t *testing.T) {
	eng := testEngine(t, ordersTable())

	for name, value := range map[string]string{
		"apostrophe":      "O'Brien",
		"quote":           `say "hi"`,
		"backslash":       `back\slash`,
		"backslash quote": `x\"y`,
		"trailing escape": `ends with \`,
		"json breakout":   `", "secret": "x`,
		"record breakout": "\"}\n{\"id\":9,\"secret\":\"x",
		"empty":           "",
		"nul":             "a\x00b",
		"newline and tab": "a\nb\tc",
		"unicode":         "Ünï 日本",
	} {
		t.Run(name, func(t *testing.T) {
			tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": value}})

			batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"amount":5}`+"\n"))
			require.NoError(t, err)
			require.Len(t, batch.Rows, 1)
			require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)

			// The exported line is ClickHouse's own writer on the stored value,
			// so it is the ground truth for what the default actually holds.
			want, err := json.Marshal(value)
			require.NoError(t, err)
			assert.Equal(t, `[1, `+string(want)+`, "", 5]`, string(batch.Rows[0].Line))
		})
	}
}

// formatsTable carries a column of each kind fieldIn spells differently: a
// string, a number, a zone-less DateTime, and a Bool (bare in JSON and CSV).
func formatsTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "formats", Columns: []discovery.Column{
		{Name: "k", Type: "UInt8", Position: 1},
		{Name: "s", Type: "String", Position: 2},
		{Name: "n", Type: "UInt32", Position: 3},
		{Name: "dt", Type: "DateTime", Position: 4},
		{Name: "b", Type: "Nullable(Bool)", IsNullable: true, Position: 5},
	}}
}

// TestRoleTable_DefaultIsReadInTheBodysFormat: chtypes reads a Defaults value
// in the body format's own field encoding, so each format gets its own
// spelling, a quote, comma, tab, newline and backslash included. The stored
// values are the same in every format, and a supplied field still wins.
func TestRoleTable_DefaultIsReadInTheBodysFormat(t *testing.T) {
	eng := testEngine(t, formatsTable())
	const s = "a\"b,c\td\ne\\f"
	tbl, err := eng.RoleTable(tenant.Default, "formats", RoleShape{Defaults: map[string]string{
		"s": s, "n": "42", "dt": "2026-01-15 10:30:00", "b": "true",
	}})
	require.NoError(t, err)
	defer tbl.Release()

	quoted, err := json.Marshal(s)
	require.NoError(t, err)
	filled := `[1, ` + string(quoted) + `, 42, "2026-01-15T10:30:00Z", true]`
	supplied := `[2, "own", 7, "2026-01-15T10:30:00Z", true]`
	for _, tc := range []struct {
		name   string
		format Format
		body   string
		want   []string
	}{
		{"JSONEachRow", FormatJSONEachRow, `{"k":1}` + "\n" + `{"k":2,"s":"own","n":7}` + "\n", []string{filled, supplied}},
		{"CSVWithNames", FormatCSVWithNames, "k,s,n\n1,,\n2,own,7\n", []string{filled, supplied}},
		{"TSVWithNames", FormatTSVWithNames, "k\n1\n", []string{filled}},
		{"TSVWithNames, supplied", FormatTSVWithNames, "k\ts\tn\n2\town\t7\n", []string{supplied}},
		{"CSV, empty fields", FormatCSV, "1,,,,\n2,own,7,,\n", []string{filled, supplied}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := tbl.Ingest(tc.format, []byte(tc.body))
			require.NoError(t, err)
			require.Empty(t, batch.Declined)
			require.Nil(t, batch.Refused)
			lines := make([]string, len(batch.Rows))
			for i, r := range batch.Rows {
				require.True(t, r.Accepted, "record %d: %s", i, r.Message)
				lines[i] = string(r.Line)
			}
			assert.Equal(t, tc.want, lines)
		})
	}
}

// TestRoleTable_UnreadableDefaultFailsClosed: a Defaults value its column
// cannot read refuses the call in every format, before any record is read. It
// is a RoleRefused — never a batch that silently drops the injection, and
// never an Unavailable, which a caller answers with a retry hint.
func TestRoleTable_UnreadableDefaultFailsClosed(t *testing.T) {
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"amount": "abc"}})

	for f, body := range map[Format]string{
		FormatJSONEachRow:  `{"id":1}` + "\n",
		FormatCSV:          "1,,,\n",
		FormatTSV:          "1\t\t\t\n",
		FormatCSVWithNames: "id\n1\n",
		FormatTSVWithNames: "id\n1\n",
	} {
		batch, err := tbl.Ingest(f, []byte(body))
		refused, ok := errors.AsType[*RoleRefused](err)
		require.True(t, ok, "format %d: %T: %v", f, err, err)
		assert.False(t, IsUnavailable(err))
		assert.Equal(t, "orders", refused.Table)
		assert.Contains(t, refused.Cause, "amount", "format %d", f)
		assert.Empty(t, batch.Rows)
	}
}

// TestRoleTable_DeniedColumnTakesWhatTheServerStores: a denied column keeps
// its declared kind, so every expression over it compiles, and the row holds
// what the server stores when an INSERT omits it — its own DEFAULT, or its
// type's default — with the DEFAULT columns over it computed from that.
func TestRoleTable_DeniedColumnTakesWhatTheServerStores(t *testing.T) {
	ts := &discovery.TableSchema{
		Name: "visits",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "ip", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'0.0.0.0'", Position: 2},
			{Name: "ip_hash", Type: "UInt64", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "cityHash64(ip)", Position: 3},
			{Name: "ip_len", Type: "UInt64", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "length(ip)", Position: 4},
			{Name: "n", Type: "Nullable(UInt8)", IsNullable: true, Position: 5},
			{Name: "n_set", Type: "UInt8", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "isNotNull(n)", Position: 6},
		},
	}
	eng := testEngine(t, ts)
	tbl, err := eng.RoleTable(tenant.Default, "visits", RoleShape{Columns: []string{"page", "ip_len", "n_set"}})
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, []string{"page", "ip", "ip_len", "n", "n_set"}, tbl.WireColumns)
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"page":"/home"}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `["/home", "0.0.0.0", 7, null, 0]`, string(batch.Rows[0].Line), "length('0.0.0.0'), and n at its type default NULL")
	for col, rec := range map[string]string{"ip": `{"page":"/a","ip":"1.2.3.4"}`, "n": `{"page":"/b","n":1}`} {
		batch, err = tbl.Ingest(FormatJSONEachRow, []byte(`{"page":"/home"}`+"\n"+rec+"\n"))
		require.NoError(t, err)
		assert.Contains(t, requireRefused(t, batch, 117, 2).Message, col)
	}

	// A check over a denied column tests what the server will store there.
	check, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"page":"/home"}`+"\n"+`{"page":"/b"}`+"\n"),
		Predicate{Column: "ip", Op: "=", Values: []string{"0.0.0.0"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"", ""}, checkReasons(t, check))
	check, err = tbl.Ingest(FormatJSONEachRow, []byte(`{"page":"/home"}`+"\n"),
		Predicate{Column: "ip", Op: "=", Values: []string{"1.2.3.4"}})
	require.NoError(t, err)
	assert.Equal(t, []string{ReasonFilter}, checkReasons(t, check))
}

// TestRoleTable_PositionalBodyMapsToTheRolesColumns: a positional CSV or TSV
// line carries the role's writable wire columns, in declaration order whatever
// the order of Columns (a name the table lacks, or a repeated one, changes
// nothing), and the row the table's whole wire list; a line that also spells
// the denied columns has too many fields.
func TestRoleTable_PositionalBodyMapsToTheRolesColumns(t *testing.T) {
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"amount", "id", "nosuch", "id"}})

	for f, body := range map[Format]string{FormatCSV: "1,5\n", FormatTSV: "1\t5\n"} {
		batch, err := tbl.Ingest(f, []byte(body))
		require.NoError(t, err)
		require.Len(t, batch.Rows, 1, "format %d", f)
		require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
		assert.Equal(t, `[1, "", "", 5]`, string(batch.Rows[0].Line), "format %d", f)
	}
	batch, err := tbl.Ingest(FormatCSV, []byte("1,x,y,5\n"))
	require.NoError(t, err)
	requireRefused(t, batch, 117, 1)
}

// TestRoleTable_ShapeWithNothingToWriteIsRefused: a role that may write no
// plain or DEFAULT column could supply nothing in any record.
func TestRoleTable_ShapeWithNothingToWriteIsRefused(t *testing.T) {
	ts := ordersTable()
	ts.Columns = append(ts.Columns, discovery.Column{
		Name: "double", Type: "UInt64", HasDefault: true,
		DefaultKind: "MATERIALIZED", DefaultExpression: "amount * 2", Position: 5,
	})
	eng := testEngine(t, ts)
	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	base.Release()

	for _, cols := range [][]string{{}, {"double"}, {"nosuch"}} {
		_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Columns: cols})
		refused, ok := errors.AsType[*RoleRefused](err)
		require.True(t, ok, "%v: %T %v", cols, err, err)
		assert.Contains(t, refused.Cause, "no plain or DEFAULT column")
	}
	require.True(t, base.mu.TryLock(), "a refusal releases the base table")
	base.mu.Unlock()
}

// TestRoleTable_EphemeralFollowsTheRoleColumns: a record may supply an
// EPHEMERAL column only when the role may write it. Denied, the DEFAULT over
// it takes its own default, and a record naming it is refused. A DEFAULT the
// role may not write still reads a value the role supplies, as the server's
// own INSERT would.
func TestRoleTable_EphemeralFollowsTheRoleColumns(t *testing.T) {
	ts := &discovery.TableSchema{
		Name: "eph",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "secret", Type: "String", Position: 2},
			{Name: "ip", Type: "String", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 3},
			{Name: "ip_len", Type: "UInt64", DefaultKind: "DEFAULT", DefaultExpression: "length(ip)", HasDefault: true, Position: 4},
		},
	}
	eng := testEngine(t, ts)
	body := []byte(`{"page":"/a","ip":"1.2.3.4"}` + "\n")

	for _, cols := range [][]string{{"page", "ip", "ip_len"}, {"page", "ip"}} {
		allowed, err := eng.RoleTable(tenant.Default, "eph", RoleShape{Columns: cols})
		require.NoError(t, err)
		batch, err := allowed.Ingest(FormatJSONEachRow, body)
		allowed.Release()
		require.NoError(t, err)
		require.True(t, batch.Rows[0].Accepted, "%v: %s", cols, batch.Rows[0].Message)
		assert.Equal(t, `["/a", "", 7]`, string(batch.Rows[0].Line), "%v", cols)
	}

	denied, err := eng.RoleTable(tenant.Default, "eph", RoleShape{Columns: []string{"page", "ip_len"}})
	require.NoError(t, err)
	refused, err := denied.Ingest(FormatJSONEachRow, append(body, `{"page":"/b"}`+"\n"...))
	require.NoError(t, err)
	requireRefused(t, refused, 117, 1)
	batch, err := denied.Ingest(FormatJSONEachRow, []byte(`{"page":"/b"}`+"\n"))
	denied.Release()
	require.NoError(t, err)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `["/b", "", 0]`, string(batch.Rows[0].Line))
}

// TestRoleTable_ContradictoryShapeIsAnError: a default for a column the shape
// does not carry cannot be expressed. Dropping it silently would turn "force
// this value" into "whatever the caller sent".
func TestRoleTable_ContradictoryShapeIsAnError(t *testing.T) {
	eng := testEngine(t, ordersTable())

	_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{
		Columns:  []string{"id", "amount"},
		Defaults: map[string]string{"tenant": "acme"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant")

	_, err = eng.RoleTable(tenant.Default, "orders", RoleShape{Defaults: map[string]string{"nosuch": "acme"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nosuch")
}

// TestRoleTable_DefaultIntoAComputedColumnIsRefused: MATERIALIZED/ALIAS/
// EPHEMERAL are never supplied by a record, and a default for one would
// change what the server stores.
func TestRoleTable_DefaultIntoAComputedColumnIsRefused(t *testing.T) {
	ts := ordersTable()
	ts.Columns = append(ts.Columns, discovery.Column{
		Name: "double", Type: "UInt64", HasDefault: true,
		DefaultKind: "MATERIALIZED", DefaultExpression: "amount * 2", Position: 5,
	})
	eng := testEngine(t, ts)

	_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Defaults: map[string]string{"double": "1"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MATERIALIZED")

	// A computed column stays off the wire whatever the allow-list says.
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "amount", "double"}})
	assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, tbl.WireColumns)
	assert.Equal(t, []string{`[1, "", "", 5]`}, exported(t, tbl, `{"id":1,"amount":5}`+"\n"))
}

// TestRoleTable_ConcurrentViewsAndRebind races views of distinct shapes,
// each injecting its own claim, against rebinds. Under -race it pins that a
// view's Release is its base table's, so every rebind waits out the views
// holding the old generation, and every view answers with its own claim.
func TestRoleTable_ConcurrentViewsAndRebind(t *testing.T) {
	eng := testEngine(t, ordersTable())

	var wg sync.WaitGroup
	for g := range 12 {
		wg.Go(func() {
			for i := range 25 {
				claim := "c" + strconv.Itoa((g*7+i)%10)
				tbl, err := eng.RoleTable(tenant.Default, "orders", RoleShape{
					Columns: []string{"id", "tenant", "amount"}, Defaults: map[string]string{"tenant": claim},
				})
				if !assert.NoError(t, err) {
					return
				}
				batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"amount":5}`+"\n"))
				tbl.Release()
				if assert.NoError(t, err) && assert.Len(t, batch.Rows, 1) {
					assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
					assert.Equal(t, `[1, "`+claim+`", "", 5]`, string(batch.Rows[0].Line))
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 6 {
			changed := ordersTable()
			if i%2 == 0 {
				changed.Columns[3].Type = "UInt32"
			}
			eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})
		}
	})
	wg.Wait()

	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	defer base.Release()
	assert.Equal(t, uint64(7), base.Generation)
}
