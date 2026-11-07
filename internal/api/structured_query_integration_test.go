//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/chversion"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/query"
)

// filterCHImage is the server the differential runs against;
// WAVEHOUSE_TEST_CLICKHOUSE_IMAGE names another line to compare.
const filterCHImage = chversion.TestImage

// startFilterClickHouse runs a ClickHouse whose server zone is Europe/Berlin
// — not UTC, and different from every zone a test column declares — so a
// value read in the wrong zone lands on the wrong row.
func startFilterClickHouse(t *testing.T) chconn.Target {
	t.Helper()
	ctx := context.Background()
	image := filterCHImage
	if v := os.Getenv("WAVEHOUSE_TEST_CLICKHOUSE_IMAGE"); v != "" {
		image = v
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"8123/tcp"},
			Env:          map[string]string{"CLICKHOUSE_PASSWORD": "test"},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader("<clickhouse><timezone>Europe/Berlin</timezone></clickhouse>"),
				ContainerFilePath: "/etc/clickhouse-server/config.d/timezone.xml",
				FileMode:          0o644,
			}},
			// The image's VOLUME would otherwise leave an anonymous volume behind.
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Tmpfs = map[string]string{"/var/lib/clickhouse": ""}
			},
			WaitingFor: wait.ForHTTP("/ping").WithPort("8123/tcp").WithStatusCodeMatcher(func(status int) bool {
				return status == http.StatusOK
			}).WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "8123")
	require.NoError(t, err)
	return chconn.Target{URL: "http://" + net.JoinHostPort(host, port.Port()), Username: "default", Password: "test", Database: "default"}
}

// filterDB is one ClickHouse and the reader the read path uses on it.
type filterDB struct {
	t      *testing.T
	target chconn.Target
	ch     *chReader
}

func (db *filterDB) exec(sql string) {
	db.t.Helper()
	_, err := db.ch.do(db.t.Context(), db.target, 1, chRequest{sql: sql, write: true})
	require.NoError(db.t, err, sql)
}

// schema is table's schema as ClickHouse reports it, the types discovery
// reads.
func (db *filterDB) schema(table string) *discovery.TableSchema {
	db.t.Helper()
	data, err := db.ch.do(db.t.Context(), db.target, 1, chRequest{
		sql:    "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = {t:String} ORDER BY position",
		params: []query.Param{{Name: "t", Value: table}},
	})
	require.NoError(db.t, err)
	var cols []discovery.Column
	require.NoError(db.t, json.Unmarshal(data, &cols))
	return &discovery.TableSchema{Name: table, Columns: cols}
}

// bound builds q the way the structured query handler does.
func (db *filterDB) bound(table string, q query.StructuredQuery) *query.Bound {
	db.t.Helper()
	res, err := query.Build(table, &q, db.schema(table), nil, 0, query.DefaultMaxRows)
	require.NoError(db.t, err)
	b, err := res.Bind()
	require.NoError(db.t, err)
	require.NoError(db.t, checkRequestSize(b))
	return b
}

func (db *filterDB) run(b *query.Bound, sql string) ([]byte, error) {
	return db.ch.do(db.t.Context(), db.target, 1, chRequest{sql: sql, params: b.Params, tables: b.Tables, settings: chReadSettings(chQueryLimits{ExecutionTime: 20 * time.Second})})
}

// ids runs q on table and returns the id of every row, sorted.
func (db *filterDB) ids(table string, filters []query.Filter, tr *query.TimeRange) ([]int, error) {
	db.t.Helper()
	b := db.bound(table, query.StructuredQuery{Columns: query.Columns{"id"}, Filters: filters, TimeRange: tr})
	data, err := db.run(b, b.SQL)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		ID int `json:"id"`
	}
	require.NoError(db.t, json.Unmarshal(data, &rows))
	out := []int{}
	for _, r := range rows {
		out = append(out, r.ID)
	}
	slices.Sort(out)
	return out, nil
}

var granulesRe = regexp.MustCompile(`Granules: (\d+)/(\d+)`)

// granules runs EXPLAIN indexes = 1 over q and returns the primary key's
// selected and total granules.
func (db *filterDB) granules(table string, filters []query.Filter, tr *query.TimeRange) (int, int) {
	db.t.Helper()
	b := db.bound(table, query.StructuredQuery{Columns: query.Columns{"id"}, Filters: filters, TimeRange: tr})
	data, err := db.run(b, "EXPLAIN indexes = 1 "+b.SQL)
	require.NoError(db.t, err)
	var rows []struct {
		Explain string `json:"explain"`
	}
	require.NoError(db.t, json.Unmarshal(data, &rows))
	for _, r := range rows {
		if m := granulesRe.FindStringSubmatch(r.Explain); m != nil {
			sel, _ := strconv.Atoi(m[1])
			total, _ := strconv.Atoi(m[2])
			return sel, total
		}
	}
	db.t.Fatalf("no primary key granules in EXPLAIN of %s", b.SQL)
	return 0, 0
}

func filterOn(col, op string, v any) []query.Filter {
	return []query.Filter{{Column: col, Op: op, Value: v}}
}

func idSpan(from, to int) []int {
	var out []int
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// TestIntegration_FilterValuesAgainstClickHouse is the differential for the
// read path's filter binding: real rows, real primary keys, and a server zone
// that is not UTC. Each time table holds id k at 2026-06-21T00:00:00Z + k×30
// min (k = 0…47), so 04:00Z is id 8 and 05:00Z id 10; tn also holds id 1000
// at 04:00:00.500Z. A value parsed in the wrong zone lands hours away.
func TestIntegration_FilterValuesAgainstClickHouse(t *testing.T) {
	db := &filterDB{t: t, target: startFilterClickHouse(t), ch: newCHReader(readerHTTPClient)}
	for _, stmt := range []string{
		"CREATE TABLE tk (id UInt32, ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1",
		"INSERT INTO tk SELECT number, toDateTime('2026-06-21 00:00:00', 'UTC') + number * 1800 FROM numbers(48)",
		"CREATE TABLE tn (id UInt32, ts DateTime64(3, 'America/New_York')) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1",
		"INSERT INTO tn SELECT number, toDateTime64('2026-06-21 00:00:00', 3, 'UTC') + toIntervalSecond(number * 1800) FROM numbers(48)",
		"INSERT INTO tn SELECT 1000, toDateTime64('2026-06-21 04:00:00.500', 3, 'UTC')",
		"OPTIMIZE TABLE tn FINAL",
		"CREATE TABLE tp (id UInt32, ts DateTime) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1",
		"INSERT INTO tp SELECT number, toDateTime('2026-06-21 00:00:00', 'UTC') + number * 1800 FROM numbers(48)",
		"CREATE TABLE td (id UInt32, d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1",
		"INSERT INTO td SELECT number, toDate('2026-06-15') + number FROM numbers(14)",
		"CREATE TABLE tv (id UInt32, s String, dec Decimal(10, 2), u8 UInt8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1",
		`INSERT INTO tv VALUES (1, 'plain', 12.50, 1), (2, 'it''s', 3, 2), (3, 'a\\b', 0, 3), (4, 'a\tb', 0, 4), (5, 'a\nb', 0, 5), (6, '\\N', 0, 6), (7, '', 0, 7)`,
	} {
		db.exec(stmt)
	}

	// The column's zone, as each time table spells 04:00Z without one.
	local := map[string]string{"tk": "2026-06-21 13:00:00", "tn": "2026-06-21 00:00:00", "tp": "2026-06-21 06:00:00"}
	for _, table := range []string{"tk", "tn", "tp"} {
		t.Run(table, func(t *testing.T) {
			db := &filterDB{t: t, target: db.target, ch: db.ch}
			// tn's id 1000 sits at 04:00:00.500Z, between ids 8 and 9.
			half := func(ids ...int) []int {
				if table == "tn" {
					ids = append(ids, 1000)
				}
				return ids
			}
			cases := []struct {
				name    string
				filters []query.Filter
				tr      *query.TimeRange
				want    []int
			}{
				{"eq Z", filterOn("ts", "eq", "2026-06-21T04:00:00Z"), nil, []int{8}},
				{"eq offset", filterOn("ts", "eq", "2026-06-21T13:00:00+09:00"), nil, []int{8}},
				{"eq zone-less, read in the column's zone", filterOn("ts", "eq", local[table]), nil, []int{8}},
				{"eq unix seconds", filterOn("ts", "eq", json.Number("1782014400")), nil, []int{8}},
				{"gt a fraction", filterOn("ts", "gt", "2026-06-21T04:00:00.5Z"), nil, idSpan(9, 47)},
				{"gte a fraction", filterOn("ts", "gte", "2026-06-21T04:00:00.5Z"), nil, half(idSpan(9, 47)...)},
				{"lt a fraction", filterOn("ts", "lt", "2026-06-21T04:00:00.5Z"), nil, idSpan(0, 8)},
				{"eq a fraction", filterOn("ts", "eq", "2026-06-21T04:00:00.5Z"), nil, half()},
				{"in", filterOn("ts", "in", []any{"2026-06-21T04:00:00Z", "2026-06-21T14:00:00+09:00"}), nil, []int{8, 10}},
				{"in a fraction", filterOn("ts", "in", []any{"2026-06-21T04:00:00.5Z"}), nil, half()},
				{"time_range", nil, &query.TimeRange{Column: "ts", Since: "2026-06-21T04:00:00Z", Until: "2026-06-21T05:00:00Z"}, half(8, 9, 10)},
			}
			for _, c := range cases {
				got, err := db.ids(table, c.filters, c.tr)
				require.NoError(t, err, c.name)
				want := c.want
				slices.Sort(want)
				if want == nil {
					want = []int{}
				}
				assert.Equal(t, want, got, c.name)
			}

			// The primary key narrows every shape the builder emits.
			for _, c := range []struct {
				name    string
				filters []query.Filter
				tr      *query.TimeRange
			}{
				{"eq", filterOn("ts", "eq", "2026-06-21T04:00:00Z"), nil},
				{"time_range", nil, &query.TimeRange{Column: "ts", Since: "2026-06-21T04:00:00Z", Until: "2026-06-21T05:00:00Z"}},
				{"in", filterOn("ts", "in", []any{"2026-06-21T04:00:00Z", "2026-06-21T14:00:00+09:00"}), nil},
			} {
				sel, total := db.granules(table, c.filters, c.tr)
				assert.Less(t, sel, total/4, "%s: the primary key must narrow the read (%d/%d granules)", c.name, sel, total)
			}

			// A value ClickHouse cannot parse is its refusal, classed as one.
			for _, filters := range [][]query.Filter{filterOn("ts", "eq", "banana"), filterOn("ts", "in", []any{"2026-06-21T04:00:00Z", "banana"})} {
				_, err := db.ids(table, filters, nil)
				require.Error(t, err)
				assert.Equal(t, chconn.Rejected, chconn.Classify(err), "%v", err)
			}

			// An in list far past the 128 KiB a query parameter takes.
			big := make([]any, 0, 60048)
			for k := range 48 {
				big = append(big, time.Date(2026, 6, 21, 0, 0, 0, 0, time.UTC).Add(time.Duration(k)*30*time.Minute).Format(time.RFC3339))
			}
			for k := range 60000 {
				big = append(big, time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(k)*time.Second).Format(time.RFC3339))
			}
			got, err := db.ids(table, filterOn("ts", "in", big), nil)
			require.NoError(t, err)
			assert.Equal(t, idSpan(0, 47), got)
		})
	}

	t.Run("Date", func(t *testing.T) {
		db := &filterDB{t: t, target: db.target, ch: db.ch}
		for _, c := range []struct {
			name string
			v    any
			want []int
		}{
			{"a date", "2026-06-21", []int{6}},
			{"an instant, on its date in the server's zone", "2026-06-21T04:00:00Z", []int{6}},
		} {
			got, err := db.ids("td", filterOn("d", "eq", c.v), nil)
			require.NoError(t, err, c.name)
			assert.Equal(t, c.want, got, c.name)
		}
		got, err := db.ids("td", filterOn("d", "in", []any{"2026-06-21", "2026-06-23T10:00:00Z"}), nil)
		require.NoError(t, err)
		assert.Equal(t, []int{6, 8}, got)
		sel, total := db.granules("td", filterOn("d", "in", []any{"2026-06-21", "2026-06-23T10:00:00Z"}), nil)
		assert.Less(t, sel, total/2, "%d/%d granules", sel, total)
	})

	t.Run("values arrive as written", func(t *testing.T) {
		db := &filterDB{t: t, target: db.target, ch: db.ch}
		values := map[int]string{1: "plain", 2: "it's", 3: `a\b`, 4: "a\tb", 5: "a\nb", 6: `\N`, 7: ""}
		var all []any
		for id, v := range values {
			got, err := db.ids("tv", filterOn("s", "eq", v), nil)
			require.NoError(t, err, "%q", v)
			assert.Equal(t, []int{id}, got, "eq %q", v)
			all = append(all, v)
		}
		got, err := db.ids("tv", filterOn("s", "in", all), nil)
		require.NoError(t, err)
		assert.Equal(t, idSpan(1, 7), got)

		// An in list compares under the column's type, not as text: 3.50 is
		// the Decimal 3.00's neighbour, 3 and 12.5 are its values; 256 is no
		// UInt8 and matches nothing.
		got, err = db.ids("tv", filterOn("dec", "in", []any{json.Number("12.5"), "3.00", "3.50"}), nil)
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, got)
		got, err = db.ids("tv", filterOn("u8", "in", []any{json.Number("2"), json.Number("256")}), nil)
		require.NoError(t, err)
		assert.Equal(t, []int{2}, got)
		sel, total := db.granules("tv", filterOn("id", "in", []any{json.Number("2"), json.Number("5")}), nil)
		assert.Less(t, sel, total, "%d/%d granules", sel, total)

		big := make([]any, 0, 100000)
		for i := range 100000 {
			big = append(big, fmt.Sprintf("value-%d", i))
		}
		big = append(big, "a\tb")
		got, err = db.ids("tv", filterOn("s", "in", big), nil)
		require.NoError(t, err)
		assert.Equal(t, []int{4}, got)
	})
}
