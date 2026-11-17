//go:build integration

package typelayer

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Wave-RF/WaveHouse/internal/chversion"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// zonedCH is a ClickHouse over its HTTP interface.
type zonedCH struct {
	t   *testing.T
	url string
}

// startZonedClickHouse runs the pinned test ClickHouse with its server zone
// set to zone.
func startZonedClickHouse(t *testing.T, zone string) *zonedCH {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        chversion.TestImage,
			ExposedPorts: []string{"8123/tcp"},
			Env:          map[string]string{"CLICKHOUSE_PASSWORD": "test"},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader("<clickhouse><timezone>" + zone + "</timezone></clickhouse>"),
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
	return &zonedCH{t: t, url: "http://" + net.JoinHostPort(host, port.Port())}
}

// do runs sql, with settings and query parameters as URL parameters, and
// returns the body.
func (ch *zonedCH) do(sql string, settings, params map[string]string) string {
	ch.t.Helper()
	q := url.Values{}
	for k, v := range settings {
		q.Set(k, v)
	}
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	req, err := http.NewRequestWithContext(ch.t.Context(), http.MethodPost, ch.url+"/?"+q.Encode(), strings.NewReader(sql))
	require.NoError(ch.t, err)
	req.SetBasicAuth("default", "test")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(ch.t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(ch.t, err)
	require.Equal(ch.t, http.StatusOK, resp.StatusCode, "%s\n%s", sql, body)
	return string(body)
}

// lines is a query's JSONCompactEachRow answer, rendered as the type layer's
// export renders it (the read path pins the same two settings), one string
// per row.
func (ch *zonedCH) lines(sql string, params map[string]string) []string {
	ch.t.Helper()
	out := ch.do(sql+" FORMAT JSONCompactEachRow", map[string]string{
		"date_time_output_format": "iso", "output_format_json_escape_forward_slashes": "0",
	}, params)
	rows := []string{}
	for line := range strings.SplitSeq(strings.TrimSuffix(out, "\n"), "\n") {
		if line != "" {
			rows = append(rows, line)
		}
	}
	return rows
}

// schema is table's columns as discovery reads them.
func (ch *zonedCH) schema(table string) *discovery.TableSchema {
	ch.t.Helper()
	out := ch.do(`SELECT name, type, default_kind, default_expression, position FROM system.columns
		WHERE database = currentDatabase() AND table = {t:String} ORDER BY position FORMAT JSONEachRow`,
		map[string]string{"output_format_json_quote_64bit_integers": "0"}, map[string]string{"t": table})
	ts := &discovery.TableSchema{Name: table}
	dec := json.NewDecoder(strings.NewReader(out))
	for dec.More() {
		var c discovery.Column
		require.NoError(ch.t, dec.Decode(&c))
		c.HasDefault = c.DefaultKind != ""
		ts.Columns = append(ts.Columns, c)
	}
	return ts
}

// TestIntegration_ServerZoneAgreesWithClickHouse is the differential for the
// server handle: a tenant bound to a ClickHouse whose zone is Asia/Tokyo
// exports, for zone-less and instant DateTime records, exactly the row that
// server stores for them (DEFAULT expressions and timezone() included), the
// worker's insert of that export stores it unchanged, and the stream's
// compiled row filter, the insert check, and the server's own WHERE agree on
// every stored row.
func TestIntegration_ServerZoneAgreesWithClickHouse(t *testing.T) {
	ch := startZonedClickHouse(t, "Asia/Tokyo")
	ch.do(`CREATE TABLE computed (id UInt8, ts DateTime, day Date DEFAULT toDate(ts), hour UInt8 DEFAULT toHour(ts),
		tz String DEFAULT timezone(), minute UInt8 MATERIALIZED toMinute(ts)) ENGINE = MergeTree ORDER BY id`, nil, nil)
	ch.do("CREATE TABLE direct AS computed", nil, nil)

	version := strings.TrimSpace(ch.do("SELECT version()", nil, nil))
	zone := strings.TrimSpace(ch.do("SELECT timezone()", nil, nil))
	schema := ch.schema("computed")
	require.Equal(t, computedTable().Columns, schema.Columns, "the unit tests' fixture is this table")
	eng := testEngine(t)
	eng.Bind("tokyo", version, zone, []*discovery.TableSchema{schema})
	tbl := tableOf(t, eng, "tokyo", "computed")
	export := exported(t, tbl, computedBody)

	ch.do("INSERT INTO computed ("+strings.Join(tbl.WireColumns, ", ")+") FORMAT JSONCompactEachRow\n"+strings.Join(export, "\n"),
		InsertSettings(), nil)
	ch.do("INSERT INTO direct FORMAT JSONEachRow\n"+computedBody, InsertSettings(), nil)

	stored := ch.lines("SELECT id, ts, day, hour, tz FROM direct ORDER BY id", nil)
	assert.Equal(t, stored, export, "the export is the row the server stores for the record")
	assert.Equal(t, stored, ch.lines("SELECT id, ts, day, hour, tz FROM computed ORDER BY id", nil),
		"and the worker's insert stores it as exported")
	assert.Equal(t, computedWant[zone], stored, "the unit tests expect what this server stores")
	assert.Equal(t, ch.lines("SELECT id, minute FROM direct ORDER BY id", nil),
		ch.lines("SELECT id, minute FROM computed ORDER BY id", nil), "MATERIALIZED is the server's to compute")

	for _, preds := range [][]Predicate{
		{{Column: "ts", Op: "=", Values: []string{"2024-01-01 05:00:00"}}},
		{{Column: "ts", Op: "=", Values: []string{"2024-01-01 20:00:00"}}},
		{{Column: "ts", Op: ">", Values: []string{"2024-01-01 00:00:00"}}, {Column: "ts", Op: "<", Values: []string{"2024-01-02 00:00:00"}}},
		{{Column: "ts", Op: ">", Values: []string{"2024-01-01 12:00:00"}}, {Column: "ts", Op: "<", Values: []string{"2024-01-02 12:00:00"}}},
		{{Column: "day", Op: "=", Values: []string{"2024-01-02"}}},
		{{Column: "hour", Op: "in", Values: []string{"5", "20"}}},
		{{Column: "tz", Op: "=", Values: []string{"Asia/Tokyo"}}},
	} {
		expr, params, ok := tbl.render(preds)
		require.True(t, ok)
		server := ch.lines("SELECT id FROM direct WHERE "+expr+" ORDER BY id", params)

		stream, checked := []string{}, []string{}
		for i, line := range export {
			row, err := tbl.ParseRow(tbl.WireColumns, []byte(line))
			require.NoError(t, err)
			if row.Visible(preds) {
				stream = append(stream, "["+strings.SplitN(line[1:], ",", 2)[0]+"]")
			}
			row.Close()
			batch, err := tbl.Ingest(FormatJSONEachRow, []byte(strings.SplitAfter(computedBody, "\n")[i]), preds...)
			require.NoError(t, err)
			if checkReasons(t, batch)[0] == "" {
				checked = append(checked, "["+strings.SplitN(line[1:], ",", 2)[0]+"]")
			}
		}
		assert.Equal(t, server, stream, "stream row filter vs WHERE %s %v", expr, params)
		assert.Equal(t, server, checked, "insert check vs WHERE %s %v", expr, params)
	}
}
