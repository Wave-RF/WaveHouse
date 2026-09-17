//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypelayerWire_PublishedRowIsTheStoredRow is the one claim the whole
// migration rests on: the bytes WaveHouse publishes — to NATS, to the SSE
// stream, to the DLQ — are the row ClickHouse itself will hold. They are
// produced by the server's own writer at validation time, so a subscriber
// reading the stream and a client reading /v1/query cannot disagree about a
// value, and the worker's INSERT re-parses bytes the server already wrote.
//
// This replaces the timestamp differential oracle (a corpus of ~30 producer
// spellings × 6 column shapes, asserting WaveHouse's canonicalizer landed on
// the same instant as the raw value). That oracle existed to prove a
// hand-written parser matched ClickHouse; there is no longer a second parser to
// disagree, so one end-to-end identity check is the whole remaining claim.
// Deliberately one test, not a corpus.
func TestTypelayerWire_PublishedRowIsTheStoredRow(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	before := time.Now().UTC().Add(-time.Minute)

	table := createTable(t,
		"user_id String, "+
			"ts DateTime('UTC'), "+
			"ts_ms DateTime64(3, 'UTC'), "+
			"amount Decimal(10, 2), "+
			"small UInt8, "+
			"tags Array(String), "+
			"maybe Nullable(String)",
		"ORDER BY user_id",
	)

	// Every value in a spelling ClickHouse does NOT store it in: an offset
	// timestamp, an epoch-tick DateTime64, a string decimal, and an integer past
	// the column's width (which wraps — the stored truth, not the producer's).
	body := `{"user_id":"erin",` +
		`"ts":"2026-06-21T06:00:00+02:00",` +
		`"ts_ms":1782014400123,` +
		`"amount":"12.50",` +
		`"small":256,` +
		`"tags":["a","b"],` +
		`"maybe":null}`

	resp, err := http.Post(
		e.server.URL+"/v1/ingest?table="+url.QueryEscape(table),
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 30s upper bound for the worker's 5s batch window plus loaded-runner slack.
	require.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s WHERE user_id = 'erin'", table),
		).Scan(&count)
		return err == nil && count == 1
	}, 30*time.Second, 250*time.Millisecond, "row never landed")

	// Read the stored row back in the same format the wire carries, so the two
	// are comparable as bytes rather than through two different renderings.
	stored := selectJSONCompactRow(t, e.chHTTPURL,
		fmt.Sprintf("SELECT user_id, ts, ts_ms, amount, small, tags, maybe FROM %s WHERE user_id = 'erin'", table))

	wire := publishedWireRow(t, e.server.URL, table, before)
	assert.Equal(t, stored, wire,
		"the published row and the stored row must be the same values: wire=%v stored=%v", wire, stored)

	// And spot-check that these really are the coerced values, not the
	// producer's spellings passed through.
	require.Len(t, wire, 7)
	assert.Equal(t, "2026-06-21 04:00:00", wire[1], "offset applied, rendered in the column's zone")
	assert.Equal(t, "2026-06-21 04:00:00.123", wire[2], "ticks at the column's precision")
	assert.EqualValues(t, 0, wire[4], "256 into a UInt8 wraps — the stored truth")
}

// publishedWireRow replays the table's SSE stream from before the insert, so
// the test reads the published bytes without racing the publish.
func publishedWireRow(t *testing.T, serverURL, table string, since time.Time) []any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet,
		serverURL+"/v1/stream?table="+url.QueryEscape(table)+
			"&since="+url.QueryEscape(since.Format(time.RFC3339Nano)), nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cells, ok := firstDataRow(t, resp.Body)
	require.True(t, ok, "no data frame arrived on the replay stream")
	return cells
}

// firstDataRow reads SSE frames until one carries a row, and returns its cells.
func firstDataRow(t *testing.T, body interface{ Read([]byte) (int, error) }) ([]any, bool) {
	t.Helper()
	buf := make([]byte, 0, 8192)
	chunk := make([]byte, 4096)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		n, err := body.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			for _, line := range strings.Split(string(buf), "\n") {
				payload, found := strings.CutPrefix(line, "data: ")
				if !found {
					continue
				}
				var frame struct {
					Row []any `json:"row"`
				}
				if json.Unmarshal([]byte(payload), &frame) == nil && frame.Row != nil {
					return frame.Row, true
				}
			}
		}
		if err != nil {
			return nil, false
		}
	}
	return nil, false
}

// selectJSONCompactRow reads one row back through ClickHouse's HTTP interface in
// JSONCompactEachRow — the same writer that produced the published row, so the
// two renderings are comparable without a second interpretation step.
func selectJSONCompactRow(t *testing.T, chHTTPURL, query string) []any {
	t.Helper()
	q := url.Values{}
	q.Set("database", testCHDatabase)
	q.Set("query", query+" FORMAT JSONCompactEachRow")

	req, err := http.NewRequest(http.MethodGet, chHTTPURL+"?"+q.Encode(), nil)
	require.NoError(t, err)
	req.Header.Set("X-ClickHouse-User", testCHUser)
	req.Header.Set("X-ClickHouse-Key", testCHPassword)

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var cells []any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&cells))
	return cells
}
