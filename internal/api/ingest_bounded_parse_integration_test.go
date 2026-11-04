//go:build integration

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// TestIngest_NearCapBodyOnAMapTable: a body near the 16 MiB cap on a table with
// a Map column, which one parse over it declines (ClickHouse's parallel
// parsing cut at 10 MiB), is accepted record for record.
func TestIngest_NearCapBodyOnAMapTable(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{{Name: "maps", Columns: []discovery.Column{
		{Name: "n", Type: "UInt64"},
		{Name: "m", Type: "Map(String, UInt64)"},
	}}})
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, reg, pub)
	line := `{"n":1,"m":{"k":1,"padding_to_make_each_record_longer":12345}}` + "\n"
	n := (maxRequestBodyBytes - 1<<20) / len(line)
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "maps", "application/x-ndjson", strings.Repeat(line, n))))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, n, resp.Total)
	assert.Equal(t, n, resp.Succeeded)
	assert.Len(t, pub.Messages, n)
}

func TestIngest_NDJSON_ErrorsTruncated(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := dedupHandler(t, pub, testutil.NewMockDeduplicator(), true)

	// Every record lacks the dedupe id the settings require: a per-record
	// rejection each.
	const total = maxReportedResults + 1
	lines := make([]string, total)
	for i := range lines {
		lines[i] = `{"page":"/a"}`
	}
	req := ndjsonRequest(t, "clicks", lines...)
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(req))

	assert.Equal(t, http.StatusOK, w.Code)
	resp := decodeBatchResult(t, w)
	assert.Equal(t, total, resp.Total)
	assert.Equal(t, total, resp.Failed)
	// Failed counts everything; the echoed per-record results are capped.
	assert.Len(t, resp.Results, maxReportedResults)
	assert.Empty(t, pub.Messages)
}

// TestIngest_ParseWindowsPublishInOrder: a body longer than one parse window
// publishes every record once, in body order, with one received time.
func TestIngest_ParseWindowsPublishInOrder(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	const n = 10_000 // three default windows
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/x-ndjson", manyClicks(n, 0, ""))))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	resp := decodeBatchResult(t, w)
	assert.Equal(t, n, resp.Total)
	assert.Equal(t, n, resp.Succeeded)

	require.Len(t, pub.Messages, n)
	received := map[string]bool{}
	for i, m := range pub.Messages {
		var evt ingest.EventMessage
		require.NoError(t, json.Unmarshal(m.Data, &evt))
		var row []any
		require.NoError(t, json.Unmarshal(evt.Row, &row))
		require.Equal(t, fmt.Sprintf("/%d", i+1), row[0], "message %d", i)
		received[evt.ReceivedTimestamp] = true
	}
	assert.Len(t, received, 1, "one request, one received time")
}

// TestIngest_Windows_PublishFailureAtK_DefaultWindows: the unit test's cases
// at the default 256-record window over 600 records.
func TestIngest_Windows_PublishFailureAtK_DefaultWindows(t *testing.T) {
	t.Parallel()
	runPublishFailureAtK(t, ingestWindow, 600, publishFailureKs{midFirst: 100, midSecond: 400, midLast: 590})
}
