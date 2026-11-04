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

// manyClicks is n NDJSON click records, page "/<i>", with bad put in place of
// record bad (1-based) when bad > 0.
func manyClicks(n, bad int, badLine string) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		if i == bad {
			b.WriteString(badLine + "\n")
			continue
		}
		fmt.Fprintf(&b, `{"page":"/%d","count":%d}`+"\n", i, i)
	}
	return b.String()
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

// TestIngest_ParseWindowsRefuseTheWholeBody: a record ClickHouse cannot read
// in a later window refuses the body, numbered from its first record, and
// nothing is published, records in earlier windows included.
func TestIngest_ParseWindowsRefuseTheWholeBody(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	body := manyClicks(9000, 8500, `{"page":"/bad","count":"many"}`)
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/x-ndjson", body)))
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	testutil.AssertJSONContains(t, w, http.StatusBadRequest, map[string]any{"code": codeCHRejected})
	assert.Contains(t, w.Body.String(), `"error":"record 8500: `)
	assert.Empty(t, pub.Messages)
}

// TestIngest_ParseSlotsFullIs503: with every parse slot taken, a body is
// refused before any parse with 503 and Retry-After, as other backpressure is,
// and is served once a slot frees.
func TestIngest_ParseSlotsFullIs503(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	h := newTestIngestHandler(t, testRegistry(t), pub)
	var held []func()
	for {
		release, ok := h.Types.TakeParseSlot()
		if !ok {
			break
		}
		held = append(held, release)
	}
	require.NotEmpty(t, held)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/home"})))
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	assert.Equal(t, retryAfterBusy, w.Header().Get("Retry-After"))
	testutil.AssertJSONContains(t, w, http.StatusServiceUnavailable, map[string]any{"error": "ingest validation is busy"})
	assert.Empty(t, pub.Messages)

	held[0]()
	w = httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/home"})))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	for _, release := range held[1:] {
		release()
	}
}

// TestIngest_NearCapBodyOnAMapTable: a body near the 16 MiB cap on a table with
// a Map column, which one parse over it declines (ClickHouse's parallel
// parsing cut at 10 MiB), is accepted record for record.
func TestIngest_NearCapBodyOnAMapTable(t *testing.T) {
	if testing.Short() {
		t.Skip("parses a 15 MiB body")
	}
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
