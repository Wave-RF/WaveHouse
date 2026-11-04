package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
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

// TestIngest_ParseSlotsWaitThenBusy: with every parse slot taken, a body
// waits a short while for one. Freed in time, it is served; not freed, it is
// refused before any parse with 503 and Retry-After, as other backpressure
// is; a client that goes away while it waits stops the wait.
func TestIngest_ParseSlotsWaitThenBusy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		freeAfter time.Duration // 0: the slots stay taken
		cancel    bool
		status    int
	}{
		{"freed within the wait", 20 * time.Millisecond, false, http.StatusOK},
		{"never freed", 0, false, http.StatusServiceUnavailable},
		{"client gone while waiting", 0, true, statusClientClosedRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, testRegistry(t), pub)
			var held []func()
			for {
				release, err := h.Types.TakeParseSlot(context.Background())
				if err != nil {
					require.ErrorIs(t, err, typelayer.ErrBusy)
					break
				}
				held = append(held, release)
			}
			require.NotEmpty(t, held)
			defer func() {
				for _, release := range held {
					release()
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch {
			case tt.freeAfter > 0:
				time.AfterFunc(tt.freeAfter, held[0])
				held = held[1:]
			case tt.cancel:
				time.AfterFunc(10*time.Millisecond, cancel)
			}
			started := time.Now()
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/home"}).WithContext(ctx)))
			require.Equal(t, tt.status, w.Code, w.Body.String())
			switch tt.status {
			case http.StatusOK:
				assert.Len(t, pub.Messages, 1)
			case http.StatusServiceUnavailable:
				assert.GreaterOrEqual(t, time.Since(started), 90*time.Millisecond, "it waited for a slot first")
				assert.Equal(t, retryAfterBusy, w.Header().Get("Retry-After"))
				testutil.AssertJSONContains(t, w, http.StatusServiceUnavailable, map[string]any{"error": "ingest validation is busy"})
				assert.Empty(t, pub.Messages)
			default:
				// A 499, not the 503 an expired wait gives: the cancel ended it.
				assert.Empty(t, pub.Messages)
			}
		})
	}
}
