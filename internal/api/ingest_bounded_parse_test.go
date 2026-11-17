package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
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

// TestIngest_ParseSlotWaitDeadline: a request whose deadline passes while it
// waits for a parse slot is the server's 500, as a deadline during the dedupe
// reserve is, logged at DEBUG rather than paged at ERROR. Not t.Parallel: it
// captures the process-wide default logger.
func TestIngest_ParseSlotWaitDeadline(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelDebug)
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
	defer func() {
		for _, release := range held {
			release()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/home"}).WithContext(ctx)))
	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	assert.Contains(t, buf.String(), `"level":"DEBUG","msg":"record validation stopped: request context ended"`)
	assert.NotContains(t, buf.String(), `"level":"ERROR"`)
	assert.Empty(t, pub.Messages)
}

// TestIngest_ParseTimeLimit: a body still being read past its tenant's time
// limit stops at the next parse window and is a 422 naming the limit, with no
// Retry-After and nothing published, logged at WARN; one read within it is
// served. Not t.Parallel: it captures the process-wide default logger.
func TestIngest_ParseTimeLimit(t *testing.T) {
	tests := []struct {
		name   string
		limit  time.Duration
		status int
	}{
		{"past the limit", time.Nanosecond, http.StatusUnprocessableEntity},
		{"within the limit", time.Minute, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := logtest.Capture(t, slog.LevelWarn)
			pub := &testutil.MockPublisher{}
			h := newTestIngestHandler(t, testRegistry(t), pub)
			h.ParseTimeout = func(*settings.Store) time.Duration { return tt.limit }
			// More records than one default window holds, so the limit is
			// checked between two.
			body := manyClicks(9000, 0, "")
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(rawIngestRequest(t, "clicks", "application/x-ndjson", body)))
			require.Equal(t, tt.status, w.Code, w.Body.String())
			if tt.status == http.StatusOK {
				assert.Len(t, pub.Messages, 9000)
				return
			}
			testutil.AssertJSONResponse(t, w, http.StatusUnprocessableEntity, map[string]any{"error": "ingest validation took longer than the time limit (0.000000001s)"})
			assert.Empty(t, w.Header().Get("Retry-After"))
			assert.Empty(t, pub.Messages)
			assert.Contains(t, buf.String(), `"level":"WARN","msg":"ingest refused: validating the body took longer than its time limit"`)
			assert.NotContains(t, buf.String(), `"level":"ERROR"`)
		})
	}
}

func TestTimeLimitAbort_NamesTheLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		limit time.Duration
		want  string
	}{
		{30 * time.Second, "(30s)"},
		{5 * time.Minute, "(300s)"},
		{1500 * time.Millisecond, "(1.5s)"},
	}
	for _, tt := range tests {
		abort := timeLimitAbort(tt.limit)
		assert.Equal(t, http.StatusUnprocessableEntity, abort.Status)
		assert.Equal(t, "ingest validation took longer than the time limit "+tt.want, abort.Message)
		assert.Empty(t, abort.RetryAfter)
	}
}
