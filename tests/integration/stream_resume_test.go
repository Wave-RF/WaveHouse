//go:build integration

package tests

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sseFrame is one row frame of a stream: its id line, when it has one, and
// its data.
type sseFrame struct{ id, data string }

// openSSE opens GET /v1/stream?query on baseURL with headers, and returns its
// row frames as they arrive, once the stream is open; schema frames and
// comments are skipped. drop closes the connection, as a viewer going away
// does.
func openSSE(t *testing.T, baseURL, query string, headers map[string]string) (frames <-chan sseFrame, drop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/stream?"+query, nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed by the reader below, on cancel
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(chan sseFrame, 256)
	connected := make(chan struct{})
	go func() {
		defer func() { _ = resp.Body.Close() }()
		defer close(out)
		var (
			f      sseFrame
			schema bool
		)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == ": connected":
				close(connected)
			case strings.HasPrefix(line, "event: "):
				schema = true
			case strings.HasPrefix(line, "id: "):
				f.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				f.data += strings.TrimPrefix(line, "data: ")
			case line == "":
				if f.data != "" && !schema {
					out <- f
				}
				f, schema = sseFrame{}, false
			}
		}
	}()
	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream never opened")
	}
	return out, cancel
}

// awaitFrame reads frames until one's data contains want, and returns it with
// every frame read on the way.
func awaitFrame(t *testing.T, frames <-chan sseFrame, want string) (sseFrame, []sseFrame) {
	t.Helper()
	var seen []sseFrame
	deadline := time.After(30 * time.Second)
	for {
		select {
		case f, ok := <-frames:
			require.True(t, ok, "the stream ended before a row with %q; read %v", want, seen)
			seen = append(seen, f)
			if strings.Contains(f.data, want) {
				return f, seen
			}
		case <-deadline:
			t.Fatalf("no row with %q within 30s; read %v", want, seen)
		}
	}
}

// TestStream_ADroppedViewerResumesWithoutAHole drives a viewer through the
// real HTTP stack on the embedded broker: it reads a row live, drops, and
// comes back with the id of the last row it was sent. It is sent every row
// accepted while it was away, first while they still wait in the queue for
// their batch and again after the worker has written them, and none of the
// replayed frames moves its resume point, so a drop in the middle of a replay
// starts that replay over rather than skipping its tail.
func TestStream_ADroppedViewerResumesWithoutAHole(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	table := createTable(t, "id String", "ORDER BY id")
	query := "table=" + url.QueryEscape(table)
	post := func(id string) {
		t.Helper()
		resp, err := httpPost(ctx, e.baseURL+"/v1/ingest?"+query, "application/json", strings.NewReader(fmt.Sprintf(`{"id":%q}`, id)))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	written := func(id string) bool {
		var n uint64
		err := e.chConn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE id = '%s'", table, id)).Scan(&n)
		return err == nil && n == 1
	}

	// A stream says ": connected" before it is registered for live rows
	// (#784), so rows are sent until one arrives live.
	live, drop := openSSE(t, e.baseURL, query, nil)
	var last sseFrame
	require.Eventually(t, func() bool {
		post("seen")
		select {
		case last = <-live:
			return true
		case <-time.After(500 * time.Millisecond):
			return false
		}
	}, 30*time.Second, time.Millisecond, "no row arrived live")
	require.NotEmpty(t, last.id, "a live row carries the id a viewer resumes from")
	drop()

	// Accepted while the viewer is away.
	for _, id := range []string{"missed-1", "missed-2", "missed-3"} {
		post(id)
	}
	resume := map[string]string{"Last-Event-ID": last.id}
	ids := func(frames []sseFrame) string {
		var b strings.Builder
		for _, f := range frames {
			b.WriteString(f.data)
		}
		return b.String()
	}

	// It comes back at once: the rows are in the queue, waiting for their
	// batch, or already written. It drops again after the first of them.
	back, drop := openSSE(t, e.baseURL, query, resume)
	first, _ := awaitFrame(t, back, "missed-1")
	assert.Empty(t, first.id, "a replayed row moves no resume point")
	drop()

	// So it resumes from the same id, and is sent all three.
	back, drop = openSSE(t, e.baseURL, query, resume)
	_, got := awaitFrame(t, back, "missed-3")
	for _, id := range []string{"missed-1", "missed-2", "missed-3"} {
		assert.Contains(t, ids(got), id)
	}
	drop()

	// And again once the worker has written them: they are replayed from the
	// history now, for the tenant's window from the write.
	require.Eventually(t, func() bool { return written("missed-3") }, 30*time.Second, 250*time.Millisecond, "the rows never reached ClickHouse")
	back, _ = openSSE(t, e.baseURL, query, resume)
	_, got = awaitFrame(t, back, "missed-3")
	for _, id := range []string{"missed-1", "missed-2", "missed-3"} {
		assert.Contains(t, ids(got), id)
	}
	for _, f := range got {
		assert.Empty(t, f.id, "a replayed row moves no resume point: %s", f.data)
	}

	// The stream then carries on live, and a live row moves the resume point.
	require.Eventually(t, func() bool {
		post("after")
		for {
			select {
			case f := <-back:
				if strings.Contains(f.data, "after") && f.id != "" {
					return true
				}
			case <-time.After(500 * time.Millisecond):
				return false
			}
		}
	}, 30*time.Second, time.Millisecond, "no live row after the replay")
}
