package api

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetBodyBuffer_IsReset: a buffer taken from the pool never carries the
// previous request's bytes — the ingest body is read into it directly, so a
// stale prefix would silently prepend one caller's payload to another's.
func TestGetBodyBuffer_IsReset(t *testing.T) {
	buf := getBodyBuffer()
	buf.WriteString(`{"page":"/secret"}`)
	putBodyBuffer(buf)

	// Not guaranteed to be the same object (sync.Pool may drop it), so assert
	// the property that matters for every buffer the pool hands out.
	for range 8 {
		got := getBodyBuffer()
		assert.Zero(t, got.Len(), "a pooled buffer must come back empty")
		putBodyBuffer(got)
	}
}

// TestPutBodyBuffer_DropsOversized: the 16 MiB body cap means one large ingest
// could otherwise park 16 MiB of idle memory in the pool for the process
// lifetime. Anything past the retention cap is dropped instead.
func TestPutBodyBuffer_DropsOversized(t *testing.T) {
	big := new(bytes.Buffer)
	big.Grow(maxPooledBufferBytes + 1)
	require.Greater(t, big.Cap(), maxPooledBufferBytes)
	putBodyBuffer(big) // must not retain

	small := getBodyBuffer()
	assert.LessOrEqual(t, small.Cap(), maxPooledBufferBytes,
		"an oversized buffer must not come back out of the pool")
	putBodyBuffer(small)
}

// TestPutBodyBuffer_NilIsSafe: the handler's deferred return runs on every exit
// path, including ones that never took a buffer.
func TestPutBodyBuffer_NilIsSafe(t *testing.T) {
	assert.NotPanics(t, func() { putBodyBuffer(nil) })
}
