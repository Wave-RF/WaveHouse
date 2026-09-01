package api

import (
	"bytes"
	"sync"
)

// maxPooledBufferBytes caps the buffer a request may hand back to the pool. The
// inbound body cap is 16 MiB, so without this one large ingest would park
// 16 MiB of idle memory per pooled entry for the life of the process. 1 MiB
// covers the overwhelming majority of bodies; anything larger is left to the
// garbage collector rather than retained.
const maxPooledBufferBytes = 1 << 20

var bodyBufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// getBodyBuffer takes a reset buffer from the pool.
func getBodyBuffer() *bytes.Buffer {
	buf, _ := bodyBufferPool.Get().(*bytes.Buffer)
	if buf == nil {
		buf = new(bytes.Buffer)
	}
	buf.Reset()
	return buf
}

// putBodyBuffer returns buf to the pool unless it outgrew
// maxPooledBufferBytes. The caller must be done reading records out of it: the
// record readers decode into freshly allocated values (encoding/json copies
// every string, json.Number included), so "done" means the last Next has
// returned — nothing a handed-back record holds points into these bytes.
func putBodyBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > maxPooledBufferBytes {
		return
	}
	bodyBufferPool.Put(buf)
}
