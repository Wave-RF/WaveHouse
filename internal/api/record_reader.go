package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
)

const (
	// maxNDJSONLineBytes caps a single NDJSON record so one pathological line
	// can't force an unbounded read buffer. 10 MiB is far above any realistic
	// flat ingest record; a line larger than this aborts the whole request.
	maxNDJSONLineBytes = 10 << 20 // 10 MiB

	// maxSniffBytes bounds how far the format sniffer peeks for the first
	// non-whitespace byte. Far beyond any reasonable amount of leading
	// whitespace; a body that is only whitespace within this window is treated
	// as empty.
	maxSniffBytes = 512
)

// recordReader yields ingest records one at a time from a request body. Each
// concrete reader covers one wire format (single JSON object, JSON array,
// NDJSON, and — later — CSV), so the handler stays format-agnostic and new
// formats / transports (streaming uploads) slot in behind this one interface.
//
// Next returns io.EOF at the clean end of the body. A *recordSyntaxError is a
// recoverable per-record decode failure — the framing let the reader resync, so
// the handler records it and continues. Any other non-EOF error is fatal to the
// request.
type recordReader interface {
	Next() (map[string]any, error)
}

// recordSyntaxError marks a per-record decode failure the reader recovered from
// (the framing let it skip to the next record). The batch handler turns it into
// a recordResult error; it carries no HTTP status because the decode layer sits
// below the validation/permission layer that owns status codes.
type recordSyntaxError struct{ msg string }

func (e *recordSyntaxError) Error() string { return e.msg }

// errEmptyBody is returned by newRecordReader when the body has no content (no
// non-whitespace byte within the sniff window). The handler maps it to a 400.
var errEmptyBody = errors.New("empty body")

// errUnterminatedArray marks a JSON array body that ended before its closing
// ']' (a truncated / cut-off upload). It is deliberately NOT io.EOF so the
// batch loop fails the whole request (400) instead of treating the records that
// did arrive as a complete, successful batch.
var errUnterminatedArray = errors.New("unterminated json array")

// objectReader decodes exactly one flat JSON object — the single-object ingest
// path. A second Next returns io.EOF. Trailing bytes after the first object are
// ignored (matching the historical single-object behavior), so the response
// shape never depends on what follows the object.
type objectReader struct {
	dec  *json.Decoder
	done bool
}

func (o *objectReader) Next() (map[string]any, error) {
	if o.done {
		return nil, io.EOF
	}
	o.done = true
	var m map[string]any
	if err := o.dec.Decode(&m); err != nil {
		return nil, err // fatal: handler maps MaxBytes → 413, else 400 invalid json
	}
	return m, nil
}

// arrayReader streams the elements of a top-level JSON array. A wrong-typed
// element (a scalar/array where an object was expected) yields a
// *json.UnmarshalTypeError, which leaves the decoder in sync — recoverable, so
// it becomes a per-record error and iteration continues. A *json.SyntaxError
// desyncs the decoder and is returned as fatal.
type arrayReader struct {
	dec     *json.Decoder
	started bool
	done    bool
}

func (a *arrayReader) Next() (map[string]any, error) {
	if a.done {
		return nil, io.EOF
	}
	if !a.started {
		if _, err := a.dec.Token(); err != nil { // consume the opening '['
			a.done = true
			return nil, err
		}
		a.started = true
	}
	if !a.dec.More() {
		a.done = true
		// More() reports false not only at a clean ']', but also on a read error
		// (the body cap tripping *between* elements) and on a truncated array
		// (EOF before ']'), swallowing both — which would let dropped records
		// masquerade as a complete partial-200 insert. Read the closing token to
		// tell the cases apart: only a ']' ends the batch. A read error
		// (MaxBytesError → 413, else 400) and a missing/non-']' close (the upload
		// was cut off → 400, via errUnterminatedArray which is NOT io.EOF) both
		// fail the whole request.
		tok, err := a.dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errUnterminatedArray
			}
			return nil, err
		}
		if d, ok := tok.(json.Delim); !ok || d != ']' {
			return nil, errUnterminatedArray
		}
		return nil, io.EOF
	}
	var m map[string]any
	if err := a.dec.Decode(&m); err != nil {
		if _, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
			// Decoder stayed in sync past the bad element — recoverable.
			return nil, &recordSyntaxError{"record must be a JSON object"}
		}
		// Syntax/read error: the decoder is desynced, the rest of the array is
		// unrecoverable. Don't try to read the closing ']'.
		a.done = true
		if errors.Is(err, io.EOF) {
			// More() said an element followed (e.g. after a trailing comma) but
			// the stream ended — a truncated array, not a clean close. Map to a
			// fatal error rather than the io.EOF the batch loop treats as "done".
			return nil, errUnterminatedArray
		}
		return nil, err
	}
	return m, nil
}

// lineReader yields one record per non-blank line of an NDJSON body. It recovers
// from both type and syntax errors per line (the newline reframes the next
// record), so a single malformed line never blocks the rest of the batch.
type lineReader struct {
	sc *bufio.Scanner
}

func (l *lineReader) Next() (map[string]any, error) {
	for l.sc.Scan() {
		line := bytes.TrimSpace(l.sc.Bytes())
		if len(line) == 0 {
			continue // skip blank lines between records
		}
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			return nil, &recordSyntaxError{"invalid json"}
		}
		return m, nil
	}
	if err := l.sc.Err(); err != nil {
		// A line exceeding maxNDJSONLineBytes (bufio.ErrTooLong) or a body read
		// error (possibly the MaxBytes cap) — the scanner can't resume, so fail
		// the request.
		return nil, err
	}
	return nil, io.EOF
}

// newRecordReader picks a reader from the Content-Type and a peek at the body.
// Content-Type is a hint, not a requirement: the first non-whitespace byte wins
// for the JSON family ('[' → array, else → single object), and an explicit
// application/x-ndjson type selects line-framing only when the body doesn't
// start with '[' (so a mislabeled JSON array still works). batch is false only
// for the single-object path; true for array/NDJSON. This is what makes ingest
// forgiving: a JSON array, a single object, or NDJSON all work regardless of the
// header. The caller is expected to have already bounded body via
// http.MaxBytesReader.
func newRecordReader(contentType string, body io.Reader) (rr recordReader, batch bool, err error) {
	br := bufio.NewReader(body)
	first, perr := peekFirstNonSpace(br)
	if perr != nil {
		return nil, false, errEmptyBody
	}

	// Explicit NDJSON wins for line-framing, unless the body is actually an
	// array. (CSV plugs in here later: isCSVContentType(contentType) && first != '['.)
	if isNDJSONContentType(contentType) && first != '[' {
		sc := bufio.NewScanner(br)
		sc.Buffer(make([]byte, 0, 64*1024), maxNDJSONLineBytes)
		return &lineReader{sc: sc}, true, nil
	}

	dec := json.NewDecoder(br)
	dec.UseNumber()
	if first == '[' {
		return &arrayReader{dec: dec}, true, nil
	}
	return &objectReader{dec: dec}, false, nil
}

// peekFirstNonSpace returns the first non-whitespace byte of the body without
// consuming it (the chosen decoder still sees the whole body). It returns
// errEmptyBody when there is no such byte within the sniff window.
func peekFirstNonSpace(br *bufio.Reader) (byte, error) {
	buf, _ := br.Peek(maxSniffBytes) // short read at EOF is fine; we only need the first token
	for _, c := range buf {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c, nil
		}
	}
	return 0, errEmptyBody
}

// emptyBodyMessage tailors the empty-body 400 message to the declared format so
// an NDJSON caller still gets the familiar "empty ndjson body".
func emptyBodyMessage(contentType string) string {
	if isNDJSONContentType(contentType) {
		return "empty ndjson body"
	}
	return "empty body"
}

// isNDJSONContentType reports whether the request Content-Type explicitly
// selects NDJSON line-framing. It matches application/x-ndjson and common
// synonyms (application/ndjson, application/jsonl, application/jsonlines),
// ignoring parameters such as "; charset=utf-8". Anything else — including a
// missing type or application/json — leaves the format to the body sniffer.
func isNDJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	switch mediaType {
	case "application/x-ndjson", "application/ndjson", "application/jsonl", "application/jsonlines":
		return true
	default:
		return false
	}
}
