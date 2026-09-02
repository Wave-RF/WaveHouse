package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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

// errUnsupportedContentType is returned when the request declares no
// Content-Type, one whose media type is not in the accepted list, or several
// that disagree. The handler maps it to a 415.
var errUnsupportedContentType = errors.New("unsupported content type")

// IngestFormat is the wire format of an ingest request body. It comes from the
// request's declared Content-Type and nothing else: the body never overrides
// what the client said it sent, so a caller can always tell how their bytes
// will be read without knowing what the first one happens to be.
type IngestFormat int

const (
	// FormatJSON is the application/json family: one flat object, or a
	// top-level array of them. Which of the two is the body's own business —
	// the first non-whitespace byte picks it — because both are the same
	// format, differing only in arity.
	FormatJSON IngestFormat = iota
	// FormatNDJSON is newline-delimited JSON: one flat object per line. A line
	// that is not a JSON object is a per-record error, never a reason to
	// re-read the body as something else.
	FormatNDJSON
	// FormatCSV plugs in here once ingest reads CSV: add the media types to
	// ingestFormat's switch and the reader to newRecordReader's.
)

// String renders a format for error messages and logs.
func (f IngestFormat) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatNDJSON:
		return "ndjson"
	default:
		return "unknown"
	}
}

// supportedContentTypes lists, in the order the 415 message names them, every
// media type ingest reads. The first of each family is the canonical spelling.
var supportedContentTypes = []string{
	"application/json",
	"application/x-ndjson",
	"application/ndjson",
	"application/jsonl",
	"application/jsonlines",
}

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

// ingestFormat resolves a declared Content-Type to the format ingest will read
// the body as. A missing, empty, or unrecognized type is errUnsupportedContentType
// (a 415): the format is the client's declaration, so there is nothing to fall
// back to.
//
// One header value may carry SEVERAL declarations: an intermediary that merges
// duplicate Content-Type header lines joins them with a comma. That is the same
// situation as two separate header lines, which the handler resolves and
// compares, so it is judged by the same rule here — resolve every part and
// require them to agree. Judging it any other way would make the outcome depend
// on which spelling an intermediary the caller does not control happened to
// emit: `application/json, application/json` says exactly what two identical
// header lines say.
//
// Agreement is on (format, acceptedness), not on text, so `application/x-ndjson`
// and `application/ndjson; charset=utf-8` agree, while a supported and an
// unsupported declaration do not. A comma inside a quoted parameter value is
// split like any other, so such a header is refused whenever the parts it
// produces disagree — `application/json; foo="x,y"` does. That is usual but not
// universal: `application/json; foo="a,application/json;q=1"` splits into parts
// that agree and is accepted. Not parsing quoted strings is the deliberate
// price, and it is bounded — acceptance requires every part to match the
// LEADING declaration, so the worst case is an over-reject, never a body framed
// as something the caller did not declare.
func ingestFormat(ct string) (IngestFormat, error) {
	parts := strings.Split(ct, ",")
	first, firstErr := ingestFormatOne(parts[0])
	for _, p := range parts[1:] {
		f, err := ingestFormatOne(p)
		if f != first || (err == nil) != (firstErr == nil) {
			return FormatJSON, errUnsupportedContentType
		}
	}
	return first, firstErr
}

// ingestFormatOne resolves ONE declaration. The media type is everything before
// the first ";", trimmed and lowercased; parameters are ignored entirely rather
// than parsed, because the rule is "parameters do not affect the format" and
// parsing them only creates ways to refuse a request that names a type this
// endpoint reads. mime.ParseMediaType refuses a parameter with no value
// ("; charset"), an empty one (";;"), an unterminated quoted value, and a
// duplicate name ("; charset=a; charset=b") — every one of which this endpoint
// accepted before the header became authoritative, and none of which changes
// what the body is.
func ingestFormatOne(ct string) (IngestFormat, error) {
	base, _, _ := strings.Cut(ct, ";")
	switch strings.ToLower(strings.TrimSpace(base)) {
	case "application/json":
		return FormatJSON, nil
	case "application/x-ndjson", "application/ndjson", "application/jsonl", "application/jsonlines":
		return FormatNDJSON, nil
	default:
		return FormatJSON, errUnsupportedContentType
	}
}

// newRecordReader picks a reader from the declared Content-Type, using a peek at
// the body only to choose arity within the JSON family ('[' → array, else →
// single object). The header is authoritative: an NDJSON body is read as NDJSON
// whatever its first byte, so a line that isn't a JSON object fails as a
// per-record error rather than silently re-framing the whole request. batch is
// false only for the single-object path; true for array/NDJSON. format is
// meaningful even when err is errEmptyBody — the declaration resolves before the
// body is read at all — and is FormatJSON alongside errUnsupportedContentType,
// where nothing was declared to resolve. The caller is expected to have already
// bounded body via http.MaxBytesReader.
func newRecordReader(contentType string, body io.Reader) (rr recordReader, format IngestFormat, batch bool, err error) {
	format, err = ingestFormat(contentType)
	if err != nil {
		return nil, format, false, err
	}

	br := bufio.NewReader(body)
	first, perr := peekFirstNonSpace(br)
	if perr != nil {
		return nil, format, false, errEmptyBody
	}

	if format == FormatNDJSON {
		sc := bufio.NewScanner(br)
		sc.Buffer(make([]byte, 0, 64*1024), maxNDJSONLineBytes)
		return &lineReader{sc: sc}, format, true, nil
	}

	dec := json.NewDecoder(br)
	dec.UseNumber()
	if first == '[' {
		return &arrayReader{dec: dec}, format, true, nil
	}
	return &objectReader{dec: dec}, format, false, nil
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
func emptyBodyMessage(format IngestFormat) string {
	if format == FormatNDJSON {
		return "empty ndjson body"
	}
	return "empty body"
}

// unsupportedContentTypeMessage is the 415 body: it says what was declared (or
// that nothing was) and lists every media type ingest reads, so a caller can fix
// the request from the response alone.
func unsupportedContentTypeMessage(ct string) string {
	declared := "no Content-Type"
	if strings.TrimSpace(ct) != "" {
		declared = fmt.Sprintf("Content-Type %q", ct)
	}
	return fmt.Sprintf("%s: ingest requires one of %s", declared, strings.Join(supportedContentTypes, ", "))
}
