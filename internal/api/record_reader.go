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
// that AGREE on being unreadable. Declarations that disagree are
// errConflictingContentType. The handler maps both to a 415.
var errUnsupportedContentType = errors.New("unsupported content type")

// errConflictingContentType is returned when the request declares the type more
// than once and the declarations do not agree. Distinct from
// errUnsupportedContentType so the 415 can say which failure it was: the
// unsupported wording lists the accepted types, which reads as nonsense when the
// caller has just declared two of them.
var errConflictingContentType = errors.New("conflicting content type")

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
	// ingestFormatOne's switch and the reader to newRecordReader's.
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
// acceptedContentTypes maps every media type ingest reads to the format it
// selects, in the order the 415 body advertises them. It is the SINGLE source:
// supportedContentTypes is derived from it, so the advertised list and the
// accepted set cannot drift apart in either direction.
//
// They could before. Adding a case to the resolver without adding it here left
// the whole suite green while ingest accepted a type the 415 message, api.md and
// architecture.md all failed to name — and no test can close that direction by
// enumeration, because the complement is unbounded. One table closes it by
// construction.
var acceptedContentTypes = []struct {
	mediaType string
	format    IngestFormat
}{
	{"application/json", FormatJSON},
	{"application/x-ndjson", FormatNDJSON},
	{"application/ndjson", FormatNDJSON},
	{"application/jsonl", FormatNDJSON},
	{"application/jsonlines", FormatNDJSON},
}

// supportedContentTypes is what the 415 body lists and the docs quote, derived
// from acceptedContentTypes so it is never a second place to edit.
var supportedContentTypes = func() []string {
	out := make([]string, len(acceptedContentTypes))
	for i, a := range acceptedContentTypes {
		out[i] = a.mediaType
	}
	return out
}()

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

// declaredContentTypes flattens the Content-Type header set into one entry per
// declaration. A caller may declare the type more than once: as repeated header
// LINES, and an intermediary that merges duplicates joins them into one line
// with a comma. Both spellings mean the same thing, so both are flattened here
// and judged by ONE predicate — keeping the rule in two places is exactly how
// the joined and repeated paths came to disagree before.
//
// Empty declarations are dropped. A bare `Content-Type:` line, or the trailing
// element of `application/json,`, declares nothing rather than contradicting
// anything, and RFC 9110 §5.6.1.2 says to ignore empty members of a list. If
// nothing survives, the request declared no type at all.
func declaredContentTypes(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

// resolveContentType resolves the whole declared header set to the format ingest
// will read the body as. Declarations must agree on (format, acceptedness):
// `application/x-ndjson` and `application/ndjson; charset=utf-8` agree, a
// supported and an unsupported declaration do not. Disagreement is
// errConflictingContentType; nothing readable at all is errUnsupportedContentType.
//
// Agreement is what makes accepting safe — the choice of which declaration to
// honor stops mattering. That is also why a comma inside a quoted parameter
// value is split like any other: such a header is refused whenever the parts it
// produces disagree — `application/json; foo="x,y"` does, while
// `application/json; foo="a,application/json;q=1"` splits into parts that agree
// and is accepted. Not parsing quoted strings is the deliberate price, and it is
// bounded: acceptance requires every part to match the LEADING declaration, so
// the worst case is an over-reject, never a body framed as something the caller
// did not declare.
func resolveContentType(values []string) (IngestFormat, error) {
	parts := declaredContentTypes(values)
	if len(parts) == 0 {
		return FormatJSON, errUnsupportedContentType
	}
	first, firstErr := ingestFormatOne(parts[0])
	for _, p := range parts[1:] {
		f, err := ingestFormatOne(p)
		if f != first || (err == nil) != (firstErr == nil) {
			return FormatJSON, errConflictingContentType
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
	mediaType := strings.ToLower(strings.TrimSpace(base))
	for _, a := range acceptedContentTypes {
		if a.mediaType == mediaType {
			return a.format, nil
		}
	}
	return FormatJSON, errUnsupportedContentType
}

// newRecordReader picks a reader from the ALREADY-RESOLVED format, using a peek
// at the body only to choose arity within the JSON family ('[' → array, else →
// single object). The header is authoritative: an NDJSON body is read as NDJSON
// whatever its first byte, so a line that isn't a JSON object fails as a
// per-record error rather than silently re-framing the whole request. batch is
// false only for the single-object path; true for array/NDJSON.
//
// It takes the format rather than a Content-Type on purpose. Resolving here as
// well as in the handler meant the two could disagree — a leading empty
// declaration resolved fine for the header SET and failed for the first line
// alone, turning a good request into an empty-body 400. The only error this can
// return now is errEmptyBody. The caller is expected to have already bounded
// body via http.MaxBytesReader.
func newRecordReader(format IngestFormat, body io.Reader) (rr recordReader, batch bool, err error) {
	br := bufio.NewReader(body)
	first, perr := peekFirstNonSpace(br)
	if perr != nil {
		return nil, false, errEmptyBody
	}

	if format == FormatNDJSON {
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
func emptyBodyMessage(format IngestFormat) string {
	if format == FormatNDJSON {
		return "empty ndjson body"
	}
	return "empty body"
}

// contentTypeMessage is the 415 body. It says what was declared — all of it,
// across header lines and joined values — and lists every media type ingest
// reads, so a caller can fix the request from the response alone.
//
// The disagreement case gets its own wording. Routed through the unsupported
// text, a joined `application/json, application/x-ndjson` told the caller
// "ingest requires one of application/json, application/x-ndjson, …" — listing
// as acceptable the two types they had just declared, and explaining nothing.
// The spelling of the request no longer selects the message either: repeated
// lines and a joined value describing the same disagreement now read alike.
func contentTypeMessage(values []string, conflicting bool) string {
	decls := declaredContentTypes(values)
	list := strings.Join(supportedContentTypes, ", ")
	accepted := "ingest requires one of " + list

	quoted := make([]string, len(decls))
	for i, d := range decls {
		quoted[i] = fmt.Sprintf("%q", d)
	}
	joined := strings.Join(quoted, ", ")

	switch {
	case conflicting:
		return fmt.Sprintf("conflicting Content-Type declarations %s: ingest reads one format per request, and requires one of %s", joined, list)
	case len(decls) == 0:
		return "no Content-Type: " + accepted
	default:
		return fmt.Sprintf("Content-Type %s: %s", joined, accepted)
	}
}
