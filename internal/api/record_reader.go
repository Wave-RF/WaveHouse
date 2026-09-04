package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"slices"
	"strings"
)

const (
	// maxNDJSONLineBytes caps a single NDJSON record so one pathological line
	// can't force an unbounded read buffer. 10 MiB is far above any realistic
	// flat ingest record; a line larger than this aborts the whole request.
	maxNDJSONLineBytes = 10 << 20 // 10 MiB

	// maxSniffBytes bounds how far the arity peek looks for the first
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
	// acceptedContentTypes, which advertises them in the 415 automatically, and
	// the reader to newRecordReader. Do NOT add them in ingestFormatOne — it
	// resolves by scanning that one table, and a second list is the drift this
	// arrangement exists to prevent.
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

// acceptedContentTypes maps every media type ingest reads to the format it
// selects, in the order the 415 body advertises them. The first entry of each
// family is the canonical spelling — the TS SDK sends those two. It is the
// SINGLE source: supportedContentTypes is derived from it, so the advertised
// list and the accepted set cannot drift apart in either direction.
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

// resolveContentType resolves the Content-Type header set to the format ingest
// reads the body as. Content-Type is a singleton field (RFC 9110 §8.3) and §5.3
// forbids repeating it, so a duplicate is malformed however it is spelled. §8.3
// warns that resolving the resulting pseudo-list by "using the last syntactically
// valid member" causes "interoperability and security issues", so we take no
// member: a value carrying a comma is refused in ingestFormatOne unless the value
// as a whole parses as one media type, and repeated LINES must agree on (format,
// acceptedness) — `application/x-ndjson` and `application/ndjson; charset=utf-8`
// do. Disagreement is errConflictingContentType.
// It also returns the index of the declaration that disagreed, or -1 when none
// did. Callers hand that straight to echoSafe as the pin, so the 415 body and
// the WARN log cannot name different declarations — the invariant is structural
// rather than three call sites independently computing the same answer and
// being trusted to agree. They did not agree once already.
func resolveContentType(values []string) (IngestFormat, int, error) {
	if len(values) == 0 {
		return FormatJSON, -1, errUnsupportedContentType
	}
	if pin := disagreeingIndex(values); pin >= 0 {
		return FormatJSON, pin, errConflictingContentType
	}
	f, err := ingestFormatOne(values[0])
	return f, -1, err
}

// ingestFormatOne resolves ONE header line, parsed per RFC 9110 §8.3. Only the
// media type decides the format; no malformed parameter costs the request.
//
// That rule needs two steps, because Go splits parse failures in a way the rule
// does not. ErrInvalidMediaParameter leaves the media type parsed and returned,
// so tolerating it is enough. A duplicate parameter name does not — it returns
// no media type — so without the re-parse below the tolerance would be drawn by
// Go's error taxonomy rather than by the rule, and `; charset=a; charset=b`
// would be refused while `; charset` and `;;` were accepted.
//
// The exception is a comma. On a line that did not parse cleanly it may be a
// second declaration joined on — `application/json; charset=utf-8,
// application/x-ndjson` yields "application/json" — and honoring the first
// member there reads an NDJSON body as one object, dropping every record past
// it behind a 200. The error cannot distinguish that from a comma inside data,
// so such a line is refused rather than guessed at (#563).
func ingestFormatOne(v string) (IngestFormat, error) {
	mediaType, _, err := mime.ParseMediaType(v)
	if err != nil {
		if strings.ContainsRune(v, ',') {
			return FormatJSON, errUnsupportedContentType
		}
		if !errors.Is(err, mime.ErrInvalidMediaParameter) {
			base, _, baseErr := mime.ParseMediaType(mediaTypePrefix(v))
			if baseErr != nil {
				return FormatJSON, errUnsupportedContentType
			}
			mediaType = base
		}
	}
	for _, a := range acceptedContentTypes {
		if a.mediaType == mediaType {
			return a.format, nil
		}
	}
	return FormatJSON, errUnsupportedContentType
}

// mediaTypePrefix is everything before the first ";" — the media type without
// its parameters. Only ingestFormatOne's re-parse uses it, and only on a line
// with no comma, so it cannot resurrect a joined declaration.
func mediaTypePrefix(v string) string {
	base, _, _ := strings.Cut(v, ";")
	return base
}

// newRecordReader picks a reader from the ALREADY-RESOLVED format, using a peek
// at the body only to choose arity within the JSON family ('[' → array, else →
// single object). The header is authoritative: an NDJSON body is read as NDJSON
// whatever its first byte, so a line that isn't a JSON object fails as a
// per-record error rather than silently re-framing the whole request. batch is
// false only for the single-object path; true for array/NDJSON.
//
// It takes the format rather than a Content-Type on purpose: resolving here as
// well as in the handler would put one rule in two places, which is how the
// joined and repeated paths came to disagree before. The only error this can
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

// Bounds on what a caller-supplied Content-Type may cost us when echoed back.
// The 415 is decided BEFORE the body is read, so a request needs no body — and,
// under the shipped compose policy (default_role: public with insert), no
// credentials — to provoke one.
//
// BOTH dimensions are caller-controlled and both must be bounded. values is
// r.Header.Values("Content-Type"), so a caller picks the length of each
// declaration AND how many there are. Bounding only the length left the
// amplification intact: 7200 lines of 128 bytes fits under Go's default 1 MiB
// MaxHeaderBytes and still bought a 4.65 MB response. Each non-UTF-8 byte costs
// 5 output bytes — %q renders \xNN, then JSON escapes the backslash. Over
// HTTP/2 it is worse than that ratio suggests, since HPACK indexes a repeated
// header line down to about one byte on the wire.
//
// 128 bytes is far longer than any real media type plus parameters, so a caller
// fixing a genuine mistake still sees what they sent. FOUR is the number that
// makes the message lossy, so it is the one that needs care: the declarations
// retained are the first four DISTINCT ones, because keeping the first four
// verbatim let a conflict hide behind them: four copies of application/json
// followed by one application/x-ndjson named only the agreeing type and buried
// the declaration that caused the refusal, which
// is precisely what the conflicting wording exists to avoid.
const (
	maxEchoedContentType  = 128 // per declaration
	maxEchoedDeclarations = 4   // how many DISTINCT declarations are named
)

// echoSafe bounds a declaration set for echoing to the caller or the log, in
// both dimensions. It keeps the first maxEchoedDeclarations DISTINCT values,
// and always keeps values[pin] if pin is in range — pass -1 for none. The
// result is O(1) in size regardless of the request.
//
// The pin exists because distinctness is not enough. Deduping by raw header
// line lets four distinct but AGREEING spellings — `application/json` beside
// `application/json; charset=utf-8` — fill every slot and bury the declaration
// that actually disagreed, which is the exact failure the conflicting wording
// exists to prevent, and the shape a header-duplicating proxy produces.
func echoSafe(values []string, pin int) []string {
	kept := make([]string, 0, maxEchoedDeclarations)
	for _, v := range values {
		if len(kept) == maxEchoedDeclarations {
			break
		}
		if !slices.Contains(kept, v) {
			kept = append(kept, v)
		}
	}
	// Guarantee the pinned declaration is named, giving up the last slot for it.
	if pin >= 0 && pin < len(values) && !slices.Contains(kept, values[pin]) {
		if len(kept) == maxEchoedDeclarations {
			kept = kept[:maxEchoedDeclarations-1]
		}
		kept = append(kept, values[pin])
	}
	out := make([]string, 0, len(kept)+1)
	for _, v := range kept {
		if len(v) > maxEchoedContentType {
			v = v[:maxEchoedContentType] + "…(truncated)"
		}
		out = append(out, v)
	}
	if rest := len(values) - len(kept); rest > 0 {
		out = append(out, fmt.Sprintf("…and %d more", rest))
	}
	return out
}

// disagreeingIndex returns the index of the first declaration that resolves
// differently from the first, or -1 when they all agree. Same predicate as
// resolveContentType's, so the declaration this names is the one that caused
// the refusal.
func disagreeingIndex(values []string) int {
	if len(values) == 0 {
		return -1
	}
	first, firstErr := ingestFormatOne(values[0])
	for i, v := range values[1:] {
		f, err := ingestFormatOne(v)
		if f != first || (err == nil) != (firstErr == nil) {
			return i + 1
		}
	}
	return -1
}

// contentTypeMessage is the 415 body. It says what was declared and lists every
// media type ingest reads, so a caller can fix the request from the response
// alone. The declarations are bounded by echoSafe — the first four DISTINCT
// ones, each capped, then "…and N more" — so a caller sending thousands cannot
// size the response, while a caller with a genuine conflict still sees the
// declaration that caused it.
//
// Disagreement between repeated LINES gets its own wording, because routing it
// through the unsupported text would tell a caller who declared both
// application/json and application/x-ndjson that ingest "requires one of
// application/json, application/x-ndjson, …" — listing as acceptable the two
// types they just declared, and explaining nothing.
//
// A comma-bearing value that does NOT parse, and is the request's only header
// line, does not reach that wording: the agreement loop never runs and it gets
// the unsupported text quoting the line whole. That is the honest report there —
// nothing resolved, so nothing disagreed. Alongside another line it can still
// come out as a disagreement, which is equally honest. api.md buckets the
// single-line case under "does not parse".
func contentTypeMessage(values []string, pin int, conflicting bool) string {
	decls := echoSafe(values, pin)
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
