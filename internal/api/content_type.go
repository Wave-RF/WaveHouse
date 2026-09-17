package api

import (
	"errors"
	"fmt"
	"mime"
	"slices"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// maxSniffBytes bounds how far the arity peek looks for the first
// non-whitespace byte. Far beyond any reasonable amount of leading whitespace;
// a body that is only whitespace within this window is treated as empty.
const maxSniffBytes = 512

// errUnsupportedContentType is returned when the request declares no
// Content-Type, one whose media type is not in the accepted list, several that
// AGREE on being unreadable, or a line carrying a comma that did not parse
// cleanly. That last case is easy to miss because its media type may well BE in
// the accepted list — `application/json; charset=utf-8, application/x-ndjson`
// resolves to application/json and is still refused, since the comma may be a
// second declaration joined on. Declarations that disagree are
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
// what the client said it sent, so a caller can always tell how their bytes will
// be read without knowing what the first one happens to be.
//
// It is NOT the same thing as the chtypes format the parser is handed — see
// wire(). The two JSON families are one format to ClickHouse and two formats
// here, because they differ in how a body frames its records, which is what
// decides the response shape and how many records the request contains.
type IngestFormat int

const (
	// FormatJSON is the application/json family: one flat object, or a
	// top-level array of them. Which of the two is the body's own business —
	// the first non-whitespace byte picks it — because both are the same
	// format to the parser, differing only in arity.
	FormatJSON IngestFormat = iota
	// FormatNDJSON is newline-delimited JSON: one flat object per line. Byte
	// for byte the same format ClickHouse calls JSONEachRow; the distinction
	// from FormatJSON survives only so a declared-NDJSON body is always a
	// batch and never has its arity sniffed.
	FormatNDJSON
	// FormatCSV is header-less CSV, positional in the table's declaration
	// order (see wireColumns): every wire column, in that order. A header line
	// is not a header — it is one record that fails to parse, with ClickHouse's
	// code 27.
	FormatCSV
	// FormatTSV is FormatCSV's tab-separated twin, with the same positional
	// contract.
	FormatTSV
)

// String renders a format for error messages and logs.
func (f IngestFormat) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatNDJSON:
		return "ndjson"
	case FormatCSV:
		return "csv"
	case FormatTSV:
		return "tsv"
	default:
		return "unknown"
	}
}

// wire is the format chtypes parses the body as. Both JSON families collapse
// onto JSONEachRow: an NDJSON body, a bare object, concatenated objects and a
// newline-framed array are all the same input to ClickHouse's own reader.
func (f IngestFormat) wire() typelayer.Format {
	switch f {
	case FormatCSV:
		return typelayer.FormatCSV
	case FormatTSV:
		return typelayer.FormatTSV
	case FormatJSON, FormatNDJSON:
		return typelayer.FormatJSONEachRow
	default:
		return typelayer.FormatJSONEachRow
	}
}

// alwaysBatch reports whether this format's response is the per-record batch
// shape whatever the body holds. Only the JSON family has an arity question,
// and the first non-whitespace byte answers it (see firstNonSpace).
func (f IngestFormat) alwaysBatch() bool { return f != FormatJSON }

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
	{"text/csv", FormatCSV},
	{"text/tab-separated-values", FormatTSV},
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

// firstNonSpace returns the body's first non-whitespace byte. ok is false when
// there is none within the sniff window — the same bound the streaming reader
// used, kept so a body of leading whitespace longer than the window still reads
// as empty rather than changing meaning now that the whole body is in memory.
func firstNonSpace(body []byte) (byte, bool) {
	for _, c := range body[:min(len(body), maxSniffBytes)] {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c, true
		}
	}
	return 0, false
}

// emptyBodyMessage tailors the empty-body 400 message to the declared format so
// an NDJSON caller still gets the familiar "empty ndjson body".
func emptyBodyMessage(format IngestFormat) string {
	if format == FormatJSON {
		return "empty body"
	}
	return "empty " + format.String() + " body"
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
// differently from the first, or -1 when they all agree. It IS the predicate
// resolveContentType refuses on — that function calls this one — so the
// declaration this names is the one that caused the refusal.
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
// It takes the ALREADY-BOUNDED declarations rather than bounding them itself,
// so the 415 body and the WARN log cannot name different sets: there is one
// echoSafe call per request and both consumers read its result.
func contentTypeMessage(decls []string, conflicting bool) string {
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
