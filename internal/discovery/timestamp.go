package discovery

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"
)

// Ingest canonicalizes every DateTime/DateTime64 column value to one wire form —
// RFC 3339 UTC (`2026-06-21T04:00:00Z`, fraction per column precision) — before
// the event is published (#372), so SSE subscribers see the same spelling
// /v1/query renders; fail-open, with fail-closed enforcement left to the stream
// row-filter (#381). The grammar is a strict subset of what ClickHouse's insert
// reads — mirrored per column kind and differential-tested against a live server
// (tests/integration/timestamp_canonicalization_test.go) — and everything else
// passes through verbatim, so ClickHouse is never second-guessed.

// IsTimestampType reports whether chType is a ClickHouse DateTime or DateTime64
// (unwrapping Nullable/LowCardinality). Date/Date32 are excluded — day-precision,
// no zone or spelling ambiguity.
func IsTimestampType(chType string) bool {
	return strings.HasPrefix(unwrapType(chType), "DateTime")
}

// CanonicalizeTimestamps rewrites every DateTime/DateTime64 column value in data
// to the canonical RFC 3339 UTC form, in place: zone-less values are read in the
// column's zone, else the server default — ClickHouse's own rule, so only the
// spelling changes, never the instant — and the fraction truncates to the
// column's precision to byte-match /v1/query. Everything else passes through
// verbatim (fail-open): absent/null values, unparseable values, columns without
// a spec, and instants outside the column kind's range (see rewritable).
func CanonicalizeTimestamps(schema *TableSchema, data map[string]any) {
	for _, col := range schema.Columns {
		spec := col.tsSpec
		if spec == nil {
			continue
		}
		v, ok := data[col.Name]
		if !ok || v == nil {
			continue
		}
		t, err := parseTimestamp(v, spec)
		if err != nil || !spec.rewritable(t) {
			continue
		}
		data[col.Name] = canonicalTimestamp(t, spec.precision)
	}
}

// timestampSpec is a timestamp column's precomputed canonicalization inputs:
// sub-second precision (0 for DateTime), the column kind (ClickHouse reads
// numbers and Unix-string fractions differently for DateTime64), and the zone
// for zone-less values (nil = unknown ⇒ only zone-explicit values canonicalize).
type timestampSpec struct {
	precision int
	isDT64    bool
	loc       *time.Location
}

// resolveTimestampSpecs precomputes the timestampSpec of every DateTime/DateTime64
// column in ts once per schema build, so the per-record ingest path parses no
// type strings and loads no zones. An unresolvable zone (the distroless image
// ships no tzdata) keeps a nil spec — warned, not fatal: those values pass
// through un-canonicalized.
func resolveTimestampSpecs(ts *TableSchema, serverTZ *time.Location, logger *slog.Logger) {
	for i := range ts.Columns {
		col := &ts.Columns[i]
		if !IsTimestampType(col.Type) {
			continue
		}
		spec, err := resolveTimestampSpec(col.Type, serverTZ)
		if err != nil {
			logger.Warn("cannot resolve timestamp column spec; its ingest values will pass through un-canonicalized",
				"table", ts.Name, "column", col.Name, "type", col.Type, "error", err)
			continue
		}
		col.tsSpec = &spec
	}
}

// resolveTimestampSpec extracts a DateTime/DateTime64 type's sub-second precision
// and time zone: `DateTime` / `DateTime('TZ')` / `DateTime64(P)` /
// `DateTime64(P, 'TZ')`. A type without an explicit zone takes serverTZ (possibly
// nil = unknown) — ClickHouse's own rule for zone-less strings.
func resolveTimestampSpec(chType string, serverTZ *time.Location) (timestampSpec, error) {
	t := unwrapType(chType)

	var args []string
	if open := strings.IndexByte(t, '('); open != -1 && strings.HasSuffix(t, ")") {
		for arg := range strings.SplitSeq(t[open+1:len(t)-1], ",") {
			args = append(args, strings.TrimSpace(arg))
		}
		t = t[:open]
	}

	isDT64 := t == "DateTime64"
	var precision int
	if isDT64 {
		if len(args) == 0 {
			return timestampSpec{}, fmt.Errorf("malformed type %q: DateTime64 requires a precision", chType)
		}
		p, err := strconv.Atoi(args[0])
		if err != nil || p < 0 || p > 9 {
			return timestampSpec{}, fmt.Errorf("malformed type %q: bad precision %q", chType, args[0])
		}
		precision = p
		args = args[1:]
	}

	if len(args) == 0 {
		return timestampSpec{precision: precision, isDT64: isDT64, loc: serverTZ}, nil
	}
	name := strings.Trim(args[0], "'")
	loc, err := loadLocation(name)
	if err != nil {
		return timestampSpec{}, fmt.Errorf("unknown time zone %q in type %q: %w", name, chType, err)
	}
	return timestampSpec{precision: precision, isDT64: isDT64, loc: loc}, nil
}

func loadLocation(name string) (*time.Location, error) {
	switch name {
	case "Etc/UTC":
		return time.UTC, nil
	case "", "Local":
		// time.LoadLocation reads "" as UTC and "Local" from the process
		// environment — neither is a zone ClickHouse would declare. Treat both
		// as unresolvable (nil spec, pass-through) instead of guessing.
		return nil, fmt.Errorf("not an IANA zone name: %q", name)
	}
	return time.LoadLocation(name)
}

// Rewrite bounds. ClickHouse saturates out-of-range instants spelling-dependently
// (the date clamps in the column zone keeping local time-of-day; DateTime64 at
// precision ≥7 even fails the insert past the Int64-nanosecond ceiling), so no
// rewrite out there is safe — the producer's own spelling passes through and
// saturates as it did before #372. One-day margins keep zone arithmetic from
// straddling a bound.
var (
	dtMin   = time.Unix(0, 0)
	dtMax   = time.Unix(4294967295-86400, 0) // UInt32 seconds ceiling, one day of margin
	dt64Min = time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)
	dt64Max = time.Date(2299, 12, 30, 23, 59, 59, 0, time.UTC)
	ns64Max = time.Date(2262, 4, 10, 23, 59, 59, 0, time.UTC) // Int64 ns ceiling (precision ≥ 7)
)

// rewritable reports whether t is safely inside the column kind's range —
// outside it the value passes through, so ClickHouse's saturation applies to
// the producer's spelling, never a rewritten one.
func (s *timestampSpec) rewritable(t time.Time) bool {
	lo, hi := dtMin, dtMax
	if s.isDT64 {
		lo, hi = dt64Min, dt64Max
		if s.precision >= 7 {
			hi = ns64Max
		}
	}
	return !t.Before(lo) && !t.After(hi)
}

// parseTimestamp converts one ingested value into a time.Time: RFC 3339
// ('.'-fraction only), zone-less `YYYY-MM-DD[ T]HH:MM:SS[.fraction]` /
// `YYYY-MM-DD` in the spec's zone (skipped when nil), and Unix seconds per
// parseUnixDigits and unixNumber — anything else errors and the caller passes
// it through. The grammar is deliberately a subset of ClickHouse best_effort's,
// instant-identical on that subset: when unsure what ClickHouse would read,
// this parser must fail, never guess.
func parseTimestamp(v any, spec *timestampSpec) (time.Time, error) {
	switch x := v.(type) {
	case string:
		// ClickHouse has no ',' decimal separator (it would fight CSV), while
		// Go's RFC3339Nano accepts one per ISO 8601. Reject up front so a
		// ",999" fraction can't widen insertability.
		if strings.ContainsRune(x, ',') {
			return time.Time{}, fmt.Errorf(
				"unrecognized timestamp %.64q (',' is not a fraction separator to ClickHouse)", x)
		}
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, nil
		}
		// Zone-less forms; Go's Parse accepts an input fraction after the seconds
		// even when the layout carries none.
		if spec.loc != nil {
			for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
				if t, err := time.ParseInLocation(layout, x, spec.loc); err == nil {
					return t, nil
				}
			}
		}
		if t, ok := parseUnixDigits(x, spec.isDT64); ok {
			return t, nil
		}
		return time.Time{}, fmt.Errorf(
			"unrecognized timestamp %.64q (accepted: RFC 3339, 'YYYY-MM-DD[ T]HH:MM:SS[.fff]', 'YYYY-MM-DD', or 9-10 digit Unix seconds)", x)
	case float64:
		// Without json.Decoder.UseNumber a producer's integer is exact only
		// through 2^53; past that the digits ClickHouse would read are already
		// lost. Non-integers pass through — ClickHouse rejects them everywhere.
		if x != math.Trunc(x) || math.Abs(x) > 1<<53 {
			return time.Time{}, fmt.Errorf("timestamp number %v is not an exact integer", x)
		}
		return unixNumber(int64(x), spec)
	case json.Number:
		s := x.String()
		if !isDigits(s) {
			// A sign, '.', or exponent: not the integer epoch shape ClickHouse
			// reads for DateTime columns — pass through for its verdict.
			return time.Time{}, fmt.Errorf("timestamp number %q is not a non-negative integer", s)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("timestamp number %q: %w", s, err)
		}
		return unixNumber(n, spec)
	default:
		return time.Time{}, fmt.Errorf("timestamp must be a string or Unix-seconds number, got %T", v)
	}
}

// unixNumber mirrors how ClickHouse reads an integer JSON number: Unix seconds
// for DateTime, ticks at the column's scale for DateTime64 — reading a
// DateTime64 number as seconds would change the stored instant (1750478400 is
// a 1970 instant on a DateTime64(3); the ms epoch 1750478400500 is a valid
// 2025 one). Negative numbers pass through, and out-of-range results are
// caught by rewritable.
func unixNumber(n int64, spec *timestampSpec) (time.Time, error) {
	if n < 0 {
		return time.Time{}, fmt.Errorf("negative timestamp number %d", n)
	}
	if !spec.isDT64 {
		return time.Unix(n, 0), nil
	}
	scale := int64(1)
	for range spec.precision {
		scale *= 10
	}
	return time.Unix(n/scale, (n%scale)*(1_000_000_000/scale)), nil
}

// parseUnixDigits reads Unix seconds in the one string shape ClickHouse
// best_effort does — 9–10 integer digits; other run lengths are its calendar
// forms (8 ⇒ YYYYMMDD, 14 ⇒ YYYYMMDDhhmmss) or its ms/µs/ns epochs (13/16/19)
// and pass through — with a '.' fraction honored only for DateTime64 targets
// (plain DateTime fails the row on it). The fraction is an exact decimal
// truncated at nine digits: ClickHouse truncates, never rounds, and a float64
// round-trip would corrupt nanoseconds or roll across the second.
func parseUnixDigits(s string, isDT64 bool) (time.Time, bool) {
	intPart, frac, hasFrac := strings.Cut(s, ".")
	if len(intPart) < 9 || len(intPart) > 10 || !isDigits(intPart) {
		return time.Time{}, false
	}
	if hasFrac && (!isDT64 || frac == "" || !isDigits(frac)) {
		return time.Time{}, false
	}
	sec, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	var ns int64
	if hasFrac {
		f := frac
		if len(f) > 9 {
			f = f[:9]
		}
		f += strings.Repeat("0", 9-len(f))
		ns, _ = strconv.ParseInt(f, 10, 64) // all digits by construction
	}
	return time.Unix(sec, ns), true
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// canonicalTimestamp renders t in the canonical wire form: UTC RFC 3339, fraction
// truncated to the column's precision. RFC3339Nano trims trailing zeros exactly
// like /v1/query's transformRow, keeping the two read paths byte-identical.
func canonicalTimestamp(t time.Time, precision int) string {
	unit := time.Second
	for range precision {
		unit /= 10
	}
	return t.UTC().Truncate(unit).Format(time.RFC3339Nano)
}
