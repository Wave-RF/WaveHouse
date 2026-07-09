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
// /v1/query renders instead of whatever the producer sent. ClickHouse discards
// the input spelling at insert, so only the streamed form changes.
//
// Canonicalization is fail-open: anything it cannot parse or resolve passes
// through verbatim — ClickHouse's more liberal best_effort parser stays the
// arbiter of what is insertable (failures land in the DLQ, as before #372).
// Fail-closed enforcement of the canonical form is the stream row-filter's (#381).

// IsTimestampType reports whether chType is a ClickHouse DateTime or DateTime64
// (unwrapping Nullable/LowCardinality). Date/Date32 are excluded — day-precision,
// no zone or spelling ambiguity.
func IsTimestampType(chType string) bool {
	return strings.HasPrefix(unwrapType(chType), "DateTime")
}

// CanonicalizeTimestamps rewrites every DateTime/DateTime64 column value in data
// to the canonical RFC 3339 UTC form, in place, best-effort. Zone-less values are
// interpreted as ClickHouse would — in the column's zone, else the server default
// (precomputed on the column's spec at schema build) — so only the spelling ever
// changes, never the stored instant; the fraction is truncated to the column's
// precision to byte-match /v1/query. Everything else passes through verbatim
// (fail-open): absent/null values, unparseable values, columns without a spec, and
// zone-less values when no zone is known (assuming one could move the instant).
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
		t, err := parseTimestamp(v, spec.loc)
		if err != nil {
			continue
		}
		data[col.Name] = canonicalTimestamp(t, spec.precision)
	}
}

// timestampSpec is a timestamp column's precomputed canonicalization inputs: the
// DateTime64 sub-second precision (0 for DateTime) and the zone for interpreting
// zone-less values (declared zone, else server default; nil when neither is
// known — then only zone-explicit values canonicalize).
type timestampSpec struct {
	precision int
	loc       *time.Location
}

// resolveTimestampSpecs precomputes the timestampSpec of every DateTime/DateTime64
// column in ts, in place — once per schema build, so the per-record ingest path
// parses no type strings and loads no zones. A column whose zone this binary
// cannot resolve (the distroless image ships no tzdata) keeps a nil spec —
// warned, not fatal: its values pass through un-canonicalized.
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

	var precision int
	if t == "DateTime64" {
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
		return timestampSpec{precision: precision, loc: serverTZ}, nil
	}
	name := strings.Trim(args[0], "'")
	loc, err := time.LoadLocation(name)
	if err != nil {
		return timestampSpec{}, fmt.Errorf("unknown time zone %q in type %q: %w", name, chType, err)
	}
	return timestampSpec{precision: precision, loc: loc}, nil
}

// parseTimestamp converts one ingested value into a time.Time. Accepted forms:
// RFC 3339, `YYYY-MM-DD[ T]HH:MM:SS[.fraction]` and `YYYY-MM-DD` (zone-less,
// interpreted in loc; skipped when loc is nil), and Unix seconds as a number or
// numeric string. Anything else errors — the caller passes it through.
func parseTimestamp(v any, loc *time.Location) (time.Time, error) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, nil
		}
		// Zone-less forms; Go's Parse accepts an input fraction after the seconds
		// even when the layout carries none.
		if loc != nil {
			for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
				if t, err := time.ParseInLocation(layout, x, loc); err == nil {
					return t, nil
				}
			}
		}
		// A bare digit-string is Unix seconds, as ClickHouse itself reads it
		// (non-finite values are not an instant).
		if f, err := strconv.ParseFloat(x, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return unixToTime(f), nil
		}
		return time.Time{}, fmt.Errorf(
			"unrecognized timestamp %.64q (accepted: RFC 3339, 'YYYY-MM-DD[ T]HH:MM:SS[.fff]', 'YYYY-MM-DD', or Unix seconds)", x)
	case float64:
		return unixToTime(x), nil
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return time.Time{}, fmt.Errorf("unrecognized timestamp %q: %w", x.String(), err)
		}
		return unixToTime(f), nil
	default:
		return time.Time{}, fmt.Errorf("timestamp must be a string or Unix-seconds number, got %T", v)
	}
}

// unixToTime converts fractional Unix seconds to a time.Time (nanoseconds rounded;
// float64 loses sub-microsecond precision near current epochs — producers that need
// exact sub-second values send the string form).
func unixToTime(f float64) time.Time {
	sec := math.Floor(f)
	return time.Unix(int64(sec), int64(math.Round((f-sec)*1e9)))
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
