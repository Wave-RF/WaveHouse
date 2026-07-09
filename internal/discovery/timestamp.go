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
// RFC 3339 UTC (`2026-06-21T04:00:00Z`, fraction per column precision) — before the
// event is published (#372). The event payload is fanned out verbatim to SSE
// subscribers and inserted into ClickHouse as-is, so without one canonical form the
// same instant reaches stream consumers in whatever spelling the producer chose
// (zone-less ClickHouse-native, Unix seconds, …) while /v1/query renders the stored
// value as RFC 3339 — the two read paths land on different clocks. ClickHouse itself
// discards the input spelling at insert, so canonicalizing costs nothing on the
// query path and makes the streamed form match it.

// IsTimestampType reports whether chType is a ClickHouse DateTime or DateTime64
// (unwrapping Nullable/LowCardinality modifiers). Date/Date32 are deliberately not
// included: they are day-precision values with no zone or spelling ambiguity on the
// paths #372 covers, so ingest passes them through untouched.
func IsTimestampType(chType string) bool {
	return strings.HasPrefix(unwrapType(chType), "DateTime")
}

// CanonicalizeTimestamps rewrites every DateTime/DateTime64 column value in data to
// the canonical RFC 3339 UTC form, in place. Zone-less values are interpreted the
// way ClickHouse itself would parse them — in the column's declared time zone, else
// the server default — so canonicalization never changes which instant is stored,
// only how it is spelled. Each column's precision and zone come from its spec,
// precomputed when the schema was built (Refresh / NewSchemaRegistryFromMap); a
// schema built outside a registry falls back to per-record resolution with UTC as
// the server default. The fraction is truncated to the column's precision so the
// streamed value equals what ClickHouse stores and /v1/query returns. Absent and
// null values are left untouched (defaults and Nullable columns). An unparseable
// value returns an error naming the column, which ingest maps to a per-record 400 —
// the same class of failure previously surfaced only at the async insert, via the
// DLQ.
func CanonicalizeTimestamps(schema *TableSchema, data map[string]any) error {
	for _, col := range schema.Columns {
		spec := col.tsSpec
		if spec == nil && !IsTimestampType(col.Type) {
			continue
		}
		v, ok := data[col.Name]
		if !ok || v == nil {
			continue
		}
		if spec == nil {
			// No precomputed spec: a hand-built schema, or a zone the schema build
			// could not resolve. Resolving here keeps that failure scoped to the
			// records that carry the column (a per-record 400), never the schema.
			s, err := resolveTimestampSpec(col.Type, nil)
			if err != nil {
				return fmt.Errorf("column %q: %w", col.Name, err)
			}
			spec = &s
		}
		t, err := parseTimestamp(v, spec.loc)
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
		data[col.Name] = canonicalTimestamp(t, spec.precision)
	}
	return nil
}

// timestampSpec is a timestamp column's precomputed canonicalization inputs: the
// DateTime64 sub-second precision (0 for DateTime) and the zone in which zone-less
// values are interpreted (the column's declared zone, else the ClickHouse server
// default at resolve time).
type timestampSpec struct {
	precision int
	loc       *time.Location
}

// resolveTimestampSpecs precomputes the timestampSpec of every DateTime/DateTime64
// column in ts, in place — run once per schema build so the per-record ingest path
// never parses type strings, loads zones, or allocates. A column whose zone Go
// cannot resolve keeps a nil spec and is logged, not fatal: ClickHouse validated
// the name at CREATE TABLE, so a miss means tzdata skew between the ClickHouse
// server and this binary (cmd/wavehouse embeds time/tzdata to keep that window
// minimal). Ingest then resolves that one column per record and rejects its values
// with the same error — per-record 400s on one column, not a failed refresh for
// the whole schema.
func resolveTimestampSpecs(ts *TableSchema, serverTZ *time.Location, logger *slog.Logger) {
	for i := range ts.Columns {
		col := &ts.Columns[i]
		if !IsTimestampType(col.Type) {
			continue
		}
		spec, err := resolveTimestampSpec(col.Type, serverTZ)
		if err != nil {
			logger.Warn("cannot resolve timestamp column spec; its ingest values will be rejected",
				"table", ts.Name, "column", col.Name, "type", col.Type, "error", err)
			continue
		}
		col.tsSpec = &spec
	}
}

// resolveTimestampSpec extracts a DateTime/DateTime64 type's sub-second precision
// and time zone: `DateTime` / `DateTime('TZ')` / `DateTime64(P)` /
// `DateTime64(P, 'TZ')`. A type without an explicit zone uses serverTZ (nil ⇒
// UTC), matching how ClickHouse interprets zone-less strings for that column.
func resolveTimestampSpec(chType string, serverTZ *time.Location) (timestampSpec, error) {
	if serverTZ == nil {
		serverTZ = time.UTC
	}
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

// parseTimestamp converts one ingested value into a time.Time. Accepted forms are
// the ones ClickHouse itself accepts on this path: RFC 3339 (zone-explicit),
// `YYYY-MM-DD[ T]HH:MM:SS[.fraction]` and `YYYY-MM-DD` (zone-less, interpreted in
// loc), and Unix seconds as a JSON number or a numeric string. Anything else is an
// error.
func parseTimestamp(v any, loc *time.Location) (time.Time, error) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, nil
		}
		// Zone-less forms; Go's Parse accepts an input fraction after the seconds
		// even when the layout carries none.
		for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, x, loc); err == nil {
				return t, nil
			}
		}
		// A bare number is Unix seconds — the quoted twin of the JSON-number form.
		// ClickHouse's own text parsing read digit-strings this way before #372, so
		// the form keeps working (non-finite values are not an instant).
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
// truncated to the column's precision. time.RFC3339Nano trims trailing fractional
// zeros — exactly how /v1/query renders (encoding/json marshals time.Time with the
// same layout), so the two read paths stay byte-identical for UTC columns.
func canonicalTimestamp(t time.Time, precision int) string {
	unit := time.Second
	for range precision {
		unit /= 10
	}
	return t.UTC().Truncate(unit).Format(time.RFC3339Nano)
}
