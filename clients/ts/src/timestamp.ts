/**
 * Timestamp comparison for the SDK's client-side work: stream `where` filters
 * and the live-query backfill seam.
 *
 * The server renders every DateTime as RFC 3339 in UTC at the column's scale
 * (`2026-01-15T10:30:00Z`, `2026-01-15T10:30:00.120Z`), and an SSE envelope's
 * `timestamp` carries up to nine trimmed fraction digits. Comparing those as
 * strings is right only when both sides share a zone and a scale: `…:00Z`
 * sorts after `…:00.000Z` (`Z` > `.`), and a filter value written with an
 * offset compares by its digits, not its instant. So two strings that both
 * read as timestamps are compared as instants, to the nanosecond.
 *
 * A value with no zone is read as UTC. The server reads one in the column's
 * zone, which the SDK cannot know; UTC is the zone every rendered value is in.
 *
 * @internal
 */

/** An instant: whole seconds since the epoch, and nanoseconds into that second. */
interface Instant {
  seconds: number;
  nanos: number;
  /** The fraction digits as written, which `"coarsest"` truncates to. */
  digits: string;
}

// YYYY-MM-DD, then optionally a time (`T`, `t` or a space; seconds and a
// fraction of up to nine digits optional), then optionally a zone.
const TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})(?:[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?(Z|z|[+-]\d{2}(?::?\d{2})?)?$/;

function parseInstant(s: string): Instant | undefined {
  const m = TIMESTAMP.exec(s);
  if (!m) return undefined;
  const [, y, mo, d, h = "0", mi = "0", sec = "0", digits = "", zone = "Z"] = m;
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);
  const second = Number(sec);
  if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) {
    return undefined;
  }
  let offsetMinutes = 0;
  if (zone !== "Z" && zone !== "z") {
    const sign = zone[0] === "-" ? -1 : 1;
    const hhmm = zone.slice(1).replace(":", "");
    offsetMinutes = sign * (Number(hhmm.slice(0, 2)) * 60 + Number(hhmm.slice(2) || "0"));
  }
  // setUTCFullYear rather than Date.UTC, which maps years 0–99 onto 1900–1999.
  const date = new Date(0);
  date.setUTCFullYear(Number(y), month - 1, day);
  date.setUTCHours(hour, minute, second, 0);
  const ms = date.getTime();
  if (Number.isNaN(ms)) return undefined;
  return { seconds: ms / 1000 - offsetMinutes * 60, nanos: Number(digits.padEnd(9, "0")), digits };
}

/**
 * Order two timestamps by instant: negative when `a` is earlier, zero when they
 * are the same instant, positive when `a` is later, and `undefined` when either
 * does not read as a timestamp (the caller falls back to its own comparison).
 *
 * `"coarsest"` compares at the precision of whichever side writes fewer
 * fraction digits, truncating the other: an event stamped `…00.123456789Z`
 * against a `DateTime64(3)` row's `…00.123Z` is the same millisecond. `"exact"`
 * (the default) compares to the nanosecond, as the server does.
 *
 * @internal
 */
export function compareInstants(
  a: string,
  b: string,
  precision: "exact" | "coarsest" = "exact",
): number | undefined {
  const x = parseInstant(a);
  const y = parseInstant(b);
  if (!x || !y) return undefined;
  if (x.seconds !== y.seconds) return x.seconds < y.seconds ? -1 : 1;
  let xn = x.nanos;
  let yn = y.nanos;
  if (precision === "coarsest") {
    const digits = Math.min(x.digits.length, y.digits.length);
    xn = Number(x.digits.slice(0, digits).padEnd(9, "0"));
    yn = Number(y.digits.slice(0, digits).padEnd(9, "0"));
  }
  return Math.sign(xn - yn);
}
