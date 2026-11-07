import { describe, expect, it } from "vitest";
import { compareInstants } from "./timestamp.js";

describe("compareInstants", () => {
  it("orders the server's spellings by instant, whatever the scale", () => {
    // Lexicographically `…00Z` > `…00.000Z` ('Z' > '.'): the same instant.
    expect(compareInstants("2026-01-15T10:30:00Z", "2026-01-15T10:30:00.000Z")).toBe(0);
    expect(compareInstants("2026-01-15T10:30:00.120Z", "2026-01-15T10:30:00.12Z")).toBe(0);
    expect(compareInstants("2026-01-15T10:30:00Z", "2026-01-15T10:30:00.001Z")).toBe(-1);
    expect(compareInstants("2026-01-15T10:30:00.5Z", "2026-01-15T10:30:00.499999999Z")).toBe(1);
  });

  it("applies an offset", () => {
    expect(compareInstants("2026-01-15T12:30:00+02:00", "2026-01-15T10:30:00.000Z")).toBe(0);
    expect(compareInstants("2026-01-15T05:30:00-0500", "2026-01-15T10:30:00Z")).toBe(0);
    expect(compareInstants("2026-01-15T19:30:00+09", "2026-01-15T10:30:00Z")).toBe(0);
    // 11:00 in +02:00 is 09:00Z — earlier, though its digits sort later.
    expect(compareInstants("2026-01-15T11:00:00+02:00", "2026-01-15T10:30:00Z")).toBe(-1);
  });

  it("reads a value with no zone as UTC, with either separator", () => {
    expect(compareInstants("2026-01-15 10:30:00", "2026-01-15T10:30:00Z")).toBe(0);
    expect(compareInstants("2026-01-15 10:30", "2026-01-15T10:30:00.000Z")).toBe(0);
    expect(compareInstants("2026-01-15", "2026-01-15T00:00:00Z")).toBe(0);
  });

  it("compares to the nanosecond", () => {
    expect(
      compareInstants("2026-01-15T10:30:00.123456789Z", "2026-01-15T10:30:00.123456788Z"),
    ).toBe(1);
  });

  it("orders instants before the epoch", () => {
    expect(compareInstants("1965-06-01T01:02:03.004Z", "1970-01-01T00:00:00Z")).toBe(-1);
    expect(compareInstants("1965-06-01T03:02:03.004+02:00", "1965-06-01T01:02:03.004Z")).toBe(0);
  });

  it("compares at the coarser precision when asked", () => {
    // An envelope's nanoseconds against a DateTime64(3) row: the same millisecond.
    const row = "2026-03-24T12:00:00.123Z";
    expect(compareInstants("2026-03-24T12:00:00.123456789Z", row, "coarsest")).toBe(0);
    expect(compareInstants("2026-03-24T12:00:00.123456789Z", row)).toBe(1);
    expect(compareInstants("2026-03-24T12:00:00.124Z", row, "coarsest")).toBe(1);
    // A trimmed fraction compares at its own digits, which are exact.
    expect(compareInstants("2026-03-24T12:00:00.12Z", row, "coarsest")).toBe(0);
    expect(compareInstants("2026-03-24T12:00:00.13Z", row, "coarsest")).toBe(1);
  });

  it("declines anything that is not a timestamp on both sides", () => {
    expect(compareInstants("/home", "2026-01-15T10:30:00Z")).toBeUndefined();
    expect(compareInstants("2026-01-15T10:30:00Z", "1768473000")).toBeUndefined();
    expect(compareInstants("2026", "2027")).toBeUndefined();
    expect(compareInstants("2026-13-01", "2026-01-01")).toBeUndefined();
    expect(compareInstants("2026-01-15T24:00:00Z", "2026-01-15T00:00:00Z")).toBeUndefined();
    expect(
      compareInstants("2026-01-15T10:30:00.1234567890Z", "2026-01-15T10:30:00Z"),
    ).toBeUndefined();
  });
});
