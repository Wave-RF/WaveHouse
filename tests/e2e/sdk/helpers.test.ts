/**
 * Unit tests for the E2E suite's own helpers (#440).
 *
 * These need no stack — they live here because they test this directory's
 * code, and this directory's vitest project is the only one that compiles it.
 * Bounds are deliberately loose (order-of-magnitude, not milliseconds): a
 * harness that polices timeouts must not itself fail on a busy machine.
 */

import { describe, expect, it } from "vitest";
import { waitForCondition } from "./helpers.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("waitForCondition", () => {
  it("returns as soon as the condition holds", async () => {
    let calls = 0;
    const started = Date.now();

    await waitForCondition(
      () => {
        calls += 1;
        return calls === 3;
      },
      5_000,
      10,
    );

    expect(calls).toBe(3);
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  it("enforces the budget even when a single poll overruns it", async () => {
    // The regression: the old loop checked the clock only on entry, so this
    // returned at ~5000ms (the fn duration), not at the 300ms budget.
    const started = Date.now();

    await expect(
      waitForCondition(async () => {
        await sleep(5_000);
        return true;
      }, 300),
    ).rejects.toThrow(/Condition not met after 300ms/);

    expect(Date.now() - started).toBeLessThan(3_000);
  });

  it("aborts the signal it hands to fn, so in-flight work can unwind", async () => {
    let sawAbort = false;

    await expect(
      waitForCondition(async (signal) => {
        await new Promise<void>((resolve) => {
          signal.addEventListener(
            "abort",
            () => {
              sawAbort = true;
              resolve();
            },
            { once: true },
          );
        });
        return false;
      }, 300),
    ).rejects.toThrow(/Condition not met/);

    expect(sawAbort).toBe(true);
  });

  it("reports how long it polled, and how many/how slow the polls were", async () => {
    // Many fast polls — the signature of "the condition never became true",
    // as opposed to "the polling itself was starved".
    await expect(waitForCondition(() => false, 400, 50)).rejects.toThrow(
      /polled for \d+ms; \d+ poll\(s\), slowest \d+ms/,
    );
  });

  it("counts a poll still in flight at the deadline as the slowest", async () => {
    const err = await waitForCondition(async () => {
      await sleep(5_000);
      return false;
    }, 500).catch((e: Error) => e);

    // Lower bound only. The property under test is that the in-flight poll was
    // counted at all — an upper bound would put a wall-clock ceiling on a
    // measurement taken while ClickHouse and the server share this machine,
    // which is the exact shape of flake this file exists to remove.
    expect(err).toBeInstanceOf(Error);
    const match = /1 poll\(s\), slowest (\d+)ms/.exec((err as Error).message);
    expect(match).not.toBeNull();
    expect(Number(match?.[1])).toBeGreaterThanOrEqual(400);
  });

  it("propagates a rejecting fn instead of masking it as a timeout", async () => {
    await expect(
      waitForCondition(async () => {
        throw new Error("ClickHouse query failed: no such table");
      }, 5_000),
    ).rejects.toThrow(/no such table/);
  });

  it("does not leak an unhandled rejection when fn rejects after the deadline", async () => {
    const unhandled: unknown[] = [];
    const capture = (err: unknown) => unhandled.push(err);
    process.on("unhandledRejection", capture);

    try {
      await expect(
        waitForCondition(async () => {
          await sleep(600);
          throw new Error("late failure");
        }, 200),
      ).rejects.toThrow(/Condition not met after 200ms/);

      // Outlive the rejecting poll, then give the microtask queue a turn so
      // an unhandled rejection would have been reported by now.
      await sleep(1_000);
      await new Promise((r) => setImmediate(r));
    } finally {
      process.off("unhandledRejection", capture);
    }

    expect(unhandled).toEqual([]);
  });
});
