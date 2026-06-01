import { describe, expect, it, vi } from "vitest";
import type { StreamEvent, StreamStatus, WaveHouseError } from "../types.js";
import type { StreamTransport } from "./controller.js";
import { StreamController } from "./controller.js";

function makeTransport<T = Record<string, unknown>>(): StreamTransport<T> & {
  fireEvent(e: StreamEvent<T>): void;
  fireStatus(s: StreamStatus): void;
  fireError(e: WaveHouseError): void;
} {
  const t: StreamTransport<T> & {
    fireEvent(e: StreamEvent<T>): void;
    fireStatus(s: StreamStatus): void;
    fireError(e: WaveHouseError): void;
  } = {
    connect: vi.fn(),
    disconnect: vi.fn(),
    onEvent: null,
    onStatus: null,
    onError: null,
    fireEvent(e: StreamEvent<T>) {
      this.onEvent?.(e);
    },
    fireStatus(s: StreamStatus) {
      this.onStatus?.(s);
    },
    fireError(e: WaveHouseError) {
      this.onError?.(e);
    },
  };
  return t;
}

const event1: StreamEvent = {
  table: "clicks",
  timestamp: "2024-01-01T00:00:00Z",
  data: { page: "/a" },
};
const event2: StreamEvent = {
  table: "clicks",
  timestamp: "2024-01-01T00:00:01Z",
  data: { page: "/b" },
};

describe("StreamController", () => {
  it("calls transport.connect() on construction", () => {
    const t = makeTransport();
    new StreamController(t);
    expect(t.connect).toHaveBeenCalledOnce();
  });

  it("initial status is connecting", () => {
    const t = makeTransport();
    const ctrl = new StreamController(t);
    expect(ctrl.status).toBe("connecting");
  });

  describe("subscribe()", () => {
    it("invokes next callback for each event", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const next = vi.fn();

      ctrl.subscribe({ next });
      t.fireEvent(event1);
      t.fireEvent(event2);

      expect(next).toHaveBeenCalledTimes(2);
      expect(next).toHaveBeenCalledWith(event1);
      expect(next).toHaveBeenCalledWith(event2);
    });

    it("fires status callback immediately with current status", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const status = vi.fn();

      ctrl.subscribe({ next: vi.fn(), status });
      expect(status).toHaveBeenCalledWith("connecting");
    });

    it("fires status callback on status changes", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const status = vi.fn();

      ctrl.subscribe({ next: vi.fn(), status });
      t.fireStatus("live");

      expect(status).toHaveBeenCalledWith("live");
      expect(ctrl.status).toBe("live");
    });

    it("fires error callback", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const error = vi.fn();

      ctrl.subscribe({ next: vi.fn(), error });
      const err: WaveHouseError = {
        status: 0,
        code: "STREAM_ERROR",
        message: "Stream error",
        retryable: true,
      };
      t.fireError(err);

      expect(error).toHaveBeenCalledWith(err);
    });

    it("returns an unsubscribe function", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const next = vi.fn();

      const unsub = ctrl.subscribe({ next });
      t.fireEvent(event1);
      unsub();
      t.fireEvent(event2);

      expect(next).toHaveBeenCalledTimes(1);
    });

    it("auto-closes when last subscriber unsubscribes", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);

      const unsub = ctrl.subscribe({ next: vi.fn() });
      unsub();

      expect(t.disconnect).toHaveBeenCalled();
    });

    it("delivers to multiple subscribers", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const next1 = vi.fn();
      const next2 = vi.fn();

      ctrl.subscribe({ next: next1 });
      ctrl.subscribe({ next: next2 });
      t.fireEvent(event1);

      expect(next1).toHaveBeenCalledWith(event1);
      expect(next2).toHaveBeenCalledWith(event1);
    });
  });

  describe("close()", () => {
    it("disconnects the transport", () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      ctrl.close();
      expect(t.disconnect).toHaveBeenCalled();
    });

    it("resolves pending async iterator waiters with done", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const iter = ctrl[Symbol.asyncIterator]();

      const promise = iter.next();
      ctrl.close();

      const result = await promise;
      expect(result.done).toBe(true);
    });
  });

  describe("async iterator", () => {
    it("yields buffered events immediately", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);

      // Fire events before creating the iterator
      t.fireEvent(event1);
      t.fireEvent(event2);

      const iter = ctrl[Symbol.asyncIterator]();
      const r1 = await iter.next();
      const r2 = await iter.next();

      expect(r1.value).toEqual(event1);
      expect(r2.value).toEqual(event2);
    });

    it("waits for future events", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const iter = ctrl[Symbol.asyncIterator]();

      const promise = iter.next();

      // Event arrives after we start waiting
      t.fireEvent(event1);

      const result = await promise;
      expect(result.value).toEqual(event1);
      expect(result.done).toBe(false);
    });

    it("returns done after close", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const iter = ctrl[Symbol.asyncIterator]();

      ctrl.close();

      const result = await iter.next();
      expect(result.done).toBe(true);
    });

    it("returns done when status is closed", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const iter = ctrl[Symbol.asyncIterator]();

      const promise = iter.next();
      t.fireStatus("closed");

      const result = await promise;
      expect(result.done).toBe(true);
    });

    it("return() closes the stream", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const iter = ctrl[Symbol.asyncIterator]();

      const result = await iter.return!();
      expect(result.done).toBe(true);
      expect(t.disconnect).toHaveBeenCalled();
    });

    it("supports for-await-of pattern", async () => {
      const t = makeTransport();
      const ctrl = new StreamController(t);
      const received: StreamEvent[] = [];

      // Fire events then close
      t.fireEvent(event1);
      t.fireEvent(event2);
      // Close after events are buffered so the loop terminates
      t.fireStatus("closed");

      for await (const event of ctrl) {
        received.push(event);
      }

      expect(received).toEqual([event1, event2]);
    });
  });
});
