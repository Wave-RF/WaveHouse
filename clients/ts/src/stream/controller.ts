import type { StreamEvent, StreamStatus, StreamSubscriber, WaveHouseError } from "../types.js";

/** @internal Transport abstraction for SSE backend. */
export interface StreamTransport<T = Record<string, unknown>> {
  connect(): void;
  disconnect(): void;
  onEvent: ((event: StreamEvent<T>) => void) | null;
  onStatus: ((status: StreamStatus) => void) | null;
  onError: ((error: WaveHouseError) => void) | null;
}

/**
 * Controls a live event stream. NOT thenable.
 * Use `.subscribe()` for callback-based consumption or `for await` for async iteration.
 */
export class StreamController<T = Record<string, unknown>> {
  private _transport: StreamTransport<T>;
  private _subscribers = new Set<StreamSubscriber<T>>();
  private _status: StreamStatus = "connecting";
  private _buffer: StreamEvent<T>[] = [];
  private _waiters: { resolve: (value: IteratorResult<StreamEvent<T>>) => void }[] = [];
  private _done = false;

  constructor(transport: StreamTransport<T>) {
    this._transport = transport;

    this._transport.onEvent = (event) => {
      for (const sub of this._subscribers) {
        sub.next(event);
      }
      // Async iterator support
      const waiter = this._waiters.shift();
      if (waiter) {
        waiter.resolve({ value: event, done: false });
      } else {
        this._buffer.push(event);
      }
    };

    this._transport.onStatus = (status) => {
      // Deduplicate: skip if status hasn't changed (e.g. transport fires
      // 'connecting' after StreamController already set it as the initial state).
      if (status === this._status) return;
      this._status = status;
      for (const sub of this._subscribers) {
        sub.status?.(status);
      }
      if (status === "closed") {
        this._done = true;
        for (const w of this._waiters) {
          w.resolve({ value: undefined as never, done: true });
        }
        this._waiters = [];
      }
    };

    this._transport.onError = (error) => {
      for (const sub of this._subscribers) {
        sub.error?.(error);
      }
    };

    this._transport.connect();
  }

  /** Current connection status. */
  get status(): StreamStatus {
    return this._status;
  }

  /**
   * Returns a promise that resolves when the stream status reaches `'live'`,
   * rejects immediately if the stream is already `'closed'`, or rejects after
   * `timeoutMs` milliseconds (default: 5 000) if it never connects.
   *
   * Safe to call before `.subscribe()` — does not trigger auto-close when
   * the internal waiter is removed.
   *
   * `@example`
   * const stream = client.from('events').stream();
   * const unsub = stream.subscribe({ next: (e) => console.log(e) });
   * await stream.connected();   // waits until the transport is live
   * await client.from('events').insert({ ... });
   */
  connected(timeoutMs = 5_000): Promise<void> {
    if (this._status === "live") return Promise.resolve();
    if (this._status === "closed") return Promise.reject(new Error("Stream is closed"));
    if (this._done) return Promise.reject(new Error("Stream closed before connecting"));

    return new Promise((resolve, reject) => {
      let settled = false;

      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        this._subscribers.delete(watcher);
        reject(new Error(`Stream did not connect within ${timeoutMs}ms`));
      }, timeoutMs);

      // Use a private subscriber directly to avoid the auto-close side-effect
      // that the public subscribe() triggers when subscriber count drops to zero.
      const watcher: StreamSubscriber<T> = {
        next: () => {
          // no-op: we only care about status transitions here
        },
        status: (s) => {
          if (s === "live") {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            this._subscribers.delete(watcher);
            resolve();
          } else if (s === "closed") {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            this._subscribers.delete(watcher);
            reject(new Error("Stream closed before connecting"));
          }
        },
      };

      this._subscribers.add(watcher);
    });
  }

  /** Subscribe to stream events via callbacks. Returns an unsubscribe function. */
  subscribe(subscriber: StreamSubscriber<T>): () => void {
    this._subscribers.add(subscriber);
    subscriber.status?.(this._status);

    return () => {
      this._subscribers.delete(subscriber);
      if (this._subscribers.size === 0 && this._waiters.length === 0) {
        this.close();
      }
    };
  }

  /** Attach an AbortSignal — when aborted, the stream is closed. */
  attachSignal(signal: AbortSignal): void {
    if (signal.aborted) {
      this.close();
      return;
    }
    signal.addEventListener("abort", () => this.close(), { once: true });
  }

  /** Close the stream and release resources. */
  close(): void {
    this._transport.disconnect();
    if (this._status !== "closed") {
      this._status = "closed";
      for (const sub of this._subscribers) {
        sub.status?.("closed");
      }
    }
    this._done = true;
    for (const w of this._waiters) {
      w.resolve({ value: undefined as never, done: true });
    }
    this._waiters = [];
  }

  /** Async iterator protocol — enables `for await (const event of stream)`. */
  [Symbol.asyncIterator](): AsyncIterableIterator<StreamEvent<T>> {
    const self = this;
    return {
      next(): Promise<IteratorResult<StreamEvent<T>>> {
        if (self._buffer.length > 0) {
          return Promise.resolve({ value: self._buffer.shift()!, done: false });
        }
        if (self._done) {
          return Promise.resolve({ value: undefined as never, done: true });
        }
        return new Promise((resolve) => {
          self._waiters.push({ resolve });
        });
      },
      return(): Promise<IteratorResult<StreamEvent<T>>> {
        self.close();
        return Promise.resolve({ value: undefined as never, done: true });
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}
