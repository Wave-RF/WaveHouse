import type { WaveHouseError, StreamEvent, StreamStatus, StreamSubscriber } from '../types.js';

/** @internal Transport abstraction for SSE and WebSocket backends. */
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
  private _status: StreamStatus = 'connecting';
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
      if (status === 'closed') {
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
    signal.addEventListener('abort', () => this.close(), { once: true });
  }

  /**
   * Returns a promise that resolves when the stream status reaches `'live'`,
   * or rejects after `timeoutMs` milliseconds (default: 10 000).
   */
  connected(timeoutMs = 10_000): Promise<void> {
    if (this._status === 'live') return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        unsub();
        reject(new Error(`Stream did not connect within ${timeoutMs}ms`));
      }, timeoutMs);

      const unsub = this.subscribe({
        status: (s) => {
          if (s === 'live') {
            clearTimeout(timer);
            unsub();
            resolve();
          }
        },
      });
    });
  }

  /** Close the stream and release resources. */
  close(): void {
    this._transport.disconnect();
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
