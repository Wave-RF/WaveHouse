import type { QueryFilter, Result, StreamEvent, StreamSubscriber } from "../types.js";
import type { StreamController } from "./controller.js";

/**
 * Stream-first live query orchestrator.
 *
 * 1. Opens a live stream immediately (buffers events).
 * 2. Fetches historical data.
 * 3. Calls `subscriber.initial(result)` with the historical snapshot.
 * 4. Deduplicates buffered events against the fetch result.
 * 5. Flushes remaining buffered events through `subscriber.next()`.
 * 6. Resumes live: pipes stream events directly to `subscriber.next()`.
 */
export class LiveQuery<T = Record<string, unknown>> {
  private _stream: StreamController<T>;
  private _subscriber: StreamSubscriber<T>;
  private _buffer: StreamEvent<T>[] = [];
  private _buffering = true;
  private _unsubStream: (() => void) | null = null;
  private _closed = false;

  constructor(
    stream: StreamController<T>,
    fetchFn: () => Promise<Result<T[]>>,
    subscriber: StreamSubscriber<T>,
    _filters: QueryFilter[],
  ) {
    this._stream = stream;
    this._subscriber = subscriber;

    // Step 1: Subscribe to live events and buffer them.
    this._unsubStream = stream.subscribe({
      next: (event) => {
        if (this._closed) return;
        if (this._buffering) {
          this._buffer.push(event);
        } else {
          subscriber.next(event);
        }
      },
      status: (s) => subscriber.status?.(s),
      error: (e) => subscriber.error?.(e),
    });

    // Step 2-5: Fetch historical and flush.
    this._runBackfill(fetchFn);
  }

  private async _runBackfill(fetchFn: () => Promise<Result<T[]>>): Promise<void> {
    try {
      const result = await fetchFn();

      if (this._closed) return;

      // Step 3: Call initial() with the historical snapshot.
      this._subscriber.initial?.(result);

      if (result.error) {
        this._buffering = false;
        return;
      }

      // Step 4: Deduplicate buffered events against fetched rows.
      // Use received_timestamp as the dedup boundary: only flush events
      // that are newer than the latest row in the fetch result.
      const rows = result.data;
      let lastTimestamp: string | undefined;
      if (rows.length > 0) {
        const lastRow = rows[rows.length - 1] as Record<string, unknown>;
        lastTimestamp = lastRow?.received_timestamp as string | undefined;
      }

      // Step 5: Flush buffered events that are newer than the fetch.
      this._buffering = false;
      for (const event of this._buffer) {
        if (this._closed) break;
        if (lastTimestamp && event.timestamp <= lastTimestamp) {
          continue; // already covered by the fetch
        }
        this._subscriber.next(event);
      }
      this._buffer = [];
    } catch {
      // Fetch failed — stop buffering and resume live only.
      this._buffering = false;
      this._buffer = [];
    }
  }

  /** Close the live query and the underlying stream. */
  close(): void {
    this._closed = true;
    this._buffering = false;
    this._buffer = [];
    this._unsubStream?.();
    this._stream.close();
  }
}
