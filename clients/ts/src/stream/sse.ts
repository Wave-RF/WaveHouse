import type { StreamEvent, StreamStatus, WaveHouseError } from '../types.js';
import type { StreamTransport } from './controller.js';

export interface SSEOptions {
  baseURL: string;
  table: string;
  since?: string;
}

/** Module-level active SSE connection counter. */
let activeSSEConnections = 0;
const SSE_WARN_THRESHOLD = 5;

/** SSE transport — used for unauthenticated streams. Native EventSource auto-reconnects. */
export class SSETransport<T = Record<string, unknown>> implements StreamTransport<T> {
  private _opts: SSEOptions;
  private _es: EventSource | null = null;

  onEvent: ((event: StreamEvent<T>) => void) | null = null;
  onStatus: ((status: StreamStatus) => void) | null = null;
  onError: ((error: WaveHouseError) => void) | null = null;

  constructor(opts: SSEOptions) {
    this._opts = opts;
  }

  connect(): void {
    if (typeof EventSource === 'undefined') {
      throw new Error(
        '[wavehouse] EventSource is not available in this environment. ' +
          'Use transport: "ws" for Node.js, or install an EventSource polyfill.',
      );
    }

    const url = new URL('/v1/stream/sse', this._opts.baseURL);
    url.searchParams.set('table', this._opts.table);
    if (this._opts.since) {
      url.searchParams.set('since', this._opts.since);
    }

    activeSSEConnections++;
    if (activeSSEConnections > SSE_WARN_THRESHOLD) {
      console.warn(
        `[wavehouse] ${activeSSEConnections} SSE connections open. ` +
          `Browsers limit HTTP/1.1 to 6 connections per domain. ` +
          `Consider using WebSocket transport instead.`,
      );
    }

    this._es = new EventSource(url.toString());

    this._es.onopen = () => {
      this.onStatus?.('live');
    };

    this._es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data as string) as {
          table_name: string;
          received_timestamp: string;
          data: T;
        };
        const event: StreamEvent<T> = {
          table: msg.table_name,
          timestamp: msg.received_timestamp,
          data: msg.data,
        };
        this.onEvent?.(event);
      } catch {
        // ignore malformed messages
      }
    };

    this._es.onerror = () => {
      if (this._es?.readyState === EventSource.CONNECTING) {
        this.onStatus?.('reconnecting');
      } else if (this._es?.readyState === EventSource.CLOSED) {
        this.onStatus?.('closed');
      }
    };
  }

  disconnect(): void {
    if (this._es) {
      this._es.close();
      this._es = null;
      activeSSEConnections = Math.max(0, activeSSEConnections - 1);
    }
    this.onStatus?.('closed');
  }
}
