import type { StreamEvent, StreamStatus, WaveHouseError } from '../types.js';
import type { StreamTransport } from './controller.js';

export interface WSOptions {
  baseURL: string;
  table: string;
  since?: string;
  auth?: () => Promise<string> | string;
}

/** WebSocket transport — used for authenticated streams with auto-reconnect + token refresh. */
export class WSTransport<T = Record<string, unknown>> implements StreamTransport<T> {
  private _opts: WSOptions;
  private _ws: WebSocket | null = null;
  private _reconnectAttempt = 0;
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private _closed = false;

  onEvent: ((event: StreamEvent<T>) => void) | null = null;
  onStatus: ((status: StreamStatus) => void) | null = null;
  onError: ((error: WaveHouseError) => void) | null = null;

  constructor(opts: WSOptions) {
    this._opts = opts;
  }

  connect(): void {
    this._closed = false;
    this._doConnect();
  }

  disconnect(): void {
    this._closed = true;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      this._ws.onclose = null;
      this._ws.onerror = null;
      this._ws.onmessage = null;
      this._ws.close();
      this._ws = null;
    }
    this.onStatus?.('closed');
  }

  private async _doConnect(): Promise<void> {
    const wsBase = this._opts.baseURL.replace(/^http/, 'ws');
    const url = new URL('/v1/stream/ws', wsBase);
    url.searchParams.set('table', this._opts.table);
    if (this._opts.since) {
      url.searchParams.set('since', this._opts.since);
    }

    // Inject auth token for WebSocket upgrade
    if (this._opts.auth) {
      const token = await this._opts.auth();
      if (token) {
        url.searchParams.set('token', token);
      }
    }

    this._ws = new WebSocket(url.toString());

    this._ws.onopen = () => {
      this._reconnectAttempt = 0;
      this.onStatus?.('live');
    };

    this._ws.onmessage = (e) => {
      try {
        // Server WS envelope: {table, data: {table_name, received_timestamp, data}}.
        // For non-EventMessage (raw-JSON pass-through) payloads, table_name on
        // the inner object may be absent — fall back to the envelope's table so
        // the StreamEvent always carries the subscribing table name.
        const envelope = JSON.parse(e.data as string) as {
          table: string;
          data: {
            table_name?: string;
            received_timestamp?: string;
            data: T;
          };
        };
        if (!envelope.table || !envelope.data) return;
        const event: StreamEvent<T> = {
          table: envelope.data.table_name ?? envelope.table,
          timestamp: envelope.data.received_timestamp ?? '',
          data: envelope.data.data,
        };
        this.onEvent?.(event);
      } catch {
        // ignore malformed messages
      }
    };

    this._ws.onerror = () => {
      this.onError?.({
        status: 0,
        code: 'WS_ERROR',
        message: 'WebSocket error',
        retryable: true,
      });
    };

    this._ws.onclose = () => {
      if (!this._closed) {
        this._scheduleReconnect();
      }
    };
  }

  private _scheduleReconnect(): void {
    this.onStatus?.('reconnecting');
    const delay = Math.min(1000 * 2 ** this._reconnectAttempt, 30_000);
    this._reconnectAttempt++;
    this._reconnectTimer = setTimeout(() => {
      this._doConnect();
    }, delay);
  }
}
