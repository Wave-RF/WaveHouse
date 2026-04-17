import type { StreamEvent, WaveHouseError } from '../types.js';

export type WSEventCallback<T = Record<string, unknown>> = (event: StreamEvent<T>) => void;
export type WSStatusCallback = (status: 'connecting' | 'live' | 'reconnecting' | 'closed') => void;
export type WSErrorCallback = (error: WaveHouseError) => void;

interface Subscription {
  callback: WSEventCallback<any>;
  onStatus?: WSStatusCallback;
  onError?: WSErrorCallback;
}

/**
 * Manages a single multiplexed WebSocket connection.
 *
 * Instead of one WebSocket per stream, all subscriptions share a single
 * connection. Topics are subscribed/unsubscribed via in-band JSON commands:
 *
 *   {"action":"subscribe","topic":"ingest.clicks"}
 *   {"action":"unsubscribe","topic":"ingest.clicks"}
 *
 * Incoming messages have a topic envelope:
 *
 *   {"topic":"ingest.clicks","data":{"table_name":"clicks",...}}
 */
export class SharedWSManager {
  private _baseURL: string;
  private _auth?: () => Promise<string> | string;
  private _ws: WebSocket | null = null;
  private _subs = new Map<string, Set<Subscription>>();
  private _reconnectAttempt = 0;
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private _closed = false;
  private _connected = false;
  private _pendingCommands: string[] = [];

  constructor(baseURL: string, auth?: () => Promise<string> | string) {
    this._baseURL = baseURL;
    this._auth = auth;
  }

  /**
   * Subscribe to a topic. Opens the WebSocket if not already connected.
   * Returns an unsubscribe function.
   */
  subscribe<T = Record<string, unknown>>(
    topic: string,
    callback: WSEventCallback<T>,
    onStatus?: WSStatusCallback,
    onError?: WSErrorCallback,
  ): () => void {
    const sub: Subscription = { callback, onStatus, onError };

    let topicSubs = this._subs.get(topic);
    const isNewTopic = !topicSubs || topicSubs.size === 0;

    if (!topicSubs) {
      topicSubs = new Set();
      this._subs.set(topic, topicSubs);
    }
    topicSubs.add(sub);

    // Ensure connection is open.
    if (!this._ws && !this._closed) {
      this._doConnect();
      // _doConnect will notify 'connecting' after auth resolves — don't double-fire.
    } else {
      // Connection already exists — immediately notify current status.
      onStatus?.(this._connected ? 'live' : 'connecting');
    }

    // Send subscribe command for new topics.
    if (isNewTopic) {
      this._send(JSON.stringify({ action: 'subscribe', topic }));
    }

    return () => {
      topicSubs!.delete(sub);
      if (topicSubs!.size === 0) {
        this._subs.delete(topic);
        this._send(JSON.stringify({ action: 'unsubscribe', topic }));
      }
      // Close connection if no subscriptions remain.
      if (this._subs.size === 0) {
        this.close();
      }
    };
  }

  /** Close the WebSocket and release all resources. */
  close(): void {
    this._closed = true;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      this._ws.onclose = null;
      this._ws.onerror = null;
      this._ws.onmessage = null;
      this._ws.onopen = null;
      this._ws.close();
      this._ws = null;
    }
    this._connected = false;
    this._notifyAllStatus('closed');
  }

  /** Allow re-opening after close (e.g. after token refresh). */
  reopen(): void {
    this._closed = false;
    this._reconnectAttempt = 0;
  }

  private async _doConnect(): Promise<void> {
    const wsBase = this._baseURL.replace(/^http/, 'ws');
    const url = new URL('/v1/stream/ws', wsBase);

    if (this._auth) {
      const token = await this._auth();
      if (token) {
        url.searchParams.set('token', token);
      }
    }

    this._notifyAllStatus('connecting');
    this._ws = new WebSocket(url.toString());

    this._ws.onopen = () => {
      this._reconnectAttempt = 0;
      this._connected = true;
      this._notifyAllStatus('live');

      // Flush pending commands.
      for (const cmd of this._pendingCommands) {
        this._ws?.send(cmd);
      }
      this._pendingCommands = [];

      // Re-subscribe all active topics.
      for (const topic of this._subs.keys()) {
        this._ws?.send(JSON.stringify({ action: 'subscribe', topic }));
      }
    };

    this._ws.onmessage = (e) => {
      try {
        const envelope = JSON.parse(e.data as string) as {
          topic: string;
          data: {
            table_name: string;
            received_timestamp: string;
            data: unknown;
          };
        };
        if (!envelope.topic || !envelope.data) return;

        const event: StreamEvent = {
          table: envelope.data.table_name,
          timestamp: envelope.data.received_timestamp,
          data: envelope.data.data as Record<string, unknown>,
        };

        // Dispatch to exact topic subscribers.
        const exact = this._subs.get(envelope.topic);
        if (exact) {
          for (const sub of exact) {
            sub.callback(event);
          }
        }

        // Dispatch to wildcard subscribers (e.g. "ingest.>").
        for (const [pattern, subs] of this._subs) {
          if (pattern === envelope.topic) continue; // already handled
          if (matchTopicPattern(pattern, envelope.topic)) {
            for (const sub of subs) {
              sub.callback(event);
            }
          }
        }
      } catch {
        // ignore malformed messages
      }
    };

    this._ws.onerror = () => {
      const error: WaveHouseError = {
        status: 0,
        code: 'WS_ERROR',
        message: 'WebSocket error',
        retryable: true,
      };
      this._notifyAllError(error);
    };

    this._ws.onclose = () => {
      this._connected = false;
      if (!this._closed) {
        this._scheduleReconnect();
      }
    };
  }

  private _send(data: string): void {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(data);
    } else {
      this._pendingCommands.push(data);
    }
  }

  private _scheduleReconnect(): void {
    this._notifyAllStatus('reconnecting');
    const delay = Math.min(1000 * 2 ** this._reconnectAttempt, 30_000);
    this._reconnectAttempt++;
    this._reconnectTimer = setTimeout(() => {
      this._doConnect();
    }, delay);
  }

  private _notifyAllStatus(status: 'connecting' | 'live' | 'reconnecting' | 'closed'): void {
    for (const subs of this._subs.values()) {
      for (const sub of subs) {
        sub.onStatus?.(status);
      }
    }
  }

  private _notifyAllError(error: WaveHouseError): void {
    for (const subs of this._subs.values()) {
      for (const sub of subs) {
        sub.onError?.(error);
      }
    }
  }
}

/**
 * Client-side NATS-style topic matching for dispatching messages.
 *  - `*` matches exactly one token
 *  - `>` as the last token matches one or more tokens
 */
function matchTopicPattern(pattern: string, subject: string): boolean {
  const pTokens = pattern.split('.');
  const sTokens = subject.split('.');

  for (let i = 0; i < pTokens.length; i++) {
    if (pTokens[i] === '>') {
      return i < sTokens.length;
    }
    if (i >= sTokens.length) return false;
    if (pTokens[i] !== '*' && pTokens[i] !== sTokens[i]) return false;
  }
  return pTokens.length === sTokens.length;
}
