import { createParser } from "eventsource-parser";
import { parseErrorResponse } from "../errors.js";
import { mergeHeaders } from "../http.js";
import type { FetchLike, StreamEvent, StreamStatus, WaveHouseError } from "../types.js";
import { resolveURL } from "../url.js";
import type { StreamTransport } from "./controller.js";

export interface SSEOptions {
  baseURL: string;
  table: string;
  since?: string;
  auth?: () => Promise<string> | string;
  fetch?: FetchLike;
  headers?: Record<string, string>;
  fetchOptions?: RequestInit;
}

/** Module-level active SSE connection counter. */
let activeSSEConnections = 0;
const SSE_WARN_THRESHOLD = 5;

/**
 * Cap on characters the frame parser will buffer before giving up, guarding
 * against unbounded growth from a malformed or hostile stream. The parser
 * defaults to unbounded, so this has to be set explicitly. 16 MiB is generous
 * headroom over the ~1 MiB NATS payload ceiling a single event can carry.
 */
const MAX_BUFFER_CHARS = 16 * 1024 * 1024;

/** Ceiling for reconnect backoff. */
const MAX_BACKOFF_MS = 30_000;

/**
 * Delay before reconnect attempt `attempt` (0-based), jittered.
 *
 * Unlike the REST backoff in `http.ts`, this one is randomized: REST retries
 * are spread across independent calls, but every subscriber to a stream that
 * drops reconnects at the same instant, so a fixed schedule synchronizes the
 * whole fleet into a thundering herd against a server that is probably already
 * unwell. Each attempt waits between 50% and 100% of its nominal delay.
 */
function backoff(attempt: number, retryFloorMs: number): number {
  const nominal = Math.max(retryFloorMs, Math.min(1000 * 2 ** attempt, MAX_BACKOFF_MS));
  return nominal / 2 + Math.random() * (nominal / 2);
}

/** Outcome of one connection attempt, driving the reconnect decision. */
interface AttemptResult {
  /** The attempt reached a readable stream — resets the backoff. */
  live: boolean;
  /** Stop for good: a bad token or missing table can't be fixed by retrying. */
  terminal: boolean;
}

/**
 * SSE transport over `fetch`.
 *
 * Replaces `EventSource`, which cannot set request headers — the reason the JWT
 * used to ride in `?token=`, where it crossed every intermediary in the request
 * URI. In exchange for the `Authorization` header this owns what `EventSource`
 * used to provide: framing, reconnect, and `Last-Event-ID` resumption.
 */
export class SSETransport<T = Record<string, unknown>> implements StreamTransport<T> {
  private _opts: SSEOptions;
  private _abort: AbortController | null = null;
  private _closed = false;
  private _started = false;
  private _counted = false;
  /**
   * Most recent non-empty event id, replayed as `Last-Event-ID` on reconnect.
   *
   * Deliberately ignores empty ids. The server emits a blank `id:` line for
   * passthrough payloads, and per the SSE spec an empty `id` field *clears* the
   * last-event-id — so tracking it faithfully would silently discard the
   * resumption point mid-stream and re-open from the beginning.
   */
  private _lastEventId: string | null = null;
  /** Reconnect floor most recently requested by the server via `retry:`. */
  private _retryFloorMs = 0;
  /** Cuts a reconnect gap short when set — see `_sleep`. */
  private _wake: (() => void) | null = null;

  onEvent: ((event: StreamEvent<T>) => void) | null = null;
  onStatus: ((status: StreamStatus) => void) | null = null;
  onError: ((error: WaveHouseError) => void) | null = null;

  constructor(opts: SSEOptions) {
    this._opts = opts;
  }

  connect(): void {
    // Idempotent: a second call would leave two reconnect loops racing over one
    // transport, each re-dialing the other's dropped connection.
    if (this._closed || this._started) return;
    this._started = true;

    if (!this._opts.fetch && typeof fetch === "undefined") {
      throw new Error(
        "[wavehouse] global fetch is not available in this environment. " +
          "Upgrade to Node 22+ or supply one via `options.fetch`.",
      );
    }

    activeSSEConnections++;
    this._counted = true;
    if (activeSSEConnections > SSE_WARN_THRESHOLD) {
      console.warn(
        `[wavehouse] ${activeSSEConnections} SSE connections open. ` +
          `Browsers limit HTTP/1.1 to 6 connections per domain.`,
      );
    }

    // `_run` resolves rather than rejects on stream failure; this catch is for
    // a programming error inside the loop itself.
    this._run().catch((err) => {
      this._emitError({
        status: 0,
        code: "SSE_CONNECT_ERROR",
        message: err instanceof Error ? err.message : String(err),
        retryable: false,
      });
      this.disconnect();
    });
  }

  disconnect(): void {
    if (!this._closed) {
      this._closed = true;
      this._abort?.abort();
      this._abort = null;
      this._wake?.();
      if (this._counted) {
        activeSSEConnections = Math.max(0, activeSSEConnections - 1);
        this._counted = false;
      }
    }
    this.onStatus?.("closed");
  }

  /** Connect/read/reconnect until the stream is closed or hits a terminal error. */
  private async _run(): Promise<void> {
    let attempt = 0;

    while (!this._closed) {
      const { live, terminal } = await this._attempt();

      if (this._closed) return;
      if (terminal) {
        this.disconnect();
        return;
      }

      // A connection that produced a readable stream resets the schedule, so a
      // long-lived subscription doesn't inherit a maxed-out delay on its first
      // drop after hours of health.
      if (live) attempt = 0;

      this.onStatus?.("reconnecting");
      await this._sleep(backoff(attempt, this._retryFloorMs));
      attempt++;
    }
  }

  /** One connection attempt: request, validate, then pump frames until it ends. */
  private async _attempt(): Promise<AttemptResult> {
    const ac = new AbortController();
    this._abort = ac;

    // A URL that won't build is deterministic — every retry produces the same
    // failure — so it ends the stream rather than looping.
    let target: string;
    try {
      target = this._url();
    } catch (e) {
      this._emitError({
        status: 0,
        code: "SSE_CONNECT_ERROR",
        message: e instanceof Error ? e.message : String(e),
        retryable: false,
      });
      return { live: false, terminal: true };
    }

    // A token provider that throws is treated as transient, unlike the URL
    // above: `auth` now runs on every attempt, so a refresh endpoint having a
    // bad minute would otherwise tear down a healthy long-lived stream.
    let init: RequestInit;
    try {
      init = await this._init(ac.signal);
    } catch (e) {
      if (this._isAbort(e)) return { live: false, terminal: true };
      this._emitError({
        status: 0,
        code: "SSE_AUTH_ERROR",
        message: e instanceof Error ? e.message : String(e),
        retryable: true,
      });
      return { live: false, terminal: false };
    }

    let res: Response;
    try {
      const doFetch = this._opts.fetch;
      res = doFetch ? await doFetch(target, init) : await fetch(target, init);
    } catch (e) {
      if (this._isAbort(e)) return { live: false, terminal: true };
      this._emitError({
        status: 0,
        code: "SSE_NETWORK_ERROR",
        message: e instanceof Error ? e.message : String(e),
        retryable: true,
      });
      return { live: false, terminal: false };
    }

    // Redirects are refused, not followed — see `_init`. Surfaced explicitly
    // because both shapes are otherwise misleading: a browser reports an
    // opaque status-0 response, and Node hands back the raw 3xx.
    if (res.type === "opaqueredirect" || (res.status >= 300 && res.status < 400)) {
      this._emitError({
        status: res.status,
        code: "SSE_REDIRECT",
        message:
          "The stream endpoint redirected, which is refused rather than followed: a " +
          "cross-origin redirect strips the Authorization header, and this endpoint answers " +
          "an unauthenticated caller with a reduced view instead of an error. Point `baseURL` " +
          "at the final URL — most often this is an http→https upgrade or a canonical-host " +
          "redirect at a proxy.",
        retryable: false,
      });
      return { live: false, terminal: true };
    }

    if (!res.ok) {
      const error = await parseErrorResponse(res);
      this._emitError(error);
      // 4xx is terminal: reconnecting can't fix a rejected token or a table
      // that doesn't exist, and native EventSource likewise stops on non-200.
      return { live: false, terminal: !error.retryable };
    }

    // A 200 that isn't an event stream is an intermediary answering for the
    // server — an auth gateway serving its login page is the common one. Left
    // unchecked the parser would chew HTML, find no frames, and leave the
    // stream "live" and permanently silent, indistinguishable from a quiet
    // table. `EventSource` fails the connection here too.
    const contentType = res.headers.get("content-type") ?? "";
    if (!contentType.toLowerCase().split(";")[0].trim().startsWith("text/event-stream")) {
      this._emitError({
        status: res.status,
        code: "SSE_BAD_CONTENT_TYPE",
        message: `Expected \`text/event-stream\`, got \`${contentType || "(none)"}\`. Something between the client and WaveHouse answered this request — an auth gateway's login page is the usual cause.`,
        retryable: false,
      });
      return { live: false, terminal: true };
    }

    // `options.fetch` only has to stream on this path — every REST call reads
    // the body with `.text()`. A wrapper that buffers, or clones and logs the
    // body, satisfies REST and then hangs here forever, so say so instead.
    const body = res.body;
    if (!body || typeof body.getReader !== "function") {
      this._emitError({
        status: res.status,
        code: "SSE_NO_STREAM_BODY",
        message:
          "Streaming requires a response with a readable body. The configured `options.fetch` " +
          "returned one without `body` — an implementation that buffers or reads the response " +
          "cannot be used for `.stream()` or `.liveQuery()`.",
        retryable: false,
      });
      return { live: false, terminal: true };
    }

    this.onStatus?.("live");
    await this._pump(body);
    return { live: true, terminal: false };
  }

  /** Resolve the stream URL. Throws on a `baseURL` that isn't absolute. */
  private _url(): string {
    const url = resolveURL(this._opts.baseURL, "/v1/stream");
    url.searchParams.set("table", this._opts.table);
    if (this._opts.since) {
      url.searchParams.set("since", this._opts.since);
    }
    return url.toString();
  }

  /** Build the request init, minting a token for this attempt. */
  private async _init(signal: AbortSignal): Promise<RequestInit> {
    const base: Record<string, string> = { Accept: "text/event-stream" };

    // Resolved per attempt, not once per stream: a stream outlives its token,
    // and a reconnect replaying an expired JWT authenticates as no one — which
    // this endpoint answers with a silently reduced view, not an error.
    if (this._opts.auth) {
      const token = await this._opts.auth();
      if (token) {
        base.Authorization = `Bearer ${token}`;
      }
    }

    // Takes precedence over `since` server-side, so the initial window stays on
    // the URL and resumption rides the header once there's something to resume.
    if (this._lastEventId) {
      base["Last-Event-ID"] = this._lastEventId;
    }

    const init: RequestInit = {
      ...this._opts.fetchOptions,
      method: "GET",
      headers: mergeHeaders(base, this._opts.headers),
      signal,
      // Owned by the SDK rather than left to `fetchOptions`, unlike the REST
      // path, because on a stream these are correctness rather than preference:
      //
      //   cache    — as an init field, not a `Cache-Control` header. The header
      //              form is not in the server's Access-Control-Allow-Headers,
      //              so it would fail every cross-origin preflight.
      //   redirect — never followed. Platforms strip `Authorization` on a
      //              cross-origin redirect (whatwg/fetch#1544), and this endpoint
      //              never rejects an unauthenticated caller — it serves them the
      //              default role. Following a redirect would therefore turn a
      //              misdirected stream into a silently downgraded one.
      //              `manual` over `error` so the outcome is inspectable: `error`
      //              rejects with a bare TypeError indistinguishable from a
      //              connection failure, which the caller above would retry
      //              forever against a redirect that will never stop happening.
      cache: "no-store",
      redirect: "manual",
    };

    // Only browsers get `credentials`: some runtimes (Cloudflare Workers) throw
    // outright if it is present. Inside a browser an explicit value from
    // `fetchOptions` survives, which is how a cookie-authenticated origin opts
    // into `include`.
    if (!("window" in globalThis)) {
      delete init.credentials;
    }

    return init;
  }

  /** Decode and parse frames until the stream ends, aborts, or errors. */
  private async _pump(body: ReadableStream<Uint8Array>): Promise<void> {
    const parser = createParser({
      onEvent: (msg) => {
        if (msg.id) this._lastEventId = msg.id;
        if (!msg.data) return;
        this._dispatch(msg.data);
      },
      onRetry: (ms) => {
        this._retryFloorMs = Math.min(Math.max(ms, 0), MAX_BACKOFF_MS);
      },
      onError: (err) => {
        this._emitError({
          status: 0,
          code: "SSE_PARSE_ERROR",
          message: err.message,
          retryable: true,
        });
      },
      maxBufferSize: MAX_BUFFER_CHARS,
    });

    const reader = body.getReader();
    const decoder = new TextDecoder();

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        parser.feed(decoder.decode(value, { stream: true }));
      }
    } catch (e) {
      if (this._isAbort(e)) return;
      this._emitError({
        status: 0,
        code: "SSE_READ_ERROR",
        message: e instanceof Error ? e.message : String(e),
        retryable: true,
      });
    } finally {
      reader.cancel().catch(() => {
        // Already torn down — nothing left to release.
      });
    }
  }

  /** Turn one frame's `data` into a StreamEvent. */
  private _dispatch(data: string): void {
    try {
      const msg = JSON.parse(data) as {
        table_name: string;
        received_timestamp: string;
        data: T;
      };
      this.onEvent?.({
        table: msg.table_name,
        timestamp: msg.received_timestamp,
        data: msg.data,
      });
    } catch {
      console.warn("[wavehouse] SSE received malformed message:", data);
      // ignore malformed messages
    }
  }

  /** Suppress callbacks that race a `disconnect()`. */
  private _emitError(error: WaveHouseError): void {
    if (this._closed) return;
    this.onError?.(error);
  }

  private _isAbort(e: unknown): boolean {
    return (e instanceof DOMException && e.name === "AbortError") || this._closed;
  }

  /** Wait out a reconnect gap, cut short by `disconnect()`. */
  private _sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this._wake = null;
        resolve();
      }, ms);
      this._wake = () => {
        clearTimeout(timer);
        this._wake = null;
        resolve();
      };
    });
  }
}
