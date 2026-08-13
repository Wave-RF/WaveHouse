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
 * How long a connection must stay live before it counts as healthy enough to
 * reset the backoff. Roughly the fixed delay native `EventSource` used, so a
 * server that accepts and instantly closes can't be hammered faster than the
 * transport this replaces.
 */
const STABLE_CONNECTION_MS = 3_000;

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
  // Jitter spans [max(floor, nominal/2), nominal), so a server-requested
  // `retry:` is honored as the lower bound the SSE spec makes it — jittering
  // over the whole nominal range could re-dial at half what the server asked.
  // With no `retry:` (the default) the floor is 0 and this is the full spread.
  const low = Math.max(retryFloorMs, nominal / 2);
  return low + Math.random() * (nominal - low);
}

/**
 * Hand a response body back to the connection pool on a path that won't read
 * it. Skipped when locked — `cancel()` throws on a locked stream, and a locked
 * body was never ours to release anyway.
 */
function releaseBody(res: Response): void {
  // `.catch` is load-bearing, not defensive: cancelling an *errored* stream
  // returns a rejected promise, and an unhandled rejection takes the host
  // process down under Node's default `--unhandled-rejections=throw`. A
  // connection reset between the headers arriving and this call is enough.
  if (res.body && !res.body.locked) res.body.cancel().catch(() => {});
}

/** Outcome of one connection attempt, driving the reconnect decision. */
interface AttemptResult {
  /** Milliseconds the connection stayed readable; 0 if it never opened. */
  liveMs: number;
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
    // Yield before the first attempt so a failure that is detectable
    // synchronously — an unresolvable `baseURL` — still reaches a subscriber.
    // `connect()` runs inside the StreamController constructor, so anything
    // emitted before this point fires before `.stream()` has returned and
    // nobody is listening yet.
    await Promise.resolve();

    let attempt = 0;

    while (!this._closed) {
      const { liveMs, terminal } = await this._attempt();

      if (this._closed) return;
      if (terminal) {
        this.disconnect();
        return;
      }

      // Only a connection that *held* resets the schedule. Resetting on any
      // connection that merely opened lets a server accepting and immediately
      // closing (slow-consumer eviction, a half-broken upstream) pin the client
      // at sub-second retries forever — more aggressive than the ~3s fixed
      // delay EventSource used, against a server already in trouble.
      if (liveMs >= STABLE_CONNECTION_MS) attempt = 0;

      this._emitStatus("reconnecting");
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
      return { liveMs: 0, terminal: true };
    }

    // A token provider that throws is treated as transient, unlike the URL
    // above: `auth` now runs on every attempt, so a refresh endpoint having a
    // bad minute would otherwise tear down a healthy long-lived stream.
    let init: RequestInit;
    try {
      init = await this._init(ac.signal);
    } catch (e) {
      if (this._isAbort(e)) return { liveMs: 0, terminal: true };
      this._emitError({
        status: 0,
        code: "SSE_AUTH_ERROR",
        message: e instanceof Error ? e.message : String(e),
        retryable: true,
      });
      return { liveMs: 0, terminal: false };
    }

    let res: Response;
    try {
      const doFetch = this._opts.fetch;
      res = doFetch ? await doFetch(target, init) : await fetch(target, init);
    } catch (e) {
      if (this._isAbort(e)) return { liveMs: 0, terminal: true };
      this._emitError({
        status: 0,
        code: "SSE_NETWORK_ERROR",
        message: e instanceof Error ? e.message : String(e),
        retryable: true,
      });
      return { liveMs: 0, terminal: false };
    }

    // Redirects are refused, not followed — see `_init`. Surfaced explicitly
    // because both shapes are otherwise misleading: a browser reports an
    // opaque status-0 response, and Node hands back the raw 3xx.
    if (res.type === "opaqueredirect" || (res.status >= 300 && res.status < 400)) {
      this._emitError({
        status: res.status,
        code: "SSE_REDIRECT",
        message:
          "The stream endpoint redirected. Because this request carries a credential, the " +
          "redirect is refused rather than followed: a cross-origin hop strips the " +
          "Authorization header — and this endpoint answers an unauthenticated caller with a " +
          "reduced view instead of an error — while forwarding any configured headers to " +
          "wherever the redirect points. Set `baseURL` to the final URL; most often this is " +
          "an http→https upgrade or a canonical-host redirect at a proxy. To follow it " +
          "anyway, supply an `options.fetch` that overrides `redirect`.",
        retryable: false,
      });
      releaseBody(res);
      return { liveMs: 0, terminal: true };
    }

    if (!res.ok) {
      const error = await parseErrorResponse(res);
      this._emitError(error);
      // 4xx is terminal: whatever rejected this won't be talked round by
      // repeating the request, and native EventSource likewise stops on
      // non-200. Note WaveHouse itself doesn't reject here — the endpoint is
      // ungated and answers a bad token with a reduced view, which is why
      // `auth` is re-read per attempt (#239 tracks enforcing expiry). A 4xx on
      // this path comes from something in front: a gateway, or a proxy.
      return { liveMs: 0, terminal: !error.retryable };
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
      releaseBody(res);
      return { liveMs: 0, terminal: true };
    }

    // `options.fetch` only has to stream on this path — every REST call reads
    // the body with `.text()`. A wrapper that buffers, or clones and logs the
    // body, satisfies REST and then hangs here forever, so say so instead.
    // `locked` and `bodyUsed` both matter and neither implies the other: after
    // `.text()` both are set, but after a bare `getReader()` only `locked` is.
    const body = res.body;
    if (!body || typeof body.getReader !== "function" || body.locked || res.bodyUsed) {
      this._emitError({
        status: res.status,
        code: "SSE_NO_STREAM_BODY",
        message:
          "Streaming requires a response with a readable body. The configured `options.fetch` " +
          "returned one whose body is absent or already read — an implementation that buffers " +
          "or consumes the response cannot be used for `.stream()` or `.liveQuery()`.",
        retryable: false,
      });
      releaseBody(res);
      return { liveMs: 0, terminal: true };
    }

    this._emitStatus("live");
    const openedAt = Date.now();
    await this._pump(body);
    return { liveMs: Date.now() - openedAt, terminal: false };
  }

  /** Resolve the stream URL. Throws on a `baseURL` that isn't a usable http(s) origin. */
  private _url(): string {
    const url = resolveURL(this._opts.baseURL, "/v1/stream");
    // Caught here rather than at `fetch`, which reports an unusable scheme as a
    // generic failure the loop would then retry forever. `baseURL` must be
    // absolute, so a bare host throws out of `resolveURL` above; this catches
    // the absolute-but-wrong case, `ws://` being the tempting one for streams.
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new Error(
        `baseURL must use http or https, got "${url.protocol}". Streaming is plain HTTP — there is no WebSocket endpoint.`,
      );
    }
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

    // Anything a redirect target must not be handed: the bearer token, or the
    // configured headers that carry proxy service-token secrets.
    const credentialed =
      base.Authorization !== undefined || Object.keys(this._opts.headers ?? {}).length > 0;

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
      //   redirect — followed only when this request carries no credential.
      //              Platforms strip `Authorization` on a cross-origin redirect
      //              (whatwg/fetch#1544) and forward other headers intact, so a
      //              credentialed hop either silently downgrades the stream to
      //              the default role — this endpoint answers an unauthenticated
      //              caller rather than rejecting them — or delivers a configured
      //              secret to whatever the redirect names. Neither is worth a
      //              convenience. Without a credential there is nothing to
      //              protect, so CDN canonicalization, geo/LB indirection, and an
      //              http→https upgrade all just work.
      //              `manual` over `error` for the credentialed case so the
      //              outcome stays inspectable: `error` rejects with a bare
      //              TypeError indistinguishable from a connection failure, which
      //              the loop above would retry forever against a redirect that
      //              is never going to stop happening.
      cache: "no-store",
      redirect: credentialed ? "manual" : "follow",
      // Neutralized like the REST path does, and for a sharper reason here: a
      // body on a GET makes `fetch` throw, which reads as a network failure and
      // would be retried forever against a request that can never be built.
      body: undefined,
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
    // Set when the parser reports a buffer overflow: it is terminated at that
    // point, so the read loop must stop rather than feed it again.
    let overflowed = false;
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
        if (err.type === "max-buffer-size-exceeded") overflowed = true;
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
        // End the connection on the overflow we just reported, instead of
        // feeding a terminated parser and re-reporting it as a read failure.
        if (overflowed) return;
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

  /**
   * Turn one frame's `data` into a StreamEvent.
   *
   * Guarded like `_emitError` and `_emitStatus`: `parser.feed()` dispatches
   * every complete frame in a chunk synchronously, so a subscriber that closes
   * from inside its own `next` — "read until I see my event, then stop" —
   * would otherwise keep receiving the rest of that chunk. `EventSource` ended
   * delivery on `close()`, and gap-fill replay routinely lands many frames in
   * one chunk.
   */
  private _dispatch(data: string): void {
    if (this._closed) return;
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

  /**
   * Same guard for status. Aborting does not retract an already-resolved
   * `Response`, so a `disconnect()` landing between the fetch settling and the
   * "live" below would otherwise flip a closed controller back to live — and it
   * stays there, since the loop then exits without emitting anything further.
   * `disconnect()` calls `onStatus` directly; that terminal "closed" is the one
   * status that must always get through.
   */
  private _emitStatus(status: StreamStatus): void {
    if (this._closed) return;
    this.onStatus?.(status);
  }

  private _isAbort(e: unknown): boolean {
    return (e instanceof DOMException && e.name === "AbortError") || this._closed;
  }

  /** Wait out a reconnect gap, cut short by `disconnect()`. */
  private _sleep(ms: number): Promise<void> {
    // `_wake` only cuts a gap short once the timer exists. A `disconnect()`
    // raised synchronously from the "reconnecting" status callback runs a line
    // earlier than that, so without this the loop would install a timer nobody
    // can cancel and hold the event loop open for the whole backoff — the one
    // hole in the `_wake` machinery.
    if (this._closed) return Promise.resolve();
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
