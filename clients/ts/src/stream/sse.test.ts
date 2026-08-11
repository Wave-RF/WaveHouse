import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SSETransport } from "./sse.js";

/** URLs handed to `new EventSource(...)`, newest last. */
let opened: string[] = [];

class FakeEventSource {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;

  readyState = FakeEventSource.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((e: MessageEvent) => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(url: string) {
    opened.push(url);
  }

  close(): void {
    this.readyState = FakeEventSource.CLOSED;
  }
}

/** Let `connect()`'s async `_doConnect()` reach `new EventSource(...)`. */
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("SSETransport URL construction", () => {
  beforeEach(() => {
    opened = [];
    vi.stubGlobal("EventSource", FakeEventSource);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("connects at the origin root for a root-hosted base", async () => {
    new SSETransport({ baseURL: "http://localhost:8080", table: "clicks" }).connect();
    await flush();

    expect(opened).toHaveLength(1);
    const url = new URL(opened[0]);
    expect(url.pathname).toBe("/v1/stream");
    expect(url.searchParams.get("table")).toBe("clicks");
  });

  it("preserves a base path prefix", async () => {
    new SSETransport({
      baseURL: "https://app.example.com/api/warehouse",
      table: "clicks",
    }).connect();
    await flush();

    const url = new URL(opened[0]);
    expect(url.pathname).toBe("/api/warehouse/v1/stream");
    expect(url.searchParams.get("table")).toBe("clicks");
  });

  it("preserves the prefix alongside since and token params", async () => {
    new SSETransport({
      baseURL: "https://app.example.com/api/warehouse/",
      table: "clicks",
      since: "2024-01-01T00:00:00Z",
      auth: async () => "my-token",
    }).connect();
    await flush();

    const url = new URL(opened[0]);
    expect(url.pathname).toBe("/api/warehouse/v1/stream");
    expect(url.searchParams.get("since")).toBe("2024-01-01T00:00:00Z");
    expect(url.searchParams.get("token")).toBe("my-token");
  });

  it("omits the token param when auth resolves empty", async () => {
    new SSETransport({
      baseURL: "https://app.example.com/api/warehouse",
      table: "clicks",
      auth: async () => "",
    }).connect();
    await flush();

    expect(new URL(opened[0]).searchParams.has("token")).toBe(false);
  });

  it("reports a bad baseURL through onError instead of throwing", async () => {
    const onError = vi.fn();
    const t = new SSETransport({ baseURL: "not-a-url", table: "clicks" });
    t.onError = onError;
    t.connect();
    await flush();

    expect(opened).toHaveLength(0);
    expect(onError).toHaveBeenCalledOnce();
    expect(onError.mock.calls[0][0].code).toBe("SSE_CONNECT_ERROR");
  });
});
