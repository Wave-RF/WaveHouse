// Type-safe SSE client for real-time streaming.

import type { WaveHouseConfig, StreamEvent } from "./types.js";

/** Options for SSE subscription. */
export interface SSEOptions {
  /** Topic filter (e.g. "ingest.my_table"). Default: all. */
  topic?: string;
  /** ISO-8601 timestamp for gap-fill replay. */
  since?: string;
  /** Called on each event. */
  onEvent: (event: StreamEvent) => void;
  /** Called on error. Return false to stop reconnecting. */
  onError?: (error: Event) => boolean | void;
  /** Called when connection opens. */
  onOpen?: () => void;
}

/** Active SSE subscription handle. */
export interface SSESubscription {
  /** Close the SSE connection. */
  close(): void;
}

/** Create an SSE subscription to the WaveHouse stream. */
export function subscribe(
  config: WaveHouseConfig,
  options: SSEOptions
): SSESubscription {
  const params = new URLSearchParams();
  if (options.topic) params.set("topic", options.topic);
  if (options.since) params.set("since", options.since);

  const qs = params.toString();
  const url = `${config.baseUrl}/v1/stream/sse${qs ? `?${qs}` : ""}`;

  // EventSource doesn't support auth headers natively.
  // If a token is configured, we append it as a query parameter.
  // For production, prefer a reverse proxy that handles auth.
  const finalUrl = appendToken(url, config.token);

  const es = new EventSource(finalUrl);

  es.onopen = () => options.onOpen?.();

  es.onmessage = (event) => {
    try {
      const parsed = JSON.parse(event.data) as StreamEvent;
      options.onEvent(parsed);
    } catch {
      // Ignore non-JSON messages.
    }
  };

  es.onerror = (event) => {
    const shouldReconnect = options.onError?.(event);
    if (shouldReconnect === false) {
      es.close();
    }
    // EventSource auto-reconnects by default.
  };

  return {
    close() {
      es.close();
    },
  };
}

function appendToken(
  url: string,
  token: WaveHouseConfig["token"]
): string {
  if (!token) return url;
  // Only static string tokens can be used with EventSource.
  if (typeof token === "string") {
    const sep = url.includes("?") ? "&" : "?";
    return `${url}${sep}token=${encodeURIComponent(token)}`;
  }
  return url;
}
