import { EventSource } from "eventsource";

// Polyfill EventSource for Node.js so SSE streaming tests work
if (typeof globalThis.EventSource === "undefined") {
  (globalThis as any).EventSource = EventSource;
}
