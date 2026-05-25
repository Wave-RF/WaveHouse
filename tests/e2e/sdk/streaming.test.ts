import { describe, it, expect, afterAll, beforeAll } from "vitest";
import { adminClient, publicClient, dataClient, testId, waitForCondition } from "./helpers.js";

describe("Streaming", () => {
  const admin = adminClient();
  let baselinePolicy: any;

  beforeAll(async () => {
    // Fetch the baseline policy to restore after tests finish
    const res = await admin.policy.get();
    baselinePolicy = structuredClone(res.data);

    // Configure the backend to assign the "anon" role to unauthenticated requests
    const publicPolicy = structuredClone(baselinePolicy);
    publicPolicy.default_role = "anon";

    // Explicitly allow the 'anon' role to SELECT (stream) from these tables
    publicPolicy.tables.clicks.select = {
      ...(publicPolicy.tables.clicks.select || {}),
      anon: { allow_columns: ["*"] },
    };
    publicPolicy.tables.events.select = {
      ...(publicPolicy.tables.events.select || {}),
      anon: { allow_columns: ["*"] },
    };

    await admin.policy.set(publicPolicy);
  });

  afterAll(async () => {
    // Clean up to ensure we don't bleed public access into other test files
    if (baselinePolicy) {
      await admin.policy.set(baselinePolicy);
    }
  });

  describe("SSE", () => {
    // TODO: re-enable after #172 merged
    it.skip("receives events after insert (public/anon)", async () => {
      const whPublic = publicClient();
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      const stream = whPublic.from("clicks").stream({ transport: "sse" });
      const unsub = stream.subscribe({
        // initial: (result) => console.log("Initial SSE result:", result),
        next: (event) => receivedEvents.push(event),
        // status: (status) => console.log("SSE status:", status),
        error: (err) => console.error("SSE error:", err),
      });

      // TODO: replace with wait for status connected?
      await new Promise((r) => setTimeout(r, 1000));

      await whAuth.from("clicks").insert({
        event_id: id,
        page: "/sse-public-test",
        user_id: "public-user",
        session_id: "sse-sess",
        country: "US",
        duration_ms: 99,
      });

      await waitForCondition(
        () => receivedEvents.some((e) => e.data?.event_id === id),
        10_000,
      );

      const matchedEvent = receivedEvents.find((e) => e.data?.event_id === id);
      expect(matchedEvent).toBeDefined();
      expect(matchedEvent?.data.user_id).toBe("public-user");

      unsub();
      stream.close();
    });

    it("receives events after insert (authenticated via ?token=)", async () => {
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      // The SDK should automatically append the JWT as ?token= here
      const stream = whAuth.from("clicks").stream({ transport: "sse" });
      const unsub = stream.subscribe({
        // initial: (result) => console.log("Initial SSE result:", result),
        next: (event) => receivedEvents.push(event),
        status: (status) => console.log("SSE status:", status),
        error: (err) => console.error("SSE error:", err),
      });

      await stream.connected(10_000);

      await whAuth.from("clicks").insert({
        event_id: id,
        page: "/sse-auth-test",
        user_id: "auth-user",
        session_id: "sse-sess",
        country: "US",
        duration_ms: 99,
      });

      await waitForCondition(
        () => receivedEvents.some((e) => e.data?.event_id === id),
        10_000,
      );

      const matchedEvent = receivedEvents.find((e) => e.data?.event_id === id);
      expect(matchedEvent).toBeDefined();
      expect(matchedEvent?.data.user_id).toBe("auth-user");

      unsub();
      stream.close();
    });
  });

  describe("WebSocket", () => {
    // TODO: re-enable after #172 merged
    it.skip("receives events after insert (public/anon)", async () => {
      const whPublic = publicClient();
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      const stream = whPublic.from("events").stream({ transport: "ws" });
      const unsub = stream.subscribe({
        // initial: (result) => console.log("WS stream initial:", result),
        next: (event) => receivedEvents.push(event),
        // status: (status) => console.log("WS stream status:", status),
        error: (err) => console.error("WS stream error:", err),
      });

      // TODO: replace with wait for status connected?
      // Give the WS connection a solid moment to handshake
      await new Promise((r) => setTimeout(r, 3000));

      await whAuth.from("events").insert({
        event_id: id,
        type: "ws_public_test",
        user_id: "ws-user",
        source: "test",
      });

      await waitForCondition(
        () => receivedEvents.some((e) => e.data?.event_id === id),
        10_000,
      );

      const matchedEvent = receivedEvents.find((e) => e.data?.event_id === id);
      expect(matchedEvent).toBeDefined();
      expect(matchedEvent?.data.user_id).toBe("ws-user");

      unsub();
      stream.close();
    });

    it("receives events after insert (authenticated via ?token=)", async () => {
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      const stream = whAuth.from("events").stream({ transport: "ws" });
      const unsub = stream.subscribe({
        // initial: (result) => console.log("Initial WS result:", result),
        next: (event) => receivedEvents.push(event),
        status: (status) => console.log("WS status:", status),
        error: (err) => console.error("WS error:", err),
      });

      await stream.connected(10_000);

      await whAuth.from("events").insert({
        event_id: id,

        user_id: "ws-auth-user",
        source: "test",
      });

      await waitForCondition(
        () => receivedEvents.some((e) => e.data?.event_id === id),
        10_000,
      );

      const matchedEvent = receivedEvents.find((e) => e.data?.event_id === id);
      expect(matchedEvent).toBeDefined();
      expect(matchedEvent?.data.user_id).toBe("ws-auth-user");

      unsub();
      stream.close();
    });
  });
});
