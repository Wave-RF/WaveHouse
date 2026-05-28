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
    it("receives events after insert (public/anon)", async () => {
      const whPublic = publicClient();
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      const stream = whPublic.from("clicks").stream({ transport: "sse" });
      let unsub: (() => void) | undefined;
      try {
        unsub = stream.subscribe({
          // initial: (result) => console.log("Initial SSE result:", result),
          next: (event) => receivedEvents.push(event),
          // status: (status) => console.log("SSE status:", status),
          error: (err) => console.error("SSE error:", err),
        });

        await stream.connected(5_000);

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
      } finally {
        if (unsub) unsub();
        stream.close();
      }
    });

    it("receives events after insert (authenticated via ?token=)", async () => {
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      // The SDK should automatically append the JWT as ?token= here
      const stream = whAuth.from("clicks").stream({ transport: "sse" });

      let unsub: (() => void) | undefined;
      try {
        unsub = stream.subscribe({
          // initial: (result) => console.log("Initial SSE result:", result),
          next: (event) => receivedEvents.push(event),
          // status: (status) => console.log("SSE status:", status),
          error: (err) => console.error("SSE error:", err),
        });

        await stream.connected(20_000);

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
      } finally {
        if (unsub) unsub();
        stream.close();
      }
    });
  });
});
