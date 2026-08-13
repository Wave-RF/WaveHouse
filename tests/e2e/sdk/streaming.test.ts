import type { Policy } from "@wavehouse/sdk";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { adminClient, dataClient, publicClient, testId, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Streaming", () => {
  const admin = adminClient();
  const T = suiteTables("streaming");
  let baselinePolicy: Policy | undefined;

  beforeAll(async () => {
    // Fetch the baseline policy to restore after tests finish
    const res = await admin.policy.get();
    if (res.error) throw new Error(`Failed to fetch baseline policy: ${res.error.message}`);
    baselinePolicy = structuredClone(res.data);

    // Configure the backend to assign the "anon" role to unauthenticated requests
    const publicPolicy = structuredClone(baselinePolicy);
    publicPolicy.default_role = "anon";

    // Explicitly allow the 'anon' role to SELECT (stream) from this suite's tables
    publicPolicy.tables[T.clicks].select = {
      ...(publicPolicy.tables[T.clicks].select || {}),
      anon: { allow_columns: ["*"] },
    };
    // `anon` deliberately cannot see `payload` here, which is what makes the
    // Authorization-header test below discriminating: `viewer` keeps the
    // baseline `["*"]`, so the two roles observe different frames. Role
    // matching is exact (internal/policy: no "*" any-role wildcard), so this
    // entry is the only thing `anon` gets on this table.
    publicPolicy.tables[T.events].select = {
      ...(publicPolicy.tables[T.events].select || {}),
      anon: { allow_columns: ["event_id", "type", "user_id", "source", "received_timestamp"] },
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

      const stream = whPublic.from(T.clicks).stream();
      let unsub: (() => void) | undefined;
      try {
        unsub = stream.subscribe({
          // initial: (result) => console.log("Initial SSE result:", result),
          next: (event) => receivedEvents.push(event),
          // status: (status) => console.log("SSE status:", status),
          error: (err) => console.error("SSE error:", err),
        });

        await stream.connected(5_000);

        await whAuth.from(T.clicks).insert({
          event_id: id,
          page: "/sse-public-test",
          user_id: "public-user",
          session_id: "sse-sess",
          country: "US",
          duration_ms: 99,
        });

        await waitForCondition(() => receivedEvents.some((e) => e.data?.event_id === id), 10_000);

        const matchedEvent = receivedEvents.find((e) => e.data?.event_id === id);
        expect(matchedEvent).toBeDefined();
        expect(matchedEvent?.data.user_id).toBe("public-user");
      } finally {
        if (unsub) unsub();
        stream.close();
      }
    });

    it("row DateTime columns arrive canonicalized, matching /v1/query (#372)", async () => {
      // Ingest spells the row timestamp with an offset; the wire form everywhere
      // downstream must be canonical RFC 3339 UTC, so the SSE frame and the
      // /v1/query rendering of the same stored instant are byte-identical — the
      // query/stream clock drift #372 reported.
      const whPublic = publicClient();
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      const stream = whPublic.from(T.clicks).stream();
      let unsub: (() => void) | undefined;
      try {
        unsub = stream.subscribe({
          next: (event) => receivedEvents.push(event),
          error: (err) => console.error("SSE error:", err),
        });
        await stream.connected(5_000);

        await whAuth.from(T.clicks).insert({
          event_id: id,
          page: "/canonical-ts",
          user_id: "canon-user",
          session_id: "canon-sess",
          received_timestamp: "2026-06-21T06:00:00.123+02:00",
        });

        await waitForCondition(() => receivedEvents.some((e) => e.data?.event_id === id), 10_000);
        const frame = receivedEvents.find((e) => e.data?.event_id === id);
        expect(frame?.data.received_timestamp).toBe("2026-06-21T04:00:00.123Z");

        // The ClickHouse insert is async behind the stream event — poll the query
        // path until the row lands, then compare the two renderings.
        let queried: any[] = [];
        await waitForCondition(async () => {
          const result = await whAuth
            .from(T.clicks)
            .select("event_id", "received_timestamp")
            .where("event_id", "=", id)
            .fetch();
          queried = result.data ?? [];
          return queried.length === 1;
        }, 15_000);
        expect(queried[0].received_timestamp).toBe(frame?.data.received_timestamp);
      } finally {
        if (unsub) unsub();
        stream.close();
      }
    });

    it("streams a column the public role cannot see (Authorization header)", async () => {
      // The end-to-end proof that the credential move works. It has to be a
      // column `anon` is denied: `/v1/stream` never rejects a bad or missing
      // token, it answers with whatever `default_role` may see — so a test
      // against a table both roles can read fully would pass just as happily
      // with the header dropped, which is exactly what it is meant to catch.
      const whAuth = dataClient();
      const whPublic = publicClient();
      const id = testId();

      const authEvents: any[] = [];
      const anonEvents: any[] = [];

      const authStream = whAuth.from(T.events).stream();
      const anonStream = whPublic.from(T.events).stream();
      let unsubAuth: (() => void) | undefined;
      let unsubAnon: (() => void) | undefined;

      try {
        unsubAuth = authStream.subscribe({
          next: (event) => authEvents.push(event),
          error: (err) => console.error("authed SSE error:", err),
        });
        unsubAnon = anonStream.subscribe({
          next: (event) => anonEvents.push(event),
          error: (err) => console.error("anon SSE error:", err),
        });

        await authStream.connected(20_000);
        await anonStream.connected(20_000);

        await whAuth.from(T.events).insert({
          event_id: id,
          type: "sse-auth-test",
          user_id: "auth-user",
          payload: '{"secret":"viewer-only"}',
          source: "web",
        });

        await waitForCondition(() => authEvents.some((e) => e.data?.event_id === id), 10_000);
        await waitForCondition(() => anonEvents.some((e) => e.data?.event_id === id), 10_000);

        const authed = authEvents.find((e) => e.data?.event_id === id);
        const anon = anonEvents.find((e) => e.data?.event_id === id);

        // Authenticated as `viewer` via the header: the restricted column is present.
        expect(authed?.data.payload).toBe('{"secret":"viewer-only"}');
        // Same table, same event, no credential: the server projected it away.
        // If the SDK stopped sending Authorization, the first assertion would
        // see this frame instead.
        expect(anon).toBeDefined();
        expect(anon?.data.payload).toBeUndefined();
        expect(anon?.data.event_id).toBe(id);
      } finally {
        if (unsubAuth) unsubAuth();
        if (unsubAnon) unsubAnon();
        authStream.close();
        anonStream.close();
      }
    });
  });
});
