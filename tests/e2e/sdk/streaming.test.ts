import type { Policy } from "@wavehouse/sdk";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  adminClient,
  authClient,
  dataClient,
  publicClient,
  testId,
  waitForCondition,
} from "./helpers.js";
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

    // Explicitly allow the 'anon' role to SELECT (stream) from this suite's tables.
    // 'scoped' additionally carries a per-subscriber row filter — streamed rows are
    // limited to the caller's own country claim — so the SSE fan-out exercises the
    // row-level-security path (ResolvedPermissions.RowVisible) end to end, not just
    // column projection.
    publicPolicy.tables[T.clicks].select = {
      ...(publicPolicy.tables[T.clicks].select || {}),
      anon: { allow_columns: ["*"] },
      scoped: {
        allow_columns: ["*"],
        filter: { country: { _eq: "{{ jwt.country }}" } },
      },
    };
    publicPolicy.tables[T.events].select = {
      ...(publicPolicy.tables[T.events].select || {}),
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

    it("receives events after insert (authenticated via ?token=)", async () => {
      const whAuth = dataClient();
      const receivedEvents: any[] = [];
      const id = testId();

      // The SDK should automatically append the JWT as ?token= here
      const stream = whAuth.from(T.clicks).stream();

      let unsub: (() => void) | undefined;
      try {
        unsub = stream.subscribe({
          // initial: (result) => console.log("Initial SSE result:", result),
          next: (event) => receivedEvents.push(event),
          // status: (status) => console.log("SSE status:", status),
          error: (err) => console.error("SSE error:", err),
        });

        await stream.connected(20_000);

        await whAuth.from(T.clicks).insert({
          event_id: id,
          page: "/sse-auth-test",
          user_id: "auth-user",
          session_id: "sse-sess",
          country: "US",
          duration_ms: 99,
        });

        await waitForCondition(() => receivedEvents.some((e) => e.data?.event_id === id), 10_000);

        const matchedEvent = receivedEvents.find((e) => e.data?.event_id === id);
        expect(matchedEvent).toBeDefined();
        expect(matchedEvent?.data.user_id).toBe("auth-user");
      } finally {
        if (unsub) unsub();
        stream.close();
      }
    });

    it("applies the role's row filter per subscriber (row-level scoping)", async () => {
      // Two subscribers, same 'scoped' role but different country claims. The role's
      // filter (country = {{ jwt.country }}) must be evaluated per subscriber against
      // the full event, so each sees only its own country's rows over the live stream —
      // the #319 row-level-security guarantee, exercised end to end on SSE.
      const inserter = dataClient(); // viewer may insert into this suite's tables
      const usClient = authClient("scoped", { country: "US" });
      const caClient = authClient("scoped", { country: "CA" });
      const usEvents: any[] = [];
      const caEvents: any[] = [];
      const usId = testId();
      const caId = testId();

      const usStream = usClient.from(T.clicks).stream();
      const caStream = caClient.from(T.clicks).stream();
      let unsubUs: (() => void) | undefined;
      let unsubCa: (() => void) | undefined;
      try {
        unsubUs = usStream.subscribe({
          next: (e) => usEvents.push(e),
          error: (err) => console.error("US SSE error:", err),
        });
        unsubCa = caStream.subscribe({
          next: (e) => caEvents.push(e),
          error: (err) => console.error("CA SSE error:", err),
        });
        await usStream.connected(20_000);
        await caStream.connected(20_000);

        // One row per country; the filter must route each to only its matching subscriber.
        const base = { page: "/scoped", user_id: "u", session_id: "s", duration_ms: 1 };
        await inserter.from(T.clicks).insert({ ...base, event_id: usId, country: "US" });
        await inserter.from(T.clicks).insert({ ...base, event_id: caId, country: "CA" });

        // Each subscriber receives its own country's row.
        await waitForCondition(() => usEvents.some((e) => e.data?.event_id === usId), 10_000);
        await waitForCondition(() => caEvents.some((e) => e.data?.event_id === caId), 10_000);

        // Barrier rows make the cross-absence checks deterministic: SSE frames on one
        // connection are strictly ordered behind the same subject, so a leaked
        // cross-country row published *before* a barrier must be parsed before the
        // barrier row is — once each stream has seen its barrier, absence is proven,
        // not just "not yet arrived".
        const usBarrierId = testId();
        const caBarrierId = testId();
        await inserter.from(T.clicks).insert({ ...base, event_id: usBarrierId, country: "US" });
        await inserter.from(T.clicks).insert({ ...base, event_id: caBarrierId, country: "CA" });
        await waitForCondition(
          () => usEvents.some((e) => e.data?.event_id === usBarrierId),
          10_000,
        );
        await waitForCondition(
          () => caEvents.some((e) => e.data?.event_id === caBarrierId),
          10_000,
        );

        expect(usEvents.some((e) => e.data?.event_id === caId)).toBe(false);
        expect(caEvents.some((e) => e.data?.event_id === usId)).toBe(false);
        expect(usEvents.some((e) => e.data?.event_id === caBarrierId)).toBe(false);
        expect(caEvents.some((e) => e.data?.event_id === usBarrierId)).toBe(false);
      } finally {
        if (unsubUs) unsubUs();
        if (unsubCa) unsubCa();
        usStream.close();
        caStream.close();
      }
    });
  });
});
