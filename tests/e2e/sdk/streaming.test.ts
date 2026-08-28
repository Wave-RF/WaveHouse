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
import { setPolicy } from "./settings.js";
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
      // 'metered' carries a numeric literal bound, so the SSE fan-out exercises
      // the storage-domain numeric comparison (canonical decimal + integer range
      // gate) end to end, not just String equality scoping.
      metered: {
        allow_columns: ["*"],
        filter: { duration_ms: { _gt: "100" } },
      },
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

    await setPolicy(publicPolicy);
  });

  afterAll(async () => {
    // Clean up to ensure we don't bleed public access into other test files
    if (baselinePolicy) {
      await setPolicy(baselinePolicy);
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

        const publicInsert = await whAuth.from(T.clicks).insert({
          event_id: id,
          page: "/sse-public-test",
          user_id: "public-user",
          session_id: "sse-sess",
          country: "US",
          duration_ms: 99,
        });
        expect(publicInsert.error).toBeNull();

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

        const canonInsert = await whAuth.from(T.clicks).insert({
          event_id: id,
          page: "/canonical-ts",
          user_id: "canon-user",
          session_id: "canon-sess",
          received_timestamp: "2026-06-21T06:00:00.123+02:00",
        });
        expect(canonInsert.error).toBeNull();

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

        const inserted = await whAuth.from(T.events).insert({
          event_id: id,
          type: "sse-auth-test",
          user_id: "auth-user",
          payload: '{"secret":"viewer-only"}',
          source: "web",
        });
        // Without this, a failed write shows up as two 10s timeouts blaming
        // the stream for an event that was never published.
        expect(inserted.error).toBeNull();

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
        // A failed write would otherwise surface as a stream timeout below,
        // blaming the subscriber for a row that was never published.
        for (const row of [
          { ...base, event_id: usId, country: "US" },
          { ...base, event_id: caId, country: "CA" },
        ]) {
          expect((await inserter.from(T.clicks).insert(row)).error).toBeNull();
        }

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
        for (const row of [
          { ...base, event_id: usBarrierId, country: "US" },
          { ...base, event_id: caBarrierId, country: "CA" },
        ]) {
          expect((await inserter.from(T.clicks).insert(row)).error).toBeNull();
        }
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

    it("evaluates a numeric row filter in the column's storage domain (UInt32 threshold)", async () => {
      // 'metered' scopes delivery to duration_ms > 100 over a UInt32 column: the
      // constant routes through the canonical-decimal reading and the integer
      // range gate, and both operands compare in the column's storage domain —
      // the #381 storage-domain path, pinned end to end on SSE.
      const inserter = dataClient();
      const client = authClient("metered");
      const events: any[] = [];
      const lowId = testId();
      const highId = testId();

      const stream = client.from(T.clicks).stream();
      let unsub: (() => void) | undefined;
      try {
        unsub = stream.subscribe({
          next: (e) => events.push(e),
          error: (err) => console.error("metered SSE error:", err),
        });
        await stream.connected(20_000);

        const base = { page: "/metered", user_id: "u", session_id: "s" };
        // Below the bound first, above it second: frames on one connection are
        // strictly ordered behind the same subject, so receiving the high row
        // proves the low row was withheld, not merely late.
        for (const row of [
          { ...base, event_id: lowId, duration_ms: 100 },
          { ...base, event_id: highId, duration_ms: 250 },
        ]) {
          expect((await inserter.from(T.clicks).insert(row)).error).toBeNull();
        }

        await waitForCondition(() => events.some((e) => e.data?.event_id === highId), 10_000);
        expect(events.some((e) => e.data?.event_id === lowId)).toBe(false);
      } finally {
        if (unsub) unsub();
        stream.close();
      }
    });
  });
});
