import { createClient } from "@wavehouse/sdk";
import { describe, expect, it } from "vitest";
import { adminClient, makeExpiredJWT, publicClient, viewerClient, WH_URL } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Auth", () => {
  const T = suiteTables("auth");

  it("health endpoint is accessible without auth", async () => {
    const wh = publicClient();
    const result = await wh.sys.health();
    expect(result.error).toBeNull();
    expect(result.data).toHaveProperty("status");
  });

  it("admin role has full access", async () => {
    const wh = adminClient();

    const schemas = await wh.schema.list();
    expect(schemas.error).toBeNull();

    const sql = await wh.sql("SELECT 1 as x");
    expect(sql.error).toBeNull();
    expect(sql.data).toBeInstanceOf(Array);
  });

  it("viewer role can read data", async () => {
    const wh = viewerClient();
    const result = await wh.from(T.clicks).fetch({ limit: 1 });
    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
  });

  it("expired JWT is rejected", async () => {
    const wh = createClient({
      baseURL: WH_URL,
      auth: () =>
        makeExpiredJWT({
          sub: "expired-user",
          role: "viewer",
          tenant_id: "acme",
        }),
    });

    const result = await wh.from(T.clicks).fetch({ limit: 1 });
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(401);
  });

  it("invalid JWT is rejected", async () => {
    const wh = createClient({
      baseURL: WH_URL,
      auth: () => "not-a-valid-jwt",
    });

    const result = await wh.from(T.clicks).fetch({ limit: 1 });
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(401);
  });

  it("no-token request is forbidden when no default_role grants access", async () => {
    // A request with no token resolves to the policy default_role. The e2e
    // policy sets none, so the roleless request matches nothing and is denied
    // with 403 — not 401, since there's no bad token to report (unlike the
    // expired/invalid cases above, which fail loud with 401).
    const wh = publicClient();
    const result = await wh.from(T.clicks).fetch({ limit: 1 });
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(403);
  });
});
