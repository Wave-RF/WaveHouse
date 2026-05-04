import { describe, it, expect } from "vitest";
import {
  publicClient,
  viewerClient,
  adminClient,
  makeExpiredJWT,
  isDevMode,
  WH_URL,
} from "./helpers.js";
import { createClient } from "@wavehouse/sdk";

describe("Auth", () => {
  it("health endpoint is accessible without auth", async () => {
    const wh = publicClient();
    const result = await wh.sys.health();
    expect(result.error).toBeNull();
    expect(result.data).toHaveProperty("status");
  });

  it("viewer role can read data", async () => {
    if (isDevMode()) {
      console.log(
        "    ⏭  Skipped: viewer role read (auth not enforced in dev mode)",
      );
      return;
    }

    const wh = viewerClient();
    const result = await wh.from("clicks").fetch({ limit: 1 });
    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
  });

  it("admin role has full access", async () => {
    const wh = adminClient();

    // Admin can list schemas
    const schemas = await wh.schema.list();
    expect(schemas.error).toBeNull();

    // Admin can run raw SQL
    const sql = await wh.sql("SELECT 1 as x");
    expect(sql.error).toBeNull();
    expect(sql.data).toBeInstanceOf(Array);
  });

  it("expired JWT is rejected", async () => {
    if (isDevMode()) {
      console.log(
        "    ⏭  Skipped: expired JWT rejection (auth not enforced in dev mode)",
      );
      return;
    }

    const wh = createClient({
      baseURL: WH_URL,
      auth: () =>
        makeExpiredJWT({
          sub: "expired-user",
          role: "viewer",
          tenant_id: "acme",
        }),
    });

    const result = await wh.from("clicks").fetch({ limit: 1 });
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(401);
  });

  it("invalid JWT is rejected", async () => {
    if (isDevMode()) {
      console.log(
        "    ⏭  Skipped: invalid JWT rejection (auth not enforced in dev mode)",
      );
      return;
    }

    const wh = createClient({
      baseURL: WH_URL,
      auth: () => "not-a-valid-jwt",
    });

    const result = await wh.from("clicks").fetch({ limit: 1 });
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(401);
  });

  it("unauthenticated request is rejected when auth is enabled", async () => {
    if (isDevMode()) {
      console.log(
        "    ⏭  Skipped: unauthenticated rejection (auth not enforced in dev mode)",
      );
      return;
    }

    const wh = publicClient();
    const result = await wh.from("clicks").fetch({ limit: 1 });
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(401);
  });
});
