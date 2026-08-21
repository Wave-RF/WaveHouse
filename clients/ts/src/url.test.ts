import { describe, expect, it } from "vitest";
import { resolveURL } from "./url.js";

describe("resolveURL", () => {
  it("resolves against a root-hosted base", () => {
    expect(resolveURL("http://localhost:8080", "/v1/query").toString()).toBe(
      "http://localhost:8080/v1/query",
    );
  });

  it("preserves a base path prefix", () => {
    expect(resolveURL("https://app.example.com/api/warehouse", "/v1/query").toString()).toBe(
      "https://app.example.com/api/warehouse/v1/query",
    );
  });

  it("preserves a prefix whether or not the base ends in a slash", () => {
    const withSlash = resolveURL("https://app.example.com/api/warehouse/", "/v1/query").toString();
    const without = resolveURL("https://app.example.com/api/warehouse", "/v1/query").toString();
    expect(withSlash).toBe(without);
    expect(without).toBe("https://app.example.com/api/warehouse/v1/query");
  });

  it("preserves a multi-segment prefix", () => {
    expect(resolveURL("https://example.com/a/b/c", "/v1/ops/pipes").toString()).toBe(
      "https://example.com/a/b/c/v1/ops/pipes",
    );
  });

  it("accepts a path with or without a leading slash", () => {
    expect(resolveURL("https://example.com/api", "v1/ops/schema").toString()).toBe(
      resolveURL("https://example.com/api", "/v1/ops/schema").toString(),
    );
  });

  it("keeps the query string embedded in a path", () => {
    expect(resolveURL("https://example.com/api", "/v1/ingest?table=clicks").toString()).toBe(
      "https://example.com/api/v1/ingest?table=clicks",
    );
  });

  it("appends params after the prefix", () => {
    const url = resolveURL("https://example.com/api", "/v1/ops/dlq/stats", { table: "clicks" });
    expect(url.toString()).toBe("https://example.com/api/v1/ops/dlq/stats?table=clicks");
  });

  it("merges params with a query string already on the path", () => {
    const url = resolveURL("https://example.com/api", "/v1/query?table=clicks", { limit: "10" });
    expect(url.pathname).toBe("/api/v1/query");
    expect(url.searchParams.get("table")).toBe("clicks");
    expect(url.searchParams.get("limit")).toBe("10");
  });

  it("percent-encodes param values", () => {
    const url = resolveURL("https://example.com", "/v1/ops/schema", { table: "a b&c" });
    expect(url.searchParams.get("table")).toBe("a b&c");
    expect(url.toString()).toContain("table=a+b%26c");
  });

  it("drops a query or fragment on the base rather than letting it eat the prefix", () => {
    expect(resolveURL("https://example.com/api?stale=1", "/v1/query").toString()).toBe(
      "https://example.com/api/v1/query",
    );
    expect(resolveURL("https://example.com/api#frag", "/v1/query").toString()).toBe(
      "https://example.com/api/v1/query",
    );
  });

  it("preserves a non-default port", () => {
    expect(resolveURL("https://example.com:8443/api", "/v1/query").toString()).toBe(
      "https://example.com:8443/api/v1/query",
    );
  });

  it("throws on a base with no scheme", () => {
    expect(() => resolveURL("example.com/api", "/v1/query")).toThrow();
  });
});
