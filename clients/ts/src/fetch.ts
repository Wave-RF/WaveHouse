// HTTP fetch wrapper with auth, error handling, and cache header extraction.

import type { WaveHouseConfig, QueryResult } from "./types.js";

/** Resolved auth token. */
async function resolveToken(
  token: WaveHouseConfig["token"]
): Promise<string | undefined> {
  if (!token) return undefined;
  if (typeof token === "function") return token();
  return token;
}

/** Make an authenticated request to the WaveHouse API. */
export async function request<T = unknown>(
  config: WaveHouseConfig,
  path: string,
  options: RequestInit = {}
): Promise<{ data: T; headers: Headers }> {
  const fetchFn = config.fetch ?? globalThis.fetch;
  const url = `${config.baseUrl}${path}`;

  const headers = new Headers(options.headers);
  const token = await resolveToken(config.token);
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  if (!headers.has("Content-Type") && options.body) {
    headers.set("Content-Type", "application/json");
  }

  const res = await fetchFn(url, { ...options, headers });

  if (!res.ok) {
    const body = await res.text();
    let message: string;
    try {
      const json = JSON.parse(body);
      message = json.error ?? body;
    } catch {
      message = body;
    }
    throw new WaveHouseError(message, res.status);
  }

  const data = (await res.json()) as T;
  return { data, headers: res.headers };
}

/** Execute a query and return typed QueryResult. */
export async function queryRequest<T = Record<string, unknown>>(
  config: WaveHouseConfig,
  path: string,
  body?: unknown
): Promise<QueryResult<T>> {
  const options: RequestInit = body
    ? { method: "POST", body: JSON.stringify(body) }
    : {};
  const { data, headers } = await request<T[]>(config, path, options);
  return {
    data,
    meta: { cached: headers.get("X-Cache") === "HIT" },
  };
}

/** WaveHouse API error with HTTP status code. */
export class WaveHouseError extends Error {
  constructor(
    message: string,
    public readonly status: number
  ) {
    super(message);
    this.name = "WaveHouseError";
  }
}
