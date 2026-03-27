// Live query: combines cached query results with real-time SSE gap-fill
// and smart aggregation updates.

import type {
  WaveHouseConfig,
  StructuredQuery,
  StreamEvent,
  LiveQueryOptions,
  QueryResult,
  AggregationFn,
} from "./types.js";
import type { SSESubscription } from "./sse.js";
import { subscribe } from "./sse.js";
import { queryRequest } from "./fetch.js";
import {
  classifyAggregation,
  initState,
  updateAggregation,
  type IncrementalState,
} from "./aggregations.js";

/** Handle returned by liveQuery. */
export interface LiveQueryHandle<T = Record<string, unknown>> {
  /** Current result (updated in place). */
  readonly result: QueryResult<T>;
  /** Stop the live query and close SSE. */
  close(): void;
}

/**
 * Execute a structured query against the cache, then open an SSE stream
 * to receive new events. Incrementable/decomposable aggregations are
 * updated in real-time; non-incrementable trigger a periodic re-poll.
 *
 * @param config - WaveHouse client config.
 * @param table - Target table name.
 * @param sq - Structured query.
 * @param onChange - Called whenever the result changes.
 * @param options - Live query options.
 * @returns A handle to access current results and close the subscription.
 */
export async function liveQuery<T = Record<string, unknown>>(
  config: WaveHouseConfig,
  table: string,
  sq: StructuredQuery,
  onChange: (result: QueryResult<T>) => void,
  options: LiveQueryOptions = {}
): Promise<LiveQueryHandle<T>> {
  // 1. Initial cached query.
  const result = await queryRequest<T>(
    config,
    `/v1/tables/${encodeURIComponent(table)}/query`,
    sq
  );

  onChange(result);

  // 2. Set up incremental state for aggregations.
  const aggStates: IncrementalState[] = [];
  if (sq.aggregations?.length) {
    for (let i = 0; i < sq.aggregations.length; i++) {
      const agg = sq.aggregations[i];
      const initialRow = result.data[0] as Record<string, unknown> | undefined;
      const alias = agg.alias ?? `${agg.fn}_${agg.column}`;
      const initialVal = initialRow ? Number(initialRow[alias] ?? 0) : 0;
      aggStates.push(initState(agg.fn as AggregationFn, agg.column, initialVal));
    }
  }

  // 3. Determine if polling is needed.
  const needsPoll = sq.aggregations?.some(
    (a) => classifyAggregation(a.fn as AggregationFn) === "poll"
  );
  let pollTimer: ReturnType<typeof setInterval> | undefined;

  if (needsPoll) {
    const interval = options.pollInterval ?? 5000;
    pollTimer = setInterval(async () => {
      try {
        const fresh = await queryRequest<T>(
          config,
          `/v1/tables/${encodeURIComponent(table)}/query`,
          sq
        );
        result.data = fresh.data;
        result.meta = fresh.meta;
        onChange(result);
      } catch {
        // Silently retry on next interval.
      }
    }, interval);
  }

  // 4. SSE subscription for real-time updates.
  const topic = options.topic ?? `ingest.${table}`;
  const since = new Date().toISOString();

  let sseSub: SSESubscription | undefined;

  // Only subscribe to SSE if we have incrementable/decomposable aggregations
  // or if we're streaming raw rows.
  const hasIncremental = aggStates.some(
    (s) => classifyAggregation(s.fn as AggregationFn) !== "poll"
  );
  const isRawRows = !sq.aggregations?.length;

  if (hasIncremental || isRawRows) {
    sseSub = subscribe(config, {
      topic,
      since,
      onEvent(event: StreamEvent) {
        if (isRawRows) {
          // Append new rows to the result set.
          result.data.push(event.data as T);
          result.meta.cached = false;
          onChange(result);
        } else {
          // Update incremental aggregations.
          let changed = false;
          for (const state of aggStates) {
            if (classifyAggregation(state.fn as AggregationFn) !== "poll") {
              updateAggregation(state, event);
              changed = true;
            }
          }
          if (changed && result.data.length > 0) {
            const row = result.data[0] as Record<string, unknown>;
            for (const state of aggStates) {
              const agg = sq.aggregations!.find((a) => a.column === state.column && a.fn === state.fn);
              const alias = agg?.alias ?? `${state.fn}_${state.column}`;
              row[alias] = state.value;
            }
            result.meta.cached = false;
            onChange(result);
          }
        }
      },
    });
  }

  return {
    result,
    close() {
      sseSub?.close();
      if (pollTimer) clearInterval(pollTimer);
    },
  };
}
