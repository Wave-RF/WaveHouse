// Smart aggregation classification and incremental update helpers.

import type { AggregationFn, AggregationClass, StreamEvent } from "./types.js";

/**
 * Classify an aggregation function by how it can be updated with new data.
 *
 * - incrementable: Can be updated with a single new value (count, sum, min, max).
 * - decomposable: Can be recomputed from sub-aggregates (avg = sum/count).
 * - poll: Must be fully recomputed (median, quantile, distinct counts).
 */
export function classifyAggregation(fn: AggregationFn): AggregationClass {
  switch (fn) {
    case "count":
    case "sum":
    case "min":
    case "max":
      return "incrementable";
    case "avg":
      return "decomposable";
    default:
      return "poll";
  }
}

/** State tracker for incrementable aggregations. */
export interface IncrementalState {
  fn: AggregationFn;
  column: string;
  value: number;
  count: number; // needed for avg decomposition
  sum: number; // needed for avg decomposition
}

/** Create initial state for an aggregation. */
export function initState(
  fn: AggregationFn,
  column: string,
  initialValue: number
): IncrementalState {
  return {
    fn,
    column,
    value: initialValue,
    count: fn === "count" ? initialValue : 1,
    sum: fn === "sum" || fn === "avg" ? initialValue : 0,
  };
}

/** Update aggregation state with a new streamed event. Returns the new value. */
export function updateAggregation(
  state: IncrementalState,
  event: StreamEvent
): number {
  const raw = event.data[state.column];
  const newVal = typeof raw === "number" ? raw : Number(raw);
  if (isNaN(newVal) && state.fn !== "count") return state.value;

  switch (state.fn) {
    case "count":
      state.count += 1;
      state.value = state.count;
      break;
    case "sum":
      state.sum += newVal;
      state.value = state.sum;
      break;
    case "min":
      state.value = Math.min(state.value, newVal);
      break;
    case "max":
      state.value = Math.max(state.value, newVal);
      break;
    case "avg":
      state.count += 1;
      state.sum += newVal;
      state.value = state.sum / state.count;
      break;
    default:
      // Non-incrementable — value unchanged, caller should poll.
      break;
  }

  return state.value;
}

/** Check if any aggregation in a list requires polling. */
export function requiresPolling(fns: AggregationFn[]): boolean {
  return fns.some((fn) => classifyAggregation(fn) === "poll");
}
