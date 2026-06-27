import type { BackoffConfig } from "./types.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Backoff strategies
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Calculate the next retry delay in ms given the backoff config and attempt number.
 * attempt is 1-based (first retry = attempt 1).
 */
export function calcBackoff(config: BackoffConfig, attempt: number): number {
  const { type, delay, max } = config;
  const cap = max ?? Infinity;

  let ms: number;

  switch (type) {
    case "fixed":
      ms = delay;
      break;

    case "exponential":
      // 2^(attempt-1) * delay, capped
      ms = Math.pow(2, attempt - 1) * delay;
      break;

    case "linear":
      // attempt * delay
      ms = attempt * delay;
      break;

    case "jitter": {
      // Full jitter: random between 0 and exponential cap
      const base = Math.pow(2, attempt - 1) * delay;
      const capped = Math.min(base, cap);
      ms = Math.random() * capped;
      break;
    }

    default:
      ms = delay;
  }

  return Math.min(ms, cap);
}
