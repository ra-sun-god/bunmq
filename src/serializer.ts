import type { Job, JobPriority, JobStatus, BackoffConfig, RepeatConfig } from "./types.ts";
import { PRIORITY_VALUES } from "./types.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Row ↔ Job mappers
// ─────────────────────────────────────────────────────────────────────────────

type Row = Record<string, unknown>;

const PRIORITY_NAMES: Record<number, JobPriority> = {
  0: "critical",
  1: "high",
  2: "normal",
  3: "low",
};

function parseDate(v: unknown): Date | null {
  if (v == null) return null;
  return new Date(v as number);
}

function parseJSON<T>(v: unknown, fallback: T): T {
  if (v == null) return fallback;
  // Already parsed (e.g. MemoryAdapter stores objects natively)
  if (typeof v !== "string") return v as T;
  try {
    return JSON.parse(v) as T;
  } catch {
    return fallback;
  }
}

export function rowToJob(row: Row): Job {
  return {
    id:           row.id as string,
    queue:        row.queue as string,
    name:         row.name as string,
    payload:      parseJSON(row.payload, {}),
    status:       row.status as JobStatus,
    priority:     PRIORITY_NAMES[row.priority as number] ?? "normal",
    attempts:     row.attempts as number,
    maxAttempts:  row.max_attempts as number,
    delay:        row.delay as number,
    backoff:      parseJSON<BackoffConfig>(row.backoff, { type: "exponential", delay: 1000 }),
    timeout:      row.timeout as number,
    scheduledAt:  new Date(row.scheduled_at as number),
    startedAt:    parseDate(row.started_at),
    completedAt:  parseDate(row.completed_at),
    failedAt:     parseDate(row.failed_at),
    deadAt:       parseDate(row.dead_at),
    nextRunAt:    parseDate(row.next_run_at),
    createdAt:    new Date(row.created_at as number),
    updatedAt:    new Date(row.updated_at as number),
    progress:     row.progress as number,
    result:       parseJSON(row.result, undefined),
    error:        (row.error as string) ?? null,
    stackTrace:   (row.stack_trace as string) ?? null,
    tags:         parseJSON<string[]>(row.tags, []),
    meta:         parseJSON<Record<string, unknown>>(row.meta, {}),
    repeatConfig: parseJSON<RepeatConfig | null>(row.repeat_config, null),
    repeatJobId:  (row.repeat_job_id as string) ?? null,
    ttl:          row.ttl as number,
    dedupKey:     (row.dedup_key as string) ?? null,
  };
}

export function priorityToInt(p: JobPriority | undefined): number {
  return PRIORITY_VALUES[p ?? "normal"];
}

export function generateId(): string {
  // High-entropy ID: timestamp (ms) + random bytes
  const ts  = Date.now().toString(36);
  const rnd = Math.random().toString(36).slice(2, 9);
  const rnd2 = Math.random().toString(36).slice(2, 5);
  return `${ts}${rnd}${rnd2}`;
}
