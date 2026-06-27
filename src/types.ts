// ─────────────────────────────────────────────────────────────────────────────
// Types & Interfaces
// ─────────────────────────────────────────────────────────────────────────────

export type JobStatus =
  | "pending"
  | "active"
  | "completed"
  | "failed"
  | "dead"
  | "scheduled"
  | "paused";

export type JobPriority = "critical" | "high" | "normal" | "low";

export const PRIORITY_VALUES: Record<JobPriority, number> = {
  critical: 0,
  high: 1,
  normal: 2,
  low: 3,
};

// ─── Job ─────────────────────────────────────────────────────────────────────

export interface Job<T = unknown> {
  id: string;
  queue: string;
  name: string;
  payload: T;
  status: JobStatus;
  priority: JobPriority;
  attempts: number;
  maxAttempts: number;
  delay: number;            // ms before first run
  backoff: BackoffConfig;
  timeout: number;          // ms — 0 = unlimited
  scheduledAt: Date;
  startedAt: Date | null;
  completedAt: Date | null;
  failedAt: Date | null;
  deadAt: Date | null;
  nextRunAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  progress: number;         // 0–100
  result: unknown;
  error: string | null;
  stackTrace: string | null;
  tags: string[];
  meta: Record<string, unknown>;
  repeatConfig: RepeatConfig | null;
  repeatJobId: string | null;
  ttl: number;              // ms — 0 = forever
  dedupKey: string | null;
}

// ─── Backoff ─────────────────────────────────────────────────────────────────

export type BackoffType = "fixed" | "exponential" | "linear" | "jitter";

export interface BackoffConfig {
  type: BackoffType;
  delay: number;   // base delay ms
  max?: number;    // cap ms
}

// ─── Repeat ──────────────────────────────────────────────────────────────────

export interface RepeatConfig {
  cron?: string;          // cron expression
  every?: number;         // ms interval
  limit?: number;         // total runs (0 = infinite)
  count: number;          // runs so far
  endDate?: Date;
}

// ─── Queue Options ────────────────────────────────────────────────────────────

export interface QueueOptions {
  /** Max concurrent workers for this queue */
  concurrency?: number;
  /** Default max retry attempts */
  defaultAttempts?: number;
  /** Default job timeout ms */
  defaultTimeout?: number;
  /** Default backoff config */
  defaultBackoff?: BackoffConfig;
  /** Default job priority */
  defaultPriority?: JobPriority;
  /** Polling interval ms */
  pollInterval?: number;
  /** Auto-delete completed jobs after ms (0 = keep forever) */
  removeOnComplete?: number;
  /** Auto-delete failed jobs after ms (0 = keep forever) */
  removeOnFail?: number;
  /** Whether the queue starts paused */
  paused?: boolean;
  /** Rate limit: max jobs per window */
  rateLimit?: { max: number; window: number };
  /** Stalled job detection interval ms */
  stalledInterval?: number;
  /** Time before active job is considered stalled ms */
  stalledTimeout?: number;
}

// ─── Add Job Options ──────────────────────────────────────────────────────────

export interface AddJobOptions {
  /** Job name/type for handler routing */
  name?: string;
  /** Priority override */
  priority?: JobPriority;
  /** Delay before first execution ms */
  delay?: number;
  /** Max retry attempts */
  attempts?: number;
  /** Backoff override */
  backoff?: BackoffConfig;
  /** Job timeout ms */
  timeout?: number;
  /** Repeat config */
  repeat?: Omit<RepeatConfig, "count">;
  /** Tags for filtering */
  tags?: string[];
  /** Arbitrary metadata */
  meta?: Record<string, unknown>;
  /** TTL ms — job auto-deleted after this */
  ttl?: number;
  /** Deduplication key — only one job per key at a time */
  dedupKey?: string;
  /** Job ID override (must be unique) */
  jobId?: string;
}

// ─── Job Context ──────────────────────────────────────────────────────────────

export interface JobContext<T = unknown> {
  job: Job<T>;
  /** Update progress 0–100 */
  updateProgress(progress: number): void;
  /** Log a message attached to the job */
  log(message: string, level?: "info" | "warn" | "error"): void;
  /** Extend the job's lock by ms */
  extendLock(ms: number): void;
  /** Move job to another queue */
  moveToQueue(queue: string): void;
}

// ─── Handler ─────────────────────────────────────────────────────────────────

export type JobHandler<T = unknown, R = unknown> = (
  ctx: JobContext<T>
) => Promise<R> | R;

// ─── Events ──────────────────────────────────────────────────────────────────

export type QueueEvent =
  | "job:added"
  | "job:started"
  | "job:completed"
  | "job:failed"
  | "job:retrying"
  | "job:dead"
  | "job:progress"
  | "job:stalled"
  | "queue:paused"
  | "queue:resumed"
  | "queue:drained"
  | "worker:idle"
  | "worker:busy"
  | "error";

export interface QueueEventMap {
  "job:added": Job;
  "job:started": Job;
  "job:completed": Job;
  "job:failed": { job: Job; error: Error };
  "job:retrying": { job: Job; attempt: number };
  "job:dead": Job;
  "job:progress": { job: Job; progress: number };
  "job:stalled": Job;
  "queue:paused": { queue: string };
  "queue:resumed": { queue: string };
  "queue:drained": { queue: string };
  "worker:idle": { workerId: string };
  "worker:busy": { workerId: string; jobId: string };
  "error": Error;
}

// ─── Stats ────────────────────────────────────────────────────────────────────

export interface QueueStats {
  queue: string;
  pending: number;
  active: number;
  completed: number;
  failed: number;
  dead: number;
  scheduled: number;
  paused: number;
  total: number;
  throughput: {
    completed_last_minute: number;
    failed_last_minute: number;
    completed_last_hour: number;
    failed_last_hour: number;
  };
  avgProcessingTime: number | null;
  oldestPendingAge: number | null;
}

// ─── Global Stats ─────────────────────────────────────────────────────────────

export interface GlobalStats {
  queues: QueueStats[];
  total: {
    pending: number;
    active: number;
    completed: number;
    failed: number;
    dead: number;
    total: number;
  };
  db: {
    size: number;
    pageSize: number;
    pageCount: number;
    walSize?: number;
  };
}

// ─── MQ Options ──────────────────────────────────────────────────────────────

export interface SqliteMQOptions {
  /** Path to SQLite database file (default: :memory:) */
  path?: string;
  /** Global default queue options */
  defaultQueueOptions?: Partial<QueueOptions>;
  /** Enable WAL mode (default: true) */
  wal?: boolean;
  /** Busy timeout ms (default: 5000) */
  busyTimeout?: number;
  /** Enable automatic migration */
  migrate?: boolean;
  /** Cleanup interval ms — removes expired jobs (default: 60_000) */
  cleanupInterval?: number;
  /** Enable job log table */
  enableLogs?: boolean;
}

// ─── Log Entry ────────────────────────────────────────────────────────────────

export interface JobLog {
  id: number;
  jobId: string;
  queue: string;
  level: "info" | "warn" | "error";
  message: string;
  createdAt: Date;
}

// ─── Batch Result ────────────────────────────────────────────────────────────

export interface BatchResult {
  added: string[];
  skipped: string[];   // dedup skips
  errors: Array<{ index: number; error: string }>;
}

// ─── Search Filters ──────────────────────────────────────────────────────────

export interface JobFilter {
  queue?: string | string[];
  status?: JobStatus | JobStatus[];
  name?: string;
  tags?: string[];
  priority?: JobPriority;
  createdAfter?: Date;
  createdBefore?: Date;
  limit?: number;
  offset?: number;
  orderBy?: "createdAt" | "scheduledAt" | "priority" | "attempts";
  order?: "asc" | "desc";
}
