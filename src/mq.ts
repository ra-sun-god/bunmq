import type { StorageAdapter } from "./adapters/adapter.ts";
import { BunSQLAdapter } from "./adapters/bun-sql.ts";
import {
  throughputSQL,
  rateBucketUpsertSQL,
  upsertSQL,
  jsonArrayContainsSQL,
} from "./adapters/schema.ts";
import { MQEventEmitter } from "./events.ts";
import { rowToJob, priorityToInt, generateId } from "./serializer.ts";
import { calcBackoff } from "./backoff.ts";
import { nextCronDate } from "./cron.ts";
import type {
  Job,
  JobStatus,
  JobHandler,
  JobContext,
  AddJobOptions,
  QueueOptions,
  QueueStats,
  GlobalStats,
  BatchResult,
  JobFilter,
  JobLog,
  RepeatConfig,
} from "./types.ts";
import { PRIORITY_VALUES } from "./types.ts";

// ─────────────────────────────────────────────────────────────────────────────
// BunMQ options
// ─────────────────────────────────────────────────────────────────────────────

export interface BunMQOptions {
  /**
   * Storage adapter. Defaults to BunSQLAdapter (in-memory SQLite via bun SQL).
   *
   * Examples:
   *   new BunSQLAdapter()                                     in-memory SQLite
   *   new BunSQLAdapter({ url: "sqlite:///queue.db" })        file SQLite
   *   new BunSQLAdapter({ url: "postgres://user:pw@host/db" }) PostgreSQL
   *   new BunSQLAdapter({ url: "mysql://user:pw@host/db" })   MySQL / MariaDB
   *   new MemoryAdapter()                                     pure in-process
   */
  adapter?: StorageAdapter;
  /** Global default queue options */
  defaultQueueOptions?: Partial<QueueOptions>;
  /** Auto-run schema migration on start (default: true) */
  migrate?: boolean;
  /** Cleanup interval ms — removes expired jobs (default: 60_000) */
  cleanupInterval?: number;
  /** Enable job log table (default: false) */
  enableLogs?: boolean;
  /** Repeat job scheduler poll interval ms (default: 5000) */
  repeatInterval?: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// BunMQ — main class
// ─────────────────────────────────────────────────────────────────────────────

export class BunMQ extends MQEventEmitter {
  readonly adapter: StorageAdapter;
  private readonly opts: Required<BunMQOptions>;

  private handlers     = new Map<string, Map<string, JobHandler>>();
  private queueOpts    = new Map<string, Required<QueueOptions>>();
  private activeWorkers = new Map<string, number>();
  private pollTimers   = new Map<string, Timer>();

  private cleanupTimer: Timer | null = null;
  private stalledTimer: Timer | null = null;
  private repeatTimer:  Timer | null = null;
  private _ready       = false;
  private _closed      = false;
  private _initPromise: Promise<void> | null = null;

  constructor(opts: BunMQOptions = {}) {
    super();
    this.adapter = opts.adapter ?? new BunSQLAdapter();
    this.opts = {
      adapter:             this.adapter,
      defaultQueueOptions: opts.defaultQueueOptions ?? {},
      migrate:             opts.migrate             ?? true,
      cleanupInterval:     opts.cleanupInterval     ?? 60_000,
      enableLogs:          opts.enableLogs           ?? false,
      repeatInterval:      opts.repeatInterval       ?? 5_000,
    };
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  /** Must be called before using the queue (or use `await BunMQ.create(...)`) */
  async connect(): Promise<this> {
    if (this._ready) return this;
    if (this._initPromise) { await this._initPromise; return this; }

    this._initPromise = (async () => {
      await this.adapter.connect();
      if (this.opts.migrate) await this.adapter.migrate(this.opts.enableLogs);
      this._ready = true;
      this._startCleanup();
      this._startStalledDetection();
      this._startRepeatScheduler();
    })();

    await this._initPromise;
    return this;
  }

  /** Convenience factory: creates and connects in one call */
  static async create(opts: BunMQOptions = {}): Promise<BunMQ> {
    const mq = new BunMQ(opts);
    await mq.connect();
    return mq;
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;

    this.stopAll();

    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    if (this.stalledTimer) clearInterval(this.stalledTimer);
    if (this.repeatTimer)  clearInterval(this.repeatTimer);

    await this.adapter.close();
  }

  // ─── Queue Registration ────────────────────────────────────────────────────

  defineQueue(queueName: string, opts: QueueOptions = {}): this {
    const d = this.opts.defaultQueueOptions;
    const merged: Required<QueueOptions> = {
      concurrency:      opts.concurrency      ?? d.concurrency      ?? 1,
      defaultAttempts:  opts.defaultAttempts  ?? d.defaultAttempts  ?? 3,
      defaultTimeout:   opts.defaultTimeout   ?? d.defaultTimeout   ?? 0,
      defaultBackoff:   opts.defaultBackoff   ?? d.defaultBackoff   ?? { type: "exponential", delay: 1000 },
      defaultPriority:  opts.defaultPriority  ?? d.defaultPriority  ?? "normal",
      pollInterval:     opts.pollInterval     ?? d.pollInterval     ?? 1000,
      removeOnComplete: opts.removeOnComplete ?? d.removeOnComplete ?? 0,
      removeOnFail:     opts.removeOnFail     ?? d.removeOnFail     ?? 0,
      paused:           opts.paused           ?? d.paused           ?? false,
      rateLimit:        opts.rateLimit        ?? d.rateLimit        ?? null as unknown as { max: number; window: number },
      stalledInterval:  opts.stalledInterval  ?? d.stalledInterval  ?? 30_000,
      stalledTimeout:   opts.stalledTimeout   ?? d.stalledTimeout   ?? 60_000,
    };
    this.queueOpts.set(queueName, merged);
    if (!this.handlers.has(queueName)) this.handlers.set(queueName, new Map());
    if (merged.paused) this._setPauseState(queueName, true).catch(() => {});
    return this;
  }

  handle<T = unknown, R = unknown>(
    queueName: string,
    jobName: string,
    handler: JobHandler<T, R>
  ): this {
    if (!this.queueOpts.has(queueName)) this.defineQueue(queueName);
    if (!this.handlers.has(queueName)) this.handlers.set(queueName, new Map());
    this.handlers.get(queueName)!.set(jobName, handler as JobHandler);
    return this;
  }

  // ─── Worker Control ────────────────────────────────────────────────────────

  start(queueName: string): this {
    if (this._closed) throw new Error("BunMQ is closed");
    if (!this.queueOpts.has(queueName)) this.defineQueue(queueName);
    if (!this.pollTimers.has(queueName)) this._startPolling(queueName);
    return this;
  }

  stop(queueName: string): this {
    const timer = this.pollTimers.get(queueName);
    if (timer) { clearInterval(timer); this.pollTimers.delete(queueName); }
    return this;
  }

  startAll(): this {
    for (const q of this.queueOpts.keys()) this.start(q);
    return this;
  }

  stopAll(): this {
    for (const q of [...this.pollTimers.keys()]) this.stop(q);
    return this;
  }

  async pause(queueName: string): Promise<this> {
    await this._setPauseState(queueName, true);
    this.stop(queueName);
    this.emit("queue:paused", { queue: queueName });
    return this;
  }

  async resume(queueName: string): Promise<this> {
    await this._setPauseState(queueName, false);
    this.start(queueName);
    this.emit("queue:resumed", { queue: queueName });
    return this;
  }

  // ─── Job Addition ──────────────────────────────────────────────────────────

  async add<T = unknown>(
    queueName: string,
    payload: T,
    opts: AddJobOptions = {}
  ): Promise<Job<T>> {
    this._assertReady();

    const qOpts = this.queueOpts.get(queueName) ?? this._defaultQueueOpts();
    const now   = Date.now();

    // Deduplication
    if (opts.dedupKey) {
      const existing = await this.adapter.get<{ id: string }>(
        `SELECT id FROM bunmq_jobs
         WHERE dedup_key = $key AND queue = $queue
           AND status NOT IN ('completed','dead')
         LIMIT 1`,
        { key: opts.dedupKey, queue: queueName }
      );
      if (existing) return (await this.getJob<T>(existing.id))!;
    }

    const delay      = opts.delay ?? 0;
    const schedAt    = now + delay;
    const status: JobStatus = delay > 0 ? "scheduled" : "pending";
    const repeatCfg: RepeatConfig | null = opts.repeat ? { ...opts.repeat, count: 0 } : null;

    const job: Job<T> = {
      id:           opts.jobId ?? generateId(),
      queue:        queueName,
      name:         opts.name ?? "default",
      payload,
      status,
      priority:     opts.priority ?? qOpts.defaultPriority,
      attempts:     0,
      maxAttempts:  opts.attempts ?? qOpts.defaultAttempts,
      delay,
      backoff:      opts.backoff ?? qOpts.defaultBackoff,
      timeout:      opts.timeout ?? qOpts.defaultTimeout,
      scheduledAt:  new Date(schedAt),
      startedAt:    null,
      completedAt:  null,
      failedAt:     null,
      deadAt:       null,
      nextRunAt:    null,
      createdAt:    new Date(now),
      updatedAt:    new Date(now),
      progress:     0,
      result:       undefined,
      error:        null,
      stackTrace:   null,
      tags:         opts.tags ?? [],
      meta:         opts.meta ?? {},
      repeatConfig: repeatCfg,
      repeatJobId:  (opts as AddJobOptions & { repeatJobId?: string }).repeatJobId ?? null,
      ttl:          opts.ttl ?? 0,
      dedupKey:     opts.dedupKey ?? null,
    };

    await this._insertJob(job);
    this.emit("job:added", job as Job);
    return job;
  }

  async addBulk<T = unknown>(
    queueName: string,
    jobs: Array<{ payload: T; opts?: AddJobOptions }>
  ): Promise<BatchResult> {
    const result: BatchResult = { added: [], skipped: [], errors: [] };
    // Run serially (transaction in SQLite adapter, sequential for others)
    for (let i = 0; i < jobs.length; i++) {
      try {
        const { payload, opts = {} } = jobs[i];
        if (opts.dedupKey) {
          const exists = await this.adapter.get(
            `SELECT id FROM bunmq_jobs WHERE dedup_key = $key AND queue = $queue
             AND status NOT IN ('completed','dead') LIMIT 1`,
            { key: opts.dedupKey, queue: queueName }
          );
          if (exists) { result.skipped.push(opts.dedupKey); continue; }
        }
        const job = await this.add(queueName, payload, opts);
        result.added.push(job.id);
      } catch (err) {
        result.errors.push({ index: i, error: String(err) });
      }
    }
    return result;
  }

  async addRepeat<T = unknown>(
    queueName: string,
    jobName: string,
    payload: T,
    repeatOpts: Omit<RepeatConfig, "count">,
    jobOpts: Omit<AddJobOptions, "repeat"> = {}
  ): Promise<string> {
    this._assertReady();
    const id  = (jobOpts as { jobId?: string }).jobId ?? `repeat:${queueName}:${jobName}:${generateId()}`;
    const now = Date.now();
    let nextRunAt: number;
    if (repeatOpts.cron)       nextRunAt = nextCronDate(repeatOpts.cron).getTime();
    else if (repeatOpts.every) nextRunAt = now + repeatOpts.every;
    else throw new Error("repeat requires either cron or every");

    await this.adapter.run(
      `INSERT INTO bunmq_repeat_jobs
         (id, queue, name, payload, options, repeat_cfg, next_run_at, created_at, updated_at)
       VALUES ($id, $queue, $name, $payload, $options, $repeat_cfg, $nextRunAt, $now, $now)`,
      {
        id, queue: queueName, name: jobName,
        payload:    JSON.stringify(payload),
        options:    JSON.stringify(jobOpts),
        repeat_cfg: JSON.stringify(repeatOpts),
        nextRunAt, now,
      }
    );
    return id;
  }

  async removeRepeat(repeatJobId: string): Promise<boolean> {
    const r = await this.adapter.run(
      "DELETE FROM bunmq_repeat_jobs WHERE id = $id", { id: repeatJobId }
    );
    return r.changes > 0;
  }

  // ─── Job Retrieval ────────────────────────────────────────────────────────

  async getJob<T = unknown>(id: string): Promise<Job<T> | null> {
    const row = await this.adapter.get("SELECT * FROM bunmq_jobs WHERE id = $id", { id });
    return row ? rowToJob(row) as Job<T> : null;
  }

  async findJobs(filter: JobFilter = {}): Promise<Job[]> {
    const conditions: string[] = [];
    const params: Record<string, unknown> = {};

    if (filter.queue) {
      if (Array.isArray(filter.queue)) {
        const keys = filter.queue.map((_, i) => `$q${i}`);
        conditions.push(`queue IN (${keys.join(",")})`);
        filter.queue.forEach((q, i) => { params[`q${i}`] = q; });
      } else {
        conditions.push("queue = $queue");
        params.queue = filter.queue;
      }
    }

    if (filter.status) {
      const statuses = Array.isArray(filter.status) ? filter.status : [filter.status];
      const keys     = statuses.map((_, i) => `$s${i}`);
      conditions.push(`status IN (${keys.join(",")})`);
      statuses.forEach((s, i) => { params[`s${i}`] = s; });
    }

    if (filter.name)     { conditions.push("name = $name");         params.name = filter.name; }
    if (filter.priority) { conditions.push("priority = $priority"); params.priority = PRIORITY_VALUES[filter.priority]; }
    if (filter.createdAfter)  { conditions.push("created_at >= $createdAfter");  params.createdAfter  = filter.createdAfter.getTime(); }
    if (filter.createdBefore) { conditions.push("created_at <= $createdBefore"); params.createdBefore = filter.createdBefore.getTime(); }

    if (filter.tags?.length) {
      const tagConds = filter.tags.map((t, i) => {
        params[`tag${i}`] = t;
        return jsonArrayContainsSQL(this.adapter.dialect, "tags", `tag${i}`);
      });
      conditions.push(`(${tagConds.join(" OR ")})`);
    }

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const orderMap: Record<string, string> = {
      createdAt: "created_at", scheduledAt: "scheduled_at",
      priority: "priority", attempts: "attempts",
    };
    const col    = orderMap[filter.orderBy ?? "createdAt"] ?? "created_at";
    const dir    = (filter.order ?? "asc").toUpperCase();
    params.limit  = filter.limit  ?? 100;
    params.offset = filter.offset ?? 0;

    const rows = await this.adapter.all(
      `SELECT * FROM bunmq_jobs ${where} ORDER BY ${col} ${dir} LIMIT $limit OFFSET $offset`,
      params
    );
    return rows.map(r => rowToJob(r));
  }

  // ─── Job Manipulation ─────────────────────────────────────────────────────

  async retry(jobId: string): Promise<boolean> {
    const now = Date.now();
    const r = await this.adapter.run(
      `UPDATE bunmq_jobs SET status='pending', attempts=0, error=NULL, stack_trace=NULL,
         failed_at=NULL, dead_at=NULL, scheduled_at=$now, updated_at=$now
       WHERE id=$id AND status IN ('failed','dead')`,
      { id: jobId, now }
    );
    return r.changes > 0;
  }

  async promote(jobId: string): Promise<boolean> {
    const now = Date.now();
    const r = await this.adapter.run(
      `UPDATE bunmq_jobs SET status='pending', scheduled_at=$now, updated_at=$now
       WHERE id=$id AND status='scheduled'`,
      { id: jobId, now }
    );
    return r.changes > 0;
  }

  async cancel(jobId: string): Promise<boolean> {
    const r = await this.adapter.run(
      `DELETE FROM bunmq_jobs WHERE id=$id AND status IN ('pending','scheduled','paused')`,
      { id: jobId }
    );
    return r.changes > 0;
  }

  async remove(jobId: string): Promise<boolean> {
    const r = await this.adapter.run("DELETE FROM bunmq_jobs WHERE id=$id", { id: jobId });
    return r.changes > 0;
  }

  async updateMeta(jobId: string, meta: Record<string, unknown>): Promise<boolean> {
    const now      = Date.now();
    const existing = await this.adapter.get<{ meta: unknown }>(
      "SELECT meta FROM bunmq_jobs WHERE id=$id", { id: jobId }
    );
    if (!existing) return false;
    const prev   = typeof existing.meta === "string"
      ? JSON.parse(existing.meta)
      : (existing.meta ?? {});
    const merged = { ...prev, ...meta };
    const r = await this.adapter.run(
      "UPDATE bunmq_jobs SET meta=$meta, updated_at=$now WHERE id=$id",
      { meta: JSON.stringify(merged), now, id: jobId }
    );
    return r.changes > 0;
  }

  // ─── Queue Management ─────────────────────────────────────────────────────

  async clean(queueName: string, status: JobStatus | "all" = "completed", olderThanMs = 0): Promise<number> {
    const cutoff = Date.now() - olderThanMs;
    const r = status === "all"
      ? await this.adapter.run(
          `DELETE FROM bunmq_jobs WHERE queue=$queue AND updated_at<=$cutoff`,
          { queue: queueName, cutoff }
        )
      : await this.adapter.run(
          `DELETE FROM bunmq_jobs WHERE queue=$queue AND status=$status AND updated_at<=$cutoff`,
          { queue: queueName, status, cutoff }
        );
    return r.changes;
  }

  async drain(queueName: string): Promise<number> {
    const r = await this.adapter.run(
      `DELETE FROM bunmq_jobs WHERE queue=$queue AND status IN ('pending','scheduled')`,
      { queue: queueName }
    );
    return r.changes;
  }

  async obliterate(queueName: string): Promise<number> {
    this.stop(queueName);
    const r = await this.adapter.run("DELETE FROM bunmq_jobs WHERE queue=$queue", { queue: queueName });
    await this.adapter.run("DELETE FROM bunmq_queue_pause WHERE queue=$queue", { queue: queueName });
    await this.adapter.run("DELETE FROM bunmq_repeat_jobs WHERE queue=$queue", { queue: queueName });
    this.queueOpts.delete(queueName);
    this.handlers.delete(queueName);
    this.activeWorkers.delete(queueName);
    return r.changes;
  }

  async retryAll(queueName: string, fromStatus: "failed" | "dead" | "all" = "all"): Promise<number> {
    const now      = Date.now();
    const statuses = fromStatus === "all" ? "('failed','dead')" : `('${fromStatus}')`;
    const r = await this.adapter.run(
      `UPDATE bunmq_jobs SET status='pending', attempts=0, error=NULL, stack_trace=NULL,
         failed_at=NULL, dead_at=NULL, scheduled_at=$now, updated_at=$now
       WHERE queue=$queue AND status IN ${statuses}`,
      { queue: queueName, now }
    );
    return r.changes;
  }

  // ─── Stats ────────────────────────────────────────────────────────────────

  async getQueueStats(queueName: string): Promise<QueueStats> {
    const counts = await this.adapter.all<{ status: string; count: number }>(
      "SELECT status, COUNT(*) as count FROM bunmq_jobs WHERE queue=$queue GROUP BY status",
      { queue: queueName }
    );
    const statusMap: Record<string, number> = {};
    let total = 0;
    for (const r of counts) { statusMap[r.status] = r.count; total += Number(r.count); }

    const now     = Date.now();
    const oneMin  = now - 60_000;
    const oneHour = now - 3_600_000;

    const thruRows = await this.adapter.all<{ status: string; count: number; period: string }>(
      throughputSQL(this.adapter.dialect),
      { queue: queueName, oneMin, oneHour }
    );

    const thru = { completed_last_minute: 0, failed_last_minute: 0, completed_last_hour: 0, failed_last_hour: 0 };
    for (const r of thruRows) {
      if (r.status === "completed" && r.period === "minute") thru.completed_last_minute = Number(r.count);
      if (r.status === "failed"    && r.period === "minute") thru.failed_last_minute    = Number(r.count);
      if (r.status === "completed") thru.completed_last_hour += Number(r.count);
      if (r.status === "failed")    thru.failed_last_hour    += Number(r.count);
    }

    const avgRow = await this.adapter.get<{ avg: number | null }>(
      `SELECT AVG(duration) as avg FROM bunmq_metrics
       WHERE queue=$queue AND status='completed' AND recorded_at>=$oneHour`,
      { queue: queueName, oneHour }
    );

    const oldestRow = await this.adapter.get<{ oldest: number | null }>(
      "SELECT MIN(scheduled_at) as oldest FROM bunmq_jobs WHERE queue=$q AND status='pending'",
      { q: queueName }
    );

    return {
      queue:   queueName,
      pending:   Number(statusMap["pending"]   ?? 0),
      active:    Number(statusMap["active"]    ?? 0),
      completed: Number(statusMap["completed"] ?? 0),
      failed:    Number(statusMap["failed"]    ?? 0),
      dead:      Number(statusMap["dead"]      ?? 0),
      scheduled: Number(statusMap["scheduled"] ?? 0),
      paused:    Number(statusMap["paused"]    ?? 0),
      total,
      throughput: thru,
      avgProcessingTime: avgRow?.avg ?? null,
      oldestPendingAge:  oldestRow?.oldest ? now - Number(oldestRow.oldest) : null,
    };
  }

  async getGlobalStats(): Promise<GlobalStats> {
    const qs         = await this.adapter.all<{ queue: string }>("SELECT DISTINCT queue FROM bunmq_jobs");
    const queueStats = await Promise.all(qs.map(r => this.getQueueStats(r.queue)));
    const total      = queueStats.reduce(
      (acc, q) => ({ pending: acc.pending+q.pending, active: acc.active+q.active,
                     completed: acc.completed+q.completed, failed: acc.failed+q.failed,
                     dead: acc.dead+q.dead, total: acc.total+q.total }),
      { pending: 0, active: 0, completed: 0, failed: 0, dead: 0, total: 0 }
    );
    // PRAGMA page_size/page_count only meaningful for SQLite
    const isSQLite = this.adapter.dialect === "sqlite";
    const dbInfo   = isSQLite ? await this.adapter.get<{ page_size: number }>("PRAGMA page_size")   : null;
    const dbCount  = isSQLite ? await this.adapter.get<{ page_count: number }>("PRAGMA page_count") : null;
    return {
      queues: queueStats, total,
      db: { size: (dbInfo?.page_size ?? 0) * (dbCount?.page_count ?? 0),
            pageSize: dbInfo?.page_size ?? 0, pageCount: dbCount?.page_count ?? 0 },
    };
  }

  async getJobLogs(jobId: string): Promise<JobLog[]> {
    if (!this.opts.enableLogs) return [];
    const rows = await this.adapter.all(
      "SELECT * FROM bunmq_job_logs WHERE job_id=$jobId ORDER BY created_at ASC",
      { jobId }
    );
    return rows.map((r, i) => ({
      id:        Number(r.id ?? i),
      jobId:     String(r.job_id),
      queue:     String(r.queue),
      level:     r.level as "info"|"warn"|"error",
      message:   String(r.message),
      createdAt: new Date(Number(r.created_at)),
    }));
  }

  // ─── Wait helpers ─────────────────────────────────────────────────────────

  waitForJob(jobId: string, timeoutMs = 60_000): Promise<Job> {
    return new Promise((resolve, reject) => {
      const start = Date.now();
      const poll  = setInterval(async () => {
        try {
          const job = await this.getJob(jobId);
          if (!job) { clearInterval(poll); reject(new Error(`Job ${jobId} not found`)); return; }
          if (["completed","dead"].includes(job.status)) { clearInterval(poll); resolve(job); return; }
          if (Date.now() - start > timeoutMs) { clearInterval(poll); reject(new Error(`Timeout waiting for job ${jobId}`)); }
        } catch (err) { clearInterval(poll); reject(err); }
      }, 100);
    });
  }

  waitForDrain(queueName: string, timeoutMs = 60_000): Promise<void> {
    return new Promise((resolve, reject) => {
      const start = Date.now();
      const check = async () => {
        const row = await this.adapter.get<{ c: number }>(
          `SELECT COUNT(*) as c FROM bunmq_jobs
           WHERE queue=$q AND status IN ('pending','active','scheduled')`,
          { q: queueName }
        );
        if (Number(row?.c ?? 0) === 0) return resolve();
        if (Date.now() - start > timeoutMs) return reject(new Error("Drain timeout"));
        setTimeout(check, 200);
      };
      check();
    });
  }

  // ─── Internal: Poll Loop ──────────────────────────────────────────────────

  private _startPolling(queueName: string): void {
    const qOpts = this.queueOpts.get(queueName)!;
    const timer = setInterval(() => {
      this._poll(queueName).catch(err =>
        this.emit("error", err instanceof Error ? err : new Error(String(err)))
      );
    }, qOpts.pollInterval);
    this.pollTimers.set(queueName, timer);
    this._poll(queueName).catch(() => {});
  }

  private async _poll(queueName: string): Promise<void> {
    if (this._closed) return;
    if (await this._isQueuePaused(queueName)) return;

    const qOpts = this.queueOpts.get(queueName);
    if (!qOpts) return;

    const active = this.activeWorkers.get(queueName) ?? 0;
    if (active >= qOpts.concurrency) return;
    const slots = qOpts.concurrency - active;

    if (qOpts.rateLimit) {
      const { max, window } = qOpts.rateLimit;
      const windowKey = Math.floor(Date.now() / window);
      const row = await this.adapter.get<{ count: number }>(
        "SELECT count FROM bunmq_rate_buckets WHERE queue=$q AND window_key=$wk",
        { q: queueName, wk: windowKey }
      );
      if (row && Number(row.count) >= max) return;
    }

    const now  = Date.now();
    const jobs = await this.adapter.transaction(async (tx) => {
      // Promote scheduled → pending
      await tx.run(
        `UPDATE bunmq_jobs SET status='pending', updated_at=$now
         WHERE queue=$queue AND status='scheduled' AND scheduled_at<=$now`,
        { queue: queueName, now }
      );

      const rows = await tx.all(
        `SELECT * FROM bunmq_jobs
         WHERE queue=$queue AND status='pending'
         ORDER BY priority ASC, scheduled_at ASC
         LIMIT $limit`,
        { queue: queueName, limit: slots }
      );

      if (!rows.length) return [];

      const lockUntil = now + (qOpts.stalledTimeout ?? 60_000);
      for (const row of rows) {
        const id = String(row.id);
        await tx.run(
          `UPDATE bunmq_jobs SET status='active', started_at=$now, lock_until=$lockUntil, updated_at=$now
           WHERE id=$id`,
          { now, lockUntil, id }
        );
      }

      return rows.map(r => rowToJob(r));
    });

    for (const job of jobs) {
      this._incrementWorkers(queueName);
      this._processJob(job, qOpts).finally(() => this._decrementWorkers(queueName));
    }
  }

  private async _processJob(job: Job, qOpts: Required<QueueOptions>): Promise<void> {
    const queue    = job.queue;
    const handlers = this.handlers.get(queue);
    const handler  = handlers?.get(job.name) ?? handlers?.get("*");

    this.emit("job:started", job);

    if (!handler) {
      await this._failJob(job, new Error(`No handler for "${job.name}" in queue "${queue}"`), qOpts);
      return;
    }

    let movedQueue: string | null = null;

    const ctx: JobContext = {
      job,
      updateProgress: async (progress: number) => {
        const pp = Math.max(0, Math.min(100, progress));
        await this.adapter.run(
          "UPDATE bunmq_jobs SET progress=$p, updated_at=$now WHERE id=$id",
          { p: pp, now: Date.now(), id: job.id }
        );
        job.progress = pp;
        this.emit("job:progress", { job, progress: pp });
      },
      log: async (message: string, level = "info") => {
        if (!this.opts.enableLogs) return;
        await this.adapter.run(
          "INSERT INTO bunmq_job_logs (job_id, queue, level, message, created_at) VALUES ($jobId,$queue,$level,$msg,$now)",
          { jobId: job.id, queue: job.queue, level, msg: message, now: Date.now() }
        );
      },
      extendLock: async (ms: number) => {
        await this.adapter.run(
          "UPDATE bunmq_jobs SET lock_until=$lock WHERE id=$id",
          { lock: Date.now() + ms, id: job.id }
        );
      },
      moveToQueue: async (targetQueue: string) => {
        await this.adapter.run(
          "UPDATE bunmq_jobs SET queue=$q, status='pending', updated_at=$now WHERE id=$id",
          { q: targetQueue, now: Date.now(), id: job.id }
        );
        movedQueue = targetQueue;
        job.queue = targetQueue;
      },
    };

    try {
      let resultPromise = Promise.resolve(handler(ctx));
      if (qOpts.defaultTimeout > 0) {
        const timeoutP = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Job timed out after ${qOpts.defaultTimeout}ms`)), qOpts.defaultTimeout)
        );
        resultPromise = Promise.race([resultPromise, timeoutP]) as Promise<unknown>;
      }

      const result     = await resultPromise;

      // If job was moved to another queue mid-handler, leave it pending there
      if (movedQueue) return;

      const finishedAt = Date.now();
      const duration   = finishedAt - (job.startedAt?.getTime() ?? finishedAt);

      await this.adapter.run(
        `UPDATE bunmq_jobs SET status='completed', completed_at=$now, progress=100,
           result=$result, updated_at=$now WHERE id=$id`,
        { now: finishedAt, result: JSON.stringify(result), id: job.id }
      );
      await this.adapter.run(
        "INSERT INTO bunmq_metrics (queue,status,duration,recorded_at) VALUES ($queue,'completed',$duration,$now)",
        { queue, duration, now: finishedAt }
      );

      if (qOpts.removeOnComplete > 0) {
        setTimeout(async () => {
          await this.adapter.run(
            "DELETE FROM bunmq_jobs WHERE id=$id AND status='completed'", { id: job.id }
          );
        }, qOpts.removeOnComplete);
      }

      if (qOpts.rateLimit) {
        const wk = Math.floor(finishedAt / qOpts.rateLimit.window);
        await this.adapter.run(rateBucketUpsertSQL(this.adapter.dialect), { q: queue, wk });
      }

      job.status = "completed"; job.completedAt = new Date(finishedAt); job.result = result;
      this.emit("job:completed", job);
      await this._checkDrained(queue);

    } catch (err) {
      await this._failJob(job, err instanceof Error ? err : new Error(String(err)), qOpts);
    }
  }

  private async _failJob(job: Job, error: Error, qOpts: Required<QueueOptions>): Promise<void> {
    const now      = Date.now();
    const attempt  = job.attempts + 1;
    const canRetry = attempt < job.maxAttempts;

    if (canRetry) {
      const backoffMs = calcBackoff(job.backoff, attempt);
      const nextRun   = now + backoffMs;
      await this.adapter.run(
        `UPDATE bunmq_jobs SET status='pending', attempts=$attempt, error=$error, stack_trace=$stack,
           failed_at=$now, scheduled_at=$nextRun, updated_at=$now WHERE id=$id`,
        { attempt, error: error.message, stack: error.stack ?? null, now, nextRun, id: job.id }
      );
      job.attempts = attempt; job.error = error.message;
      this.emit("job:retrying", { job, attempt });
    } else {
      await this.adapter.run(
        `UPDATE bunmq_jobs SET status='dead', attempts=$attempt, error=$error, stack_trace=$stack,
           failed_at=$now, dead_at=$now, updated_at=$now WHERE id=$id`,
        { attempt, error: error.message, stack: error.stack ?? null, now, id: job.id }
      );
      await this.adapter.run(
        "INSERT INTO bunmq_metrics (queue,status,recorded_at) VALUES ($queue,'failed',$now)",
        { queue: job.queue, now }
      );
      if (qOpts.removeOnFail > 0) {
        setTimeout(async () => {
          await this.adapter.run(
            "DELETE FROM bunmq_jobs WHERE id=$id AND status='dead'", { id: job.id }
          );
        }, qOpts.removeOnFail);
      }
      job.status = "dead"; job.deadAt = new Date(now);
      this.emit("job:dead", job);
      this.emit("job:failed", { job, error });
    }
  }

  // ─── Background tasks ─────────────────────────────────────────────────────

  private _startStalledDetection(): void {
    this.stalledTimer = setInterval(async () => {
      const now     = Date.now();
      const stalled = await this.adapter.all(
        "SELECT * FROM bunmq_jobs WHERE status='active' AND lock_until<$now", { now }
      );
      for (const row of stalled) {
        const job = rowToJob(row);
        this.emit("job:stalled", job);
        await this.adapter.run(
          "UPDATE bunmq_jobs SET status='pending', started_at=NULL, updated_at=$now WHERE id=$id",
          { now, id: job.id }
        );
      }
    }, 30_000);
  }

  private _startRepeatScheduler(): void {
    this.repeatTimer = setInterval(async () => {
      const now = Date.now();
      const due = await this.adapter.all(
        "SELECT * FROM bunmq_repeat_jobs WHERE next_run_at<=$now AND paused=0", { now }
      );
      for (const row of due) {
        const cfg     = typeof row.repeat_cfg === "string"
          ? JSON.parse(row.repeat_cfg) : (row.repeat_cfg ?? {}) as Omit<RepeatConfig, "count">;
        const opts    = typeof row.options === "string"
          ? JSON.parse(row.options) : (row.options ?? {}) as AddJobOptions;
        const payload = typeof row.payload === "string"
          ? JSON.parse(row.payload) : row.payload;

        if (cfg.limit && Number(row.run_count ?? 0) >= cfg.limit) {
          await this.adapter.run("DELETE FROM bunmq_repeat_jobs WHERE id=$id", { id: row.id }); continue;
        }
        if (cfg.endDate && new Date(cfg.endDate) < new Date(now)) {
          await this.adapter.run("DELETE FROM bunmq_repeat_jobs WHERE id=$id", { id: row.id }); continue;
        }

        await this.add(String(row.queue), payload, {
          ...opts, name: String(row.name), repeatJobId: String(row.id),
        } as AddJobOptions & { repeatJobId: string });

        let nextRunAt: number;
        if (cfg.cron)        nextRunAt = nextCronDate(cfg.cron, new Date(now)).getTime();
        else if (cfg.every)  nextRunAt = now + cfg.every;
        else continue;

        await this.adapter.run(
          `UPDATE bunmq_repeat_jobs SET next_run_at=$next, last_run_at=$now,
             run_count=run_count+1, updated_at=$now WHERE id=$id`,
          { next: nextRunAt, now, id: row.id }
        );
      }
    }, this.opts.repeatInterval);
  }

  private _startCleanup(): void {
    this.cleanupTimer = setInterval(async () => {
      const now = Date.now();
      await this.adapter.run(
        "DELETE FROM bunmq_jobs WHERE ttl>0 AND (created_at+ttl)<$now", { now }
      );
      await this.adapter.run(
        "DELETE FROM bunmq_metrics WHERE recorded_at<$cutoff", { cutoff: now - 86_400_000 }
      );
      if (this.opts.enableLogs) {
        await this.adapter.run(
          "DELETE FROM bunmq_job_logs WHERE created_at<$cutoff", { cutoff: now - 7 * 86_400_000 }
        );
      }
    }, this.opts.cleanupInterval);
  }

  // ─── Internal helpers ─────────────────────────────────────────────────────

  private async _insertJob(job: Job): Promise<void> {
    await this.adapter.run(
      `INSERT INTO bunmq_jobs (
         id, queue, name, payload, status, priority,
         attempts, max_attempts, delay, backoff, timeout,
         scheduled_at, created_at, updated_at,
         tags, meta, repeat_config, repeat_job_id,
         ttl, dedup_key, progress
       ) VALUES (
         $id, $queue, $name, $payload, $status, $priority,
         $attempts, $maxAttempts, $delay, $backoff, $timeout,
         $scheduledAt, $createdAt, $updatedAt,
         $tags, $meta, $repeatConfig, $repeatJobId,
         $ttl, $dedupKey, 0
       )`,
      {
        id: job.id, queue: job.queue, name: job.name,
        payload:     JSON.stringify(job.payload),
        status:      job.status,
        priority:    priorityToInt(job.priority),
        attempts:    job.attempts,
        maxAttempts: job.maxAttempts,
        delay:       job.delay,
        backoff:     JSON.stringify(job.backoff),
        timeout:     job.timeout,
        scheduledAt: job.scheduledAt.getTime(),
        createdAt:   job.createdAt.getTime(),
        updatedAt:   job.updatedAt.getTime(),
        tags:        JSON.stringify(job.tags),
        meta:        JSON.stringify(job.meta),
        repeatConfig: job.repeatConfig ? JSON.stringify(job.repeatConfig) : null,
        repeatJobId:  job.repeatJobId,
        ttl:          job.ttl,
        dedupKey:     job.dedupKey,
      }
    );
  }

  private async _isQueuePaused(queueName: string): Promise<boolean> {
    const row = await this.adapter.get<{ paused: number }>(
      "SELECT paused FROM bunmq_queue_pause WHERE queue=$q", { q: queueName }
    );
    return Number(row?.paused ?? 0) === 1;
  }

  private async _setPauseState(queueName: string, paused: boolean): Promise<void> {
    await this.adapter.run(
      upsertSQL(
        this.adapter.dialect,
        "bunmq_queue_pause",
        ["queue", "paused", "updated_at"],
        ["q", "p", "now"],
        "queue",
        ["paused", "updated_at"],
        ["p", "now"]
      ),
      { q: queueName, p: paused ? 1 : 0, now: Date.now() }
    );
  }

  private _incrementWorkers(q: string) { this.activeWorkers.set(q, (this.activeWorkers.get(q) ?? 0) + 1); }
  private _decrementWorkers(q: string) {
    const c = Math.max(0, (this.activeWorkers.get(q) ?? 0) - 1);
    this.activeWorkers.set(q, c);
    if (c === 0) this.emit("worker:idle", { workerId: q });
  }
  private async _checkDrained(q: string): Promise<void> {
    const row = await this.adapter.get<{ c: number }>(
      `SELECT COUNT(*) as c FROM bunmq_jobs WHERE queue=$q AND status IN ('pending','active','scheduled')`,
      { q }
    );
    if (Number(row?.c ?? 0) === 0) this.emit("queue:drained", { queue: q });
  }
  private _assertReady(): void {
    if (!this._ready) throw new Error("BunMQ not connected. Call await mq.connect() first.");
  }
  private _defaultQueueOpts(): Required<QueueOptions> {
    return {
      concurrency: 1, defaultAttempts: 3, defaultTimeout: 0,
      defaultBackoff: { type: "exponential", delay: 1000 },
      defaultPriority: "normal", pollInterval: 1000,
      removeOnComplete: 0, removeOnFail: 0, paused: false,
      rateLimit: null as unknown as { max: number; window: number },
      stalledInterval: 30_000, stalledTimeout: 60_000,
    };
  }
}
