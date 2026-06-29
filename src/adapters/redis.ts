import { RedisClient } from "bun";
import type { StorageAdapter, AdapterRow, MutationResult } from "./adapter.ts";

// ─────────────────────────────────────────────────────────────────────────────
// BunRedisAdapter — message queue storage using Bun's built-in RedisClient
//
// Because Redis is a key-value / data-structure store and not a relational DB,
// the SQL-style queries used by the MQ core are handled by a pattern-matching
// shim. Each "table" maps to a Redis data structure:
//
//   bunmq_jobs          → Hash  {prefix}:jobs:{id}
//   queue/status index  → ZSet  {prefix}:q:{queue}:{status}  score=priority+scheduledAt
//   dedup index         → String {prefix}:dedup:{key} → job id
//   bunmq_repeat_jobs   → Hash  {prefix}:repeat:{id}
//   repeat next-run     → ZSet  {prefix}:repeats  score=next_run_at
//   bunmq_queue_pause   → Hash  {prefix}:pause  field=queue value=0|1
//   bunmq_rate_buckets  → String {prefix}:rl:{queue}:{windowKey}  (with TTL)
//   bunmq_metrics       → List  {prefix}:metrics:{queue}  (capped at 10k)
//   bunmq_job_logs      → List  {prefix}:logs:{jobId}     (capped at 1k)
//   bunmq_meta          → Hash  {prefix}:meta
// ─────────────────────────────────────────────────────────────────────────────

export interface BunRedisOptions {
  /** Redis connection URL (default: "redis://localhost:6379") */
  url?: string;
  /** Key prefix (default: "bunmq") */
  prefix?: string;
  /**
   * Stale metric TTL seconds (default: 90_000 = 25 hours).
   * Metrics lists are capped at 10k entries; this is a belt-and-suspenders TTL.
   */
  metricsTTL?: number;
}

// ─── Internal types ────────────────────────────────────────────────────────────

type JobRow = Record<string, unknown>;

interface MetricEntry {
  status: string;
  duration: number | null;
  recorded_at: number;
}

interface LogEntry {
  id?: number;
  job_id: string;
  queue: string;
  level: string;
  message: string;
  created_at: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// BunRedisAdapter
// ─────────────────────────────────────────────────────────────────────────────

export class BunRedisAdapter implements StorageAdapter {
  readonly dialect = "memory" as const; // closest SQL dialect for schema helpers
  private _redis!: RedisClient;
  private readonly _url: string;
  readonly prefix: string;
  private readonly _metricsTTL: number;

  constructor(opts: BunRedisOptions = {}) {
    this._url        = opts.url        ?? "redis://localhost:6379";
    this.prefix      = opts.prefix     ?? "bunmq";
    this._metricsTTL = opts.metricsTTL ?? 90_000;
  }

  // ─── Keys ────────────────────────────────────────────────────────────────

  private k = {
    job:     (id: string)             => `${this.prefix}:jobs:${id}`,
    queue:   (q: string, s: string)   => `${this.prefix}:q:${q}:${s}`,
    dedup:   (key: string)            => `${this.prefix}:dedup:${key}`,
    repeat:  (id: string)             => `${this.prefix}:repeat:${id}`,
    repeats: ()                        => `${this.prefix}:repeats`,
    pause:   ()                        => `${this.prefix}:pause`,
    rl:      (q: string, wk: unknown) => `${this.prefix}:rl:${q}:${wk}`,
    metrics: (q: string)              => `${this.prefix}:metrics:${q}`,
    logs:    (jobId: string)          => `${this.prefix}:logs:${jobId}`,
    meta:    ()                        => `${this.prefix}:meta`,
    queues:  ()                        => `${this.prefix}:queues`,
  };

  /** Encodes priority+scheduledAt into a single sort score */
  private score(priority: number, scheduledAt: number): number {
    return priority * 1e13 + scheduledAt;
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    this._redis = new RedisClient(this._url);
  }

  async close(): Promise<void> {
    this._redis?.close();
  }

  async migrate(_enableLogs: boolean): Promise<void> {
    await this._redis.hset(this.k.meta(), { schema_version: "1" });
  }

  // ─── transaction — best-effort sequential for Redis ──────────────────────

  async transaction<T>(fn: (adapter: StorageAdapter) => Promise<T>): Promise<T> {
    return fn(this);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // run() — handles INSERT / UPDATE / DELETE patterns
  // ─────────────────────────────────────────────────────────────────────────

  async run(sql: string, params: Record<string, unknown> = {}): Promise<MutationResult> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const s = sql.trim().replace(/\s+/g, " ");

    // ── INSERT bunmq_jobs ─────────────────────────────────────────────────
    if (/INSERT (?:OR IGNORE )?INTO bunmq_jobs/i.test(s)) {
      return this._insertJob(s, params);
    }

    // ── UPDATE bunmq_jobs ─────────────────────────────────────────────────
    if (/UPDATE bunmq_jobs/i.test(s)) {
      return this._updateJob(s, params);
    }

    // ── DELETE FROM bunmq_jobs ────────────────────────────────────────────
    if (/DELETE FROM bunmq_jobs/i.test(s)) {
      return this._deleteJobs(s, params);
    }

    // ── Rate bucket upsert ────────────────────────────────────────────────
    if (/bunmq_rate_buckets/i.test(s)) {
      const q  = String(p("q")  ?? "");
      const wk = String(p("wk") ?? "");
      const key = this.k.rl(q, wk);
      await this._redis.incr(key);
      await this._redis.expire(key, 3_600);
      return { changes: 1 };
    }

    // ── Queue pause upsert ────────────────────────────────────────────────
    if (/bunmq_queue_pause/i.test(s)) {
      const q      = String(p("q") ?? "");
      const paused = String(p("p") ?? "0");
      await this._redis.hset(this.k.pause(), { [q]: paused });
      return { changes: 1 };
    }

    // ── Insert metric ─────────────────────────────────────────────────────
    if (/bunmq_metrics/i.test(s) && /INSERT/i.test(s)) {
      const queue = String(p("queue") ?? "");
      const entry: MetricEntry = {
        status:      String(p("status") ?? ""),
        duration:    p("duration") != null ? Number(p("duration")) : null,
        recorded_at: Number(p("now") ?? Date.now()),
      };
      await this._redis.lpush(this.k.metrics(queue), JSON.stringify(entry));
      await this._redis.ltrim(this.k.metrics(queue), 0, 9_999);
      return { changes: 1 };
    }

    // ── Insert job log ────────────────────────────────────────────────────
    if (/bunmq_job_logs/i.test(s) && /INSERT/i.test(s)) {
      const jobId = String(p("jobId") ?? "");
      const entry: LogEntry = {
        job_id:     jobId,
        queue:      String(p("queue")  ?? ""),
        level:      String(p("level")  ?? "info"),
        message:    String(p("msg")    ?? ""),
        created_at: Number(p("now") ?? Date.now()),
      };
      await this._redis.lpush(this.k.logs(jobId), JSON.stringify(entry));
      await this._redis.ltrim(this.k.logs(jobId), 0, 999);
      return { changes: 1 };
    }

    // ── Repeat jobs ───────────────────────────────────────────────────────
    if (/bunmq_repeat_jobs/i.test(s)) {
      return this._handleRepeatJob(s, params);
    }

    // ── Meta ─────────────────────────────────────────────────────────────
    if (/bunmq_meta/i.test(s)) {
      const key = String(p("key") ?? "");
      const val = String(p("value") ?? "");
      const exists = await this._redis.hexists(this.k.meta(), key);
      if (!exists) await this._redis.hset(this.k.meta(), { [key]: val });
      return { changes: 1 };
    }

    // ── DDL / PRAGMA — no-op ──────────────────────────────────────────────
    return { changes: 0 };
  }

  // ─────────────────────────────────────────────────────────────────────────
  // get() / all() — handles SELECT patterns
  // ─────────────────────────────────────────────────────────────────────────

  async get<T extends AdapterRow = AdapterRow>(
    sql: string,
    params: Record<string, unknown> = {}
  ): Promise<T | null> {
    const rows = await this.all<T>(sql, params);
    return rows[0] ?? null;
  }

  async all<T extends AdapterRow = AdapterRow>(
    sql: string,
    params: Record<string, unknown> = {}
  ): Promise<T[]> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const s = sql.trim().replace(/\s+/g, " ");

    // ── SELECT * FROM bunmq_jobs WHERE id=$id ────────────────────────────
    if (/SELECT .+ FROM bunmq_jobs WHERE id\s*=\s*\?/i.test(s) ||
        /SELECT .+ FROM bunmq_jobs WHERE id\s*=\s*\$id/i.test(s)) {
      const id  = String(p("id") ?? "");
      const row = await this._getJobById(id);
      return row ? [row as unknown as T] : [];
    }

    // ── Dedup check ───────────────────────────────────────────────────────
    if (/SELECT id FROM bunmq_jobs WHERE dedup_key/i.test(s)) {
      const key   = String(p("key") ?? "");
      const queue = String(p("queue") ?? "");
      const id    = await this._redis.get(this.k.dedup(key));
      if (!id) return [];
      // Verify the job is still active in this queue
      const job = await this._getJobById(id);
      if (!job || String(job.queue) !== queue) return [];
      const active = ["pending","active","scheduled","paused","failed"];
      if (!active.includes(String(job.status))) return [];
      return [{ id } as unknown as T];
    }

    // ── Pending jobs for poll loop ────────────────────────────────────────
    if (/SELECT \* FROM bunmq_jobs.*WHERE.*queue.*status.*pending/i.test(s)) {
      return (await this._getPendingJobs(params)) as unknown as T[];
    }

    // ── Stalled jobs ──────────────────────────────────────────────────────
    if (/SELECT \* FROM bunmq_jobs WHERE status='active' AND lock_until/i.test(s)) {
      const now = Number(p("now") ?? 0);
      return (await this._getStalledJobs(now)) as unknown as T[];
    }

    // ── Status counts ─────────────────────────────────────────────────────
    if (/SELECT status, COUNT/i.test(s)) {
      const queue = String(p("queue") ?? "");
      return (await this._countByStatus(queue)) as unknown as T[];
    }

    // ── Distinct queues ───────────────────────────────────────────────────
    if (/SELECT DISTINCT queue FROM bunmq_jobs/i.test(s)) {
      const queueKeys = await this._redis.keys(`${this.prefix}:q:*:pending`);
      const queues = [...new Set(
        queueKeys.map(k => k.split(":")[2]).filter(Boolean)
      )];
      return queues.map(q => ({ queue: q })) as unknown as T[];
    }

    // ── findJobs (filtered SELECT *) ──────────────────────────────────────
    if (/SELECT \* FROM bunmq_jobs/i.test(s)) {
      return (await this._filterJobs(s, params)) as unknown as T[];
    }

    // ── Due repeat jobs ───────────────────────────────────────────────────
    if (/SELECT \* FROM bunmq_repeat_jobs.*WHERE next_run_at/i.test(s)) {
      const now = Number(p("now") ?? 0);
      return (await this._getDueRepeatJobs(now)) as unknown as T[];
    }

    // ── Rate bucket count ─────────────────────────────────────────────────
    if (/SELECT count FROM bunmq_rate_buckets/i.test(s)) {
      const q  = String(p("q")  ?? "");
      const wk = String(p("wk") ?? "");
      const v  = await this._redis.get(this.k.rl(q, wk));
      return v != null ? [{ count: parseInt(v, 10) } as unknown as T] : [];
    }

    // ── Queue pause state ─────────────────────────────────────────────────
    if (/SELECT paused FROM bunmq_queue_pause/i.test(s)) {
      const q   = String(p("q") ?? "");
      const val = await this._redis.hget(this.k.pause(), q);
      return val != null ? [{ paused: parseInt(val, 10) } as unknown as T] : [];
    }

    // ── AVG processing time ───────────────────────────────────────────────
    if (/SELECT AVG\(duration\)/i.test(s)) {
      const q   = String(p("queue") ?? "");
      const avg = await this._avgDuration(q);
      return [{ avg } as unknown as T];
    }

    // ── Throughput metrics ────────────────────────────────────────────────
    if (/throughput_metrics|bunmq_metrics/i.test(s)) {
      const q      = String(p("queue") ?? "");
      const oneMin = Number(p("oneMin") ?? 0);
      return (await this._throughputStats(q, oneMin)) as unknown as T[];
    }

    // ── Oldest pending ────────────────────────────────────────────────────
    if (/SELECT MIN\(scheduled_at\)/i.test(s)) {
      const q   = String(p("q") ?? "");
      const ids = await this._redis.zrange(this.k.queue(q, "pending"), "0", "0");
      if (!ids.length) return [{ oldest: null } as unknown as T];
      const job = await this._getJobById(ids[0] as string);
      return [{ oldest: job?.scheduled_at ?? null } as unknown as T];
    }

    // ── Pending+active+scheduled count ───────────────────────────────────
    if (/SELECT COUNT\(\*\) as c FROM bunmq_jobs/i.test(s)) {
      const q = String(p("q") ?? "");
      let c = 0;
      for (const st of ["pending","active","scheduled"]) {
        const ids = await this._redis.zrange(this.k.queue(q, st), "0", "-1");
        c += (ids as string[]).length;
      }
      return [{ c } as unknown as T];
    }

    // ── Job logs ─────────────────────────────────────────────────────────
    if (/SELECT \* FROM bunmq_job_logs/i.test(s)) {
      const jobId   = String(p("jobId") ?? "");
      const entries = await this._redis.lrange(this.k.logs(jobId), 0, -1);
      return (entries as string[])
        .map((e, i) => {
          try { return { id: i + 1, ...JSON.parse(e) }; } catch { return null; }
        })
        .filter(Boolean)
        .reverse() as unknown as T[];
    }

    // ── PRAGMA — no-op ────────────────────────────────────────────────────
    if (/PRAGMA page_size/i.test(s))  return [{ page_size: 0 }  as unknown as T];
    if (/PRAGMA page_count/i.test(s)) return [{ page_count: 0 } as unknown as T];

    return [];
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Private helpers
  // ─────────────────────────────────────────────────────────────────────────

  private async _getJobById(id: string): Promise<JobRow | null> {
    if (!id) return null;
    const hash = await this._redis.hgetall(this.k.job(id));
    if (!hash || Object.keys(hash).length === 0) return null;

    // Parse into a mutable unknown record so we can assign numbers / nulls
    const row: Record<string, unknown> = { ...hash };

    // Parse JSON fields
    for (const key of ["payload","backoff","tags","meta","repeat_config"]) {
      if (typeof row[key] === "string" && row[key]) {
        try { row[key] = JSON.parse(row[key] as string); } catch { /* keep */ }
      }
    }
    // Parse numeric fields
    for (const key of ["priority","attempts","max_attempts","delay","timeout",
                        "scheduled_at","started_at","completed_at","failed_at",
                        "dead_at","created_at","updated_at","lock_until","progress","ttl"]) {
      const v = row[key];
      if (v != null && v !== "") {
        row[key] = parseInt(v as string, 10);
      } else {
        row[key] = null;
      }
    }
    return row as JobRow;
  }

  private async _insertJob(sql: string, params: Record<string, unknown>): Promise<MutationResult> {
    // Extract col→value mapping from the SQL INSERT column list
    const colMatch = sql.match(/\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/is);
    const hash: Record<string, string> = {};

    if (colMatch) {
      const cols   = colMatch[1].split(",").map(s => s.trim());
      const pnames = colMatch[2].split(",").map(s => s.trim().replace(/^\?$/, "").replace(/^\$/, ""));
      // pnames from ? are empty — fall back to params in order
      const paramValues = Object.values(params);
      for (let i = 0; i < cols.length; i++) {
        const v = pnames[i] ? (params[pnames[i]] ?? params[`$${pnames[i]}`] ?? paramValues[i]) : paramValues[i];
        hash[cols[i]] = v == null ? "" : String(v);
      }
    } else {
      for (const [k, v] of Object.entries(params)) {
        hash[k.replace(/^\$/, "")] = v == null ? "" : String(v);
      }
    }

    const id    = hash["id"];
    const queue = hash["queue"];
    const status = hash["status"] ?? "pending";
    const priority = parseInt(hash["priority"] ?? "2", 10);
    const schedAt  = parseInt(hash["scheduled_at"] ?? "0", 10);
    const dedupKey = hash["dedup_key"];
    const ttl      = parseInt(hash["ttl"] ?? "0", 10);

    if (!id) return { changes: 0 };

    await this._redis.hset(this.k.job(id), hash);

    if (ttl > 0) {
      await this._redis.expire(this.k.job(id), Math.ceil(ttl / 1000));
    }

    // Add to queue index
    await this._redis.zadd(this.k.queue(queue, status), this.score(priority, schedAt), id);

    // Track queue name
    await this._redis.sadd(this.k.queues(), queue);

    // Dedup pointer
    if (dedupKey) {
      await this._redis.set(this.k.dedup(dedupKey), id);
    }

    return { changes: 1 };
  }

  private async _updateJob(sql: string, params: Record<string, unknown>): Promise<MutationResult> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;

    // Extract SET block from SQL for literal assignments
    const setBlock = sql.match(/SET\s+(.*?)\s+WHERE/is)?.[1] ?? "";
    const literals: Record<string, string> = {};
    for (const clause of setBlock.split(",")) {
      const t = clause.trim();
      const lit = t.match(/^(\w+)\s*=\s*'([^']*)'$/);
      if (lit) literals[lit[1]] = lit[2];
      const num = t.match(/^(\w+)\s*=\s*(\d+)$/);
      if (num) literals[num[1]] = num[2];
      const nul = t.match(/^(\w+)\s*=\s*NULL$/i);
      if (nul) literals[nul[1]] = "";
    }

    const id = String(p("id") ?? "");

    if (id) {
      const hash = await this._redis.hgetall(this.k.job(id));
      if (!hash) return { changes: 0 };

      const oldStatus = hash["status"];
      const queue     = hash["queue"];
      const priority  = parseInt(hash["priority"] ?? "2", 10);

      // Apply literal SET values
      const updates: Record<string, string> = { ...literals };

      // Apply parametrised SET values from the SET block
      for (const clause of setBlock.split(",")) {
        const t = clause.trim();
        const paramMatch = t.match(/^(\w+)\s*=\s*\$(\w+)$/);
        if (paramMatch) {
          const col   = paramMatch[1];
          const pname = paramMatch[2];
          const v     = params[pname] ?? params[`$${pname}`] ?? null;
          updates[col] = v == null ? "" : String(v);
        }
        // computed: col=col+1
        const comp = t.match(/^(\w+)\s*=\s*(\w+)\s*\+\s*(\d+)$/);
        if (comp) {
          const cur = parseInt(hash[comp[2]] ?? "0", 10);
          updates[comp[1]] = String(cur + parseInt(comp[3], 10));
        }
      }

      // WHERE constraints
      if (/AND status='scheduled'/i.test(sql) && oldStatus !== "scheduled") return { changes: 0 };
      if (/AND status IN \('failed','dead'\)/i.test(sql) && !["failed","dead"].includes(oldStatus ?? "")) return { changes: 0 };

      const newStatus = updates["status"] ?? oldStatus;

      // Update queue index if status changed
      if (newStatus !== oldStatus) {
        await this._redis.zrem(this.k.queue(queue, oldStatus ?? ""), id);
        if (!["completed","dead"].includes(newStatus ?? "")) {
          const schedAt = parseInt(updates["scheduled_at"] ?? hash["scheduled_at"] ?? "0", 10);
          await this._redis.zadd(this.k.queue(queue, newStatus ?? "pending"), this.score(priority, schedAt), id);
        }
        // Clear dedup when job reaches terminal state
        if (["completed","dead"].includes(newStatus ?? "") && hash["dedup_key"]) {
          await this._redis.del(this.k.dedup(hash["dedup_key"]));
        }
      }

      await this._redis.hset(this.k.job(id), updates);
      return { changes: 1 };
    }

    // Bulk update (promote scheduled → pending, retryAll)
    const queue = String(p("queue") ?? "");
    let changed = 0;

    // Promote scheduled jobs whose time has come
    if (/UPDATE bunmq_jobs SET status='pending'.*scheduled_at<=/i.test(sql)) {
      const now = Number(p("now") ?? 0);
      const scheduledIds = await this._redis.zrangebyscore(
        this.k.queue(queue, "scheduled"), "-inf", String(now)
      );
      for (const jid of scheduledIds as string[]) {
        const h = await this._redis.hgetall(this.k.job(jid));
        if (!h) continue;
        const prio   = parseInt(h["priority"] ?? "2", 10);
        const sched  = parseInt(h["scheduled_at"] ?? "0", 10);
        await this._redis.zrem(this.k.queue(queue, "scheduled"), jid);
        await this._redis.zadd(this.k.queue(queue, "pending"), this.score(prio, sched), jid);
        await this._redis.hset(this.k.job(jid), { status: "pending", updated_at: String(now) });
        changed++;
      }
      return { changes: changed };
    }

    // retryAll
    if (/status='pending'.*status IN/i.test(sql) || /failed.*dead/i.test(sql)) {
      const now         = Number(p("now") ?? Date.now());
      const fromStatuses = /dead/i.test(sql) && /failed/i.test(sql)
        ? ["failed","dead"] : /dead/i.test(sql) ? ["dead"] : ["failed"];
      for (const st of fromStatuses) {
        const ids = await this._redis.zrange(this.k.queue(queue, st), "0", "-1");
        for (const jid of ids as string[]) {
          await this._redis.zrem(this.k.queue(queue, st), jid);
          const h = await this._redis.hgetall(this.k.job(jid));
          if (!h) continue;
          const prio = parseInt(h["priority"] ?? "2", 10);
          await this._redis.zadd(this.k.queue(queue, "pending"), this.score(prio, now), jid);
          await this._redis.hset(this.k.job(jid), {
            status: "pending", attempts: "0", error: "",
            stack_trace: "", failed_at: "", dead_at: "",
            scheduled_at: String(now), updated_at: String(now),
          });
          changed++;
        }
      }
      return { changes: changed };
    }

    return { changes: 0 };
  }

  private async _deleteJobs(sql: string, params: Record<string, unknown>): Promise<MutationResult> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const id     = p("id")    ? String(p("id"))    : null;
    const queue  = p("queue") ? String(p("queue")) : null;
    const status = p("status") ? String(p("status")) : null;
    const cutoff = p("cutoff") ? Number(p("cutoff")) : null;

    if (id) {
      const hash = await this._redis.hgetall(this.k.job(id));
      if (!hash) return { changes: 0 };

      // Honour status IN (...) constraint
      if (/AND status IN/i.test(sql)) {
        const allowed = [...sql.matchAll(/'([^']+)'/g)].map(m => m[1]);
        if (!allowed.includes(hash["status"] ?? "")) return { changes: 0 };
      }

      await this._redis.zrem(this.k.queue(hash["queue"], hash["status"] ?? ""), id);
      if (hash["dedup_key"]) await this._redis.del(this.k.dedup(hash["dedup_key"]));
      await this._redis.del(this.k.job(id));
      return { changes: 1 };
    }

    // Bulk delete
    const statuses = status
      ? [status]
      : /status IN.*pending.*scheduled/i.test(sql)
        ? ["pending","scheduled"]
        : ["pending","active","completed","failed","dead","scheduled"];

    let changed = 0;
    const queues = queue ? [queue] : await this._allQueues();

    for (const q of queues) {
      for (const st of statuses) {
        const ids = await this._redis.zrange(this.k.queue(q, st), "0", "-1");
        for (const jid of ids as string[]) {
          const hash = await this._redis.hgetall(this.k.job(jid));
          if (!hash) continue;

          // TTL delete filter
          if (/ttl>0/i.test(sql)) {
            const ttl     = parseInt(hash["ttl"] ?? "0", 10);
            const created = parseInt(hash["created_at"] ?? "0", 10);
            const now     = Number(p("now") ?? 0);
            if (!(ttl > 0 && (created + ttl) < now)) continue;
          }

          // cutoff filter (clean by age)
          if (cutoff != null) {
            const updated = parseInt(hash["updated_at"] ?? "0", 10);
            if (updated > cutoff) continue;
          }

          await this._redis.zrem(this.k.queue(q, st), jid);
          if (hash["dedup_key"]) await this._redis.del(this.k.dedup(hash["dedup_key"]));
          await this._redis.del(this.k.job(jid));
          changed++;
        }
      }
    }
    return { changes: changed };
  }

  private async _getPendingJobs(params: Record<string, unknown>): Promise<JobRow[]> {
    const p     = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const queue = String(p("queue") ?? "");
    const limit = Number(p("limit") ?? 10);
    const now   = Date.now();

    // Promote scheduled jobs first
    const scheduledIds = await this._redis.zrangebyscore(
      this.k.queue(queue, "scheduled"), "-inf", String(now)
    );
    for (const jid of scheduledIds as string[]) {
      const h = await this._redis.hgetall(this.k.job(jid));
      if (!h) continue;
      const prio  = parseInt(h["priority"] ?? "2", 10);
      const sched = parseInt(h["scheduled_at"] ?? "0", 10);
      await this._redis.zrem(this.k.queue(queue, "scheduled"), jid);
      await this._redis.zadd(this.k.queue(queue, "pending"), this.score(prio, sched), jid);
      await this._redis.hset(this.k.job(jid), { status: "pending", updated_at: String(now) });
    }

    // Fetch pending by priority score
    const ids = await this._redis.zrange(this.k.queue(queue, "pending"), "0", String(limit - 1));
    const jobs: JobRow[] = [];
    for (const id of ids as string[]) {
      const job = await this._getJobById(id);
      if (job) jobs.push(job);
    }
    return jobs;
  }

  private async _getStalledJobs(now: number): Promise<JobRow[]> {
    const queues  = await this._allQueues();
    const stalled: JobRow[] = [];
    for (const q of queues) {
      const ids = await this._redis.zrangebyscore(
        this.k.queue(q, "active"), "-inf", String(now)
      );
      for (const id of ids as string[]) {
        const job = await this._getJobById(id);
        if (job) stalled.push(job);
      }
    }
    return stalled;
  }

  private async _countByStatus(queue: string): Promise<Array<{ status: string; count: number }>> {
    const statuses = ["pending","active","completed","failed","dead","scheduled"];
    const result: Array<{ status: string; count: number }> = [];
    for (const st of statuses) {
      const ids = await this._redis.zrange(this.k.queue(queue, st), "0", "-1");
      if ((ids as string[]).length > 0) {
        result.push({ status: st, count: (ids as string[]).length });
      }
    }
    return result;
  }

  private async _filterJobs(sql: string, params: Record<string, unknown>): Promise<JobRow[]> {
    const p      = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const limit  = Number(p("limit")  ?? 100);
    const offset = Number(p("offset") ?? 0);
    const createdAfter  = p("createdAfter")  != null ? Number(p("createdAfter"))  : null;
    const createdBefore = p("createdBefore") != null ? Number(p("createdBefore")) : null;
    const name   = p("name")     ? String(p("name"))     : null;
    const prio   = p("priority") != null ? Number(p("priority")) : null;

    const queues: string[] = [];
    if (p("queue")) queues.push(String(p("queue")));
    for (const [k, v] of Object.entries(params)) {
      if (/^\$?q\d+$/.test(k)) queues.push(String(v));
    }

    const statuses: string[] = [];
    if (p("status")) statuses.push(String(p("status")));
    for (const [k, v] of Object.entries(params)) {
      if (/^\$?s\d+$/.test(k)) statuses.push(String(v));
    }

    const allQueues = queues.length ? queues : await this._allQueues();
    const allStatuses = statuses.length
      ? statuses
      : ["pending","active","completed","failed","dead","scheduled"];

    const jobs: JobRow[] = [];
    for (const q of allQueues) {
      for (const st of allStatuses) {
        const ids = await this._redis.zrange(this.k.queue(q, st), "0", "-1");
        for (const id of ids as string[]) {
          const job = await this._getJobById(id);
          if (!job) continue;
          if (name  != null && job.name     !== name)  continue;
          if (prio  != null && Number(job.priority) !== prio) continue;
          if (createdAfter  != null && Number(job.created_at) < createdAfter)  continue;
          if (createdBefore != null && Number(job.created_at) > createdBefore) continue;
          jobs.push(job);
        }
      }
    }

    // Sort
    const dir = /DESC/i.test(sql) ? -1 : 1;
    if      (/ORDER BY priority/i.test(sql))     jobs.sort((a,b) => dir*(Number(a.priority)-Number(b.priority)));
    else if (/ORDER BY scheduled_at/i.test(sql)) jobs.sort((a,b) => dir*(Number(a.scheduled_at)-Number(b.scheduled_at)));
    else                                          jobs.sort((a,b) => dir*(Number(a.created_at)-Number(b.created_at)));

    return jobs.slice(offset, offset + limit);
  }

  private async _handleRepeatJob(sql: string, params: Record<string, unknown>): Promise<MutationResult> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const id = String(p("id") ?? "");

    if (/DELETE/i.test(sql)) {
      await this._redis.del(this.k.repeat(id));
      await this._redis.zrem(this.k.repeats(), id);
      return { changes: 1 };
    }

    if (/INSERT/i.test(sql)) {
      const hash: Record<string, string> = {};
      for (const [k, v] of Object.entries(params)) {
        hash[k.replace(/^\$/, "")] = v == null ? "" : String(v);
      }
      await this._redis.hset(this.k.repeat(id), hash);
      const nextRunAt = Number(p("nextRunAt") ?? 0);
      await this._redis.zadd(this.k.repeats(), nextRunAt, id );
    }

    if (/UPDATE/i.test(sql)) {
      const next = Number(p("next") ?? 0);
      const now  = Number(p("now")  ?? 0);
      const hash = await this._redis.hgetall(this.k.repeat(id));
      if (hash) {
        const runCount = (parseInt(hash["run_count"] ?? "0", 10) + 1).toString();
        await this._redis.hset(this.k.repeat(id), {
          next_run_at: String(next),
          last_run_at: String(now),
          run_count:   runCount,
          updated_at:  String(now),
        });
        await this._redis.zadd(this.k.repeats(), next, id );
      }
    }

    return { changes: 1 };
  }

  private async _getDueRepeatJobs(now: number): Promise<JobRow[]> {
    const ids = await this._redis.zrangebyscore(this.k.repeats(), "-inf", String(now));
    const jobs: JobRow[] = [];
    for (const id of ids as string[]) {
      const hash = await this._redis.hgetall(this.k.repeat(id));
      if (!hash) continue;
      for (const k of ["payload","options","repeat_cfg"]) {
        if (typeof hash[k] === "string" && hash[k]) {
          try { hash[k] = JSON.parse(hash[k]); } catch { /* keep */ }
        }
      }
      jobs.push(hash as JobRow);
    }
    return jobs;
  }

  private async _avgDuration(queue: string): Promise<number | null> {
    const entries = await this._redis.lrange(this.k.metrics(queue), 0, 999);
    const durations: number[] = [];
    for (const e of entries as string[]) {
      try {
        const m = JSON.parse(e) as MetricEntry;
        if (m.status === "completed" && m.duration != null) durations.push(m.duration);
      } catch { /* skip */ }
    }
    if (!durations.length) return null;
    return durations.reduce((a, b) => a + b, 0) / durations.length;
  }

  private async _throughputStats(
    queue: string,
    oneMin: number
  ): Promise<Array<{ status: string; count: number; period: string }>> {
    const entries = await this._redis.lrange(this.k.metrics(queue), 0, 9_999);
    const now     = Date.now();
    const oneHour = now - 3_600_000;
    const counts: Record<string, { minute: number; hour: number }> = {
      completed: { minute: 0, hour: 0 },
      failed:    { minute: 0, hour: 0 },
    };
    for (const e of entries as string[]) {
      try {
        const m = JSON.parse(e) as MetricEntry;
        if (m.recorded_at < oneHour) continue;
        if (!counts[m.status]) continue;
        if (m.recorded_at >= oneMin) counts[m.status].minute++;
        counts[m.status].hour++;
      } catch { /* skip */ }
    }
    return [
      { status: "completed", count: counts.completed.minute, period: "minute" },
      { status: "failed",    count: counts.failed.minute,    period: "minute" },
      { status: "completed", count: counts.completed.hour,   period: "hour"   },
      { status: "failed",    count: counts.failed.hour,      period: "hour"   },
    ].filter(r => r.count > 0);
  }

  private async _allQueues(): Promise<string[]> {
    const members = await this._redis.smembers(this.k.queues());
    return members as string[];
  }
}
