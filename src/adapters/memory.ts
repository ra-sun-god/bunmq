import type { StorageAdapter, AdapterRow, MutationResult } from "./adapter.ts";

// ─────────────────────────────────────────────────────────────────────────────
// MemoryAdapter — fully in-process, no dependencies, ideal for testing
// ─────────────────────────────────────────────────────────────────────────────

type JobRow = Record<string, unknown>;

export class MemoryAdapter implements StorageAdapter {
  readonly dialect = "memory" as const;

  private jobs        = new Map<string, JobRow>();
  private repeatJobs  = new Map<string, JobRow>();
  private queuePause  = new Map<string, number>();
  private rateBuckets = new Map<string, number>();
  private metrics     = new Map<string, Array<{ status: string; duration: number | null; recorded_at: number }>>();
  private logs        = new Map<string, Array<{ job_id: string; queue: string; level: string; message: string; created_at: number }>>();
  private meta        = new Map<string, string>();

  async connect(): Promise<void> {}

  async run(sql: string, params: Record<string, unknown> = {}): Promise<MutationResult> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const s = sql.trim().replace(/\s+/g, " ");

    if (/^INSERT (?:OR IGNORE )?INTO bunmq_jobs/i.test(s)) {
      return this._insertJob(params, s);
    }
    if (/^UPDATE bunmq_jobs/i.test(s)) {
      return this._updateJob(s, params);
    }
    if (/^DELETE FROM bunmq_jobs/i.test(s)) {
      return this._deleteJobs(s, params);
    }
    if (/bunmq_rate_buckets/i.test(s)) {
      const q = String(p("q") ?? ""); const wk = String(p("wk") ?? "");
      if (/INSERT/i.test(s)) this.rateBuckets.set(`${q}:${wk}`, (this.rateBuckets.get(`${q}:${wk}`) ?? 0) + 1);
      return { changes: 1 };
    }
    if (/bunmq_queue_pause/i.test(s)) {
      this.queuePause.set(String(p("q") ?? ""), Number(p("p") ?? 0));
      return { changes: 1 };
    }
    if (/bunmq_metrics/i.test(s)) {
      const queue = String(p("queue") ?? "");
      if (!this.metrics.has(queue)) this.metrics.set(queue, []);
      this.metrics.get(queue)!.push({ status: String(p("status") ?? ""), duration: p("duration") != null ? Number(p("duration")) : null, recorded_at: Number(p("now") ?? Date.now()) });
      return { changes: 1 };
    }
    if (/bunmq_job_logs/i.test(s)) {
      const jobId = String(p("jobId") ?? "");
      if (!this.logs.has(jobId)) this.logs.set(jobId, []);
      this.logs.get(jobId)!.push({ job_id: jobId, queue: String(p("queue") ?? ""), level: String(p("level") ?? "info"), message: String(p("msg") ?? ""), created_at: Number(p("now") ?? Date.now()) });
      return { changes: 1 };
    }
    if (/bunmq_repeat_jobs/i.test(s)) {
      if (/DELETE/i.test(s)) { this.repeatJobs.delete(String(p("id") ?? "")); return { changes: 1 }; }
      if (/INSERT/i.test(s)) {
        const id = String(p("id") ?? "");
        const row: JobRow = {};
        for (const [k, v] of Object.entries(params)) {
          const clean = k.replace(/^\$/, "");
          if (["payload","options","repeat_cfg"].includes(clean) && typeof v === "string") {
            try { row[clean] = JSON.parse(v); } catch { row[clean] = v; }
          } else { row[clean] = v ?? null; }
        }
        this.repeatJobs.set(id, row);
      } else if (/UPDATE/i.test(s)) {
        const id = String(p("id") ?? "");
        const job = this.repeatJobs.get(id);
        if (job) {
          job.next_run_at = Number(p("next") ?? 0);
          job.last_run_at = Number(p("now") ?? 0);
          job.run_count   = (Number(job.run_count ?? 0) + 1);
          job.updated_at  = Number(p("now") ?? 0);
        }
      }
      return { changes: 1 };
    }
    if (/bunmq_meta/i.test(s)) {
      const key = String(p("key") ?? "");
      if (!this.meta.has(key)) this.meta.set(key, String(p("value") ?? ""));
      return { changes: 1 };
    }
    // DDL no-op
    return { changes: 0 };
  }

  async get<T extends AdapterRow = AdapterRow>(sql: string, params: Record<string, unknown> = {}): Promise<T | null> {
    const rows = await this.all<T>(sql, params);
    return rows[0] ?? null;
  }

  async all<T extends AdapterRow = AdapterRow>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const s = sql.trim().replace(/\s+/g, " ");

    // Any SELECT from bunmq_jobs WHERE id=$id (including partial column selects)
    if (/SELECT .+ FROM bunmq_jobs WHERE id\s*=\s*\$id/i.test(s)) {
      const job = this.jobs.get(String(p("id") ?? ""));
      return job ? [job as unknown as T] : [];
    }
    if (/SELECT id FROM bunmq_jobs WHERE dedup_key/i.test(s)) {
      const key = String(p("key") ?? ""); const queue = String(p("queue") ?? "");
      const active = ["pending","active","scheduled","paused","failed"];
      for (const [id, job] of this.jobs) {
        if (String(job.dedup_key) === key && String(job.queue) === queue && active.includes(String(job.status)))
          return [{ id } as unknown as T];
      }
      return [];
    }
    if (/SELECT \* FROM bunmq_jobs.*WHERE.*queue.*status.*pending/i.test(s)) {
      return this._getPendingJobs(params) as unknown as T[];
    }
    if (/SELECT \* FROM bunmq_jobs WHERE status='active' AND lock_until/i.test(s)) {
      const now = Number(p("now") ?? 0);
      return [...this.jobs.values()].filter(j => j.status === "active" && Number(j.lock_until ?? 0) < now) as unknown as T[];
    }
    if (/SELECT status, COUNT/i.test(s)) {
      return this._countByStatus(String(p("queue") ?? "")) as unknown as T[];
    }
    if (/SELECT DISTINCT queue/i.test(s)) {
      const queues = new Set([...this.jobs.values()].map(j => String(j.queue)));
      return [...queues].map(q => ({ queue: q })) as unknown as T[];
    }
    if (/SELECT \* FROM bunmq_jobs/i.test(s)) {
      return this._filterJobs(s, params) as unknown as T[];
    }
    if (/SELECT \* FROM bunmq_repeat_jobs.*WHERE next_run_at/i.test(s)) {
      const now = Number(p("now") ?? 0);
      return [...this.repeatJobs.values()].filter(j => Number(j.next_run_at ?? 0) <= now && !j.paused) as unknown as T[];
    }
    if (/SELECT count FROM bunmq_rate_buckets/i.test(s)) {
      const v = this.rateBuckets.get(`${p("q")}:${p("wk")}`);
      return v != null ? [{ count: v } as unknown as T] : [];
    }
    if (/SELECT paused FROM bunmq_queue_pause/i.test(s)) {
      const v = this.queuePause.get(String(p("q") ?? ""));
      return v != null ? [{ paused: v } as unknown as T] : [];
    }
    if (/SELECT AVG\(duration\)/i.test(s)) {
      const q = String(p("queue") ?? "");
      const entries = (this.metrics.get(q) ?? []).filter(m => m.status === "completed" && m.duration != null);
      const avg = entries.length ? entries.reduce((a, m) => a + m.duration!, 0) / entries.length : null;
      return [{ avg } as unknown as T];
    }
    if (/throughput_metrics|bunmq_metrics/i.test(s)) {
      const q = String(p("queue") ?? ""); const oneMin = Number(p("oneMin") ?? 0);
      const oneHour = Date.now() - 3_600_000;
      const entries = (this.metrics.get(q) ?? []).filter(m => m.recorded_at >= oneHour);
      const counts: Record<string, { minute: number; hour: number }> = { completed: { minute: 0, hour: 0 }, failed: { minute: 0, hour: 0 } };
      for (const m of entries) {
        if (!counts[m.status]) continue;
        if (m.recorded_at >= oneMin) counts[m.status].minute++;
        counts[m.status].hour++;
      }
      return [
        { status: "completed", count: counts.completed.minute, period: "minute" },
        { status: "failed",    count: counts.failed.minute,    period: "minute" },
        { status: "completed", count: counts.completed.hour,   period: "hour" },
        { status: "failed",    count: counts.failed.hour,      period: "hour" },
      ].filter(r => r.count > 0) as unknown as T[];
    }
    if (/SELECT MIN\(scheduled_at\)/i.test(s)) {
      const q = String(p("q") ?? "");
      const pending = [...this.jobs.values()].filter(j => j.queue === q && j.status === "pending");
      const oldest = pending.reduce<number | null>((min, j) => {
        const sa = Number(j.scheduled_at ?? 0);
        return min == null ? sa : Math.min(min, sa);
      }, null);
      return [{ oldest } as unknown as T];
    }
    if (/SELECT COUNT\(\*\) as c FROM bunmq_jobs/i.test(s)) {
      const q = String(p("q") ?? "");
      const c = [...this.jobs.values()].filter(j => j.queue === q && ["pending","active","scheduled"].includes(String(j.status))).length;
      return [{ c } as unknown as T];
    }
    if (/SELECT \* FROM bunmq_job_logs/i.test(s)) {
      const jobId = String(p("jobId") ?? "");
      return (this.logs.get(jobId) ?? []).map((r, i) => ({ id: i + 1, ...r })) as unknown as T[];
    }
    if (/PRAGMA page_size/i.test(s))  return [{ page_size: 0 }  as unknown as T];
    if (/PRAGMA page_count/i.test(s)) return [{ page_count: 0 } as unknown as T];
    return [];
  }

  async transaction<T>(fn: (adapter: StorageAdapter) => Promise<T>): Promise<T> {
    return fn(this);
  }

  async migrate(_enableLogs: boolean): Promise<void> {
    this.meta.set("schema_version", "1");
  }

  async close(): Promise<void> {
    this.jobs.clear(); this.repeatJobs.clear(); this.metrics.clear(); this.logs.clear();
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────

  private _insertJob(params: Record<string, unknown>, sql: string): MutationResult {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;

    // Extract column names and param placeholders from the SQL
    const colMatch = sql.match(/\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/is);
    const row: JobRow = {};

    if (colMatch) {
      const cols   = colMatch[1].split(",").map(s => s.trim());
      const pholds = colMatch[2].split(",").map(s => s.trim().replace(/^\$/, ""));

      for (let i = 0; i < cols.length; i++) {
        const col    = cols[i];
        const pname  = pholds[i];
        const v      = params[pname] ?? params[`$${pname}`] ?? null;

        if (["payload","backoff","tags","meta","repeat_config"].includes(col) && typeof v === "string") {
          try { row[col] = JSON.parse(v); } catch { row[col] = v; }
        } else {
          row[col] = v;
        }
      }
    } else {
      // Fallback: use param keys directly (best-effort)
      for (const [k, v] of Object.entries(params)) {
        const clean = k.replace(/^\$/, "");
        row[clean] = v ?? null;
      }
    }

    // Numeric fields
    for (const f of ["priority","attempts","max_attempts","delay","timeout","scheduled_at",
                     "created_at","updated_at","ttl","progress"]) {
      if (row[f] != null) row[f] = Number(row[f]);
    }

    const id = String(row.id ?? p("id"));
    this.jobs.set(id, row);
    return { changes: 1 };
  }

  private _updateJob(sql: string, params: Record<string, unknown>): MutationResult {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const id = p("id") ? String(p("id")) : null;

    if (id) {
      const job = this.jobs.get(id);
      if (!job) return { changes: 0 };

      // Honour WHERE status constraints (promote, retry guards)
      if (/WHERE id=\$id AND status='scheduled'/i.test(sql) && job.status !== "scheduled") return { changes: 0 };
      if (/WHERE id=\$id AND status IN \('failed','dead'\)/i.test(sql) && !["failed","dead"].includes(String(job.status))) return { changes: 0 };

      // Extract the SET block
      const setBlock = sql.match(/SET\s+(.*?)\s+WHERE/is)?.[1] ?? "";

      for (const clause of setBlock.split(",")) {
        const trimmed = clause.trim();

        // Hardcoded literal: col='value'
        const litMatch = trimmed.match(/^(\w+)\s*=\s*'([^']*)'$/);
        if (litMatch) { job[litMatch[1]] = litMatch[2]; continue; }

        // Hardcoded number: col=number
        const numMatch = trimmed.match(/^(\w+)\s*=\s*(\d+)$/);
        if (numMatch) { job[numMatch[1]] = Number(numMatch[2]); continue; }

        // Hardcoded NULL: col=NULL
        const nullMatch = trimmed.match(/^(\w+)\s*=\s*NULL$/i);
        if (nullMatch) { job[nullMatch[1]] = null; continue; }

        // Parametrised: col=$param — look up the param value
        const paramMatch = trimmed.match(/^(\w+)\s*=\s*\$(\w+)$/);
        if (paramMatch) {
          const col   = paramMatch[1];
          const pname = paramMatch[2];
          const v     = params[pname] ?? params[`$${pname}`] ?? null;
          if (["meta","payload","backoff","tags","repeat_config"].includes(col) && typeof v === "string") {
            try { job[col] = JSON.parse(v); } catch { job[col] = v; }
          } else {
            job[col] = v;
          }
          continue;
        }

        // Computed: col=col+1 etc — handle run_count+1 pattern
        const computedMatch = trimmed.match(/^(\w+)\s*=\s*(\w+)\s*\+\s*(\d+)$/);
        if (computedMatch) {
          job[computedMatch[1]] = Number(job[computedMatch[2]] ?? 0) + Number(computedMatch[3]);
          continue;
        }
      }

      this.jobs.set(id, job);
      return { changes: 1 };
    }

    // Bulk update: promote scheduled → pending
    const queue = p("queue") ? String(p("queue")) : null;
    let changed = 0;

    for (const [jid, job] of this.jobs) {
      if (queue && job.queue !== queue) continue;

      // Promote scheduled jobs in poll loop
      if (/UPDATE bunmq_jobs SET status='pending'.*scheduled_at<=/i.test(sql)) {
        const now = Number(p("now") ?? 0);
        if (job.status === "scheduled" && Number(job.scheduled_at ?? 0) <= now) {
          job.status = "pending"; job.updated_at = now;
          this.jobs.set(jid, job); changed++;
        }
        continue;
      }

      // retryAll: SET status='pending'...WHERE queue=... AND status IN ('failed','dead')
      if (/status='pending'.*status IN.*failed.*dead/i.test(sql) || /failed.*dead/i.test(sql)) {
        const fromStatus = /failed/i.test(sql) && /dead/i.test(sql)
          ? ["failed","dead"]
          : /dead/i.test(sql) ? ["dead"] : ["failed"];
        if (fromStatus.includes(String(job.status))) {
          const now = Number(p("now") ?? Date.now());
          // Extract all literal SET values from SQL
          const setBlock2 = sql.match(/SET\s+(.*?)\s+WHERE/is)?.[1] ?? "";
          for (const clause of setBlock2.split(",")) {
            const lm = clause.match(/^\s*(\w+)\s*=\s*'([^']*)'\s*$/);
            if (lm) job[lm[1]] = lm[2];
          }
          job.attempts = 0; job.error = null; job.stack_trace = null;
          job.failed_at = null; job.dead_at = null;
          job.scheduled_at = now; job.updated_at = now;
          this.jobs.set(jid, job); changed++;
        }
      }
    }
    return { changes: changed };
  }

  private _deleteJobs(sql: string, params: Record<string, unknown>): MutationResult {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const id     = p("id")    ? String(p("id"))    : null;
    const queue  = p("queue") ? String(p("queue")) : null;
    const status = p("status") ? String(p("status")) : null;
    const cutoff = p("cutoff") ? Number(p("cutoff")) : null;

    if (id) {
      const job = this.jobs.get(id);
      if (!job) return { changes: 0 };
      // Honour status constraints: DELETE WHERE id=... AND status IN (...)
      if (/AND status IN/i.test(sql)) {
        const allowed = [...sql.matchAll(/'([^']+)'/g)].map(m => m[1]);
        if (!allowed.includes(String(job.status))) return { changes: 0 };
      }
      this.jobs.delete(id);
      return { changes: 1 };
    }

    let changed = 0;
    for (const [jid, job] of this.jobs) {
      if (queue && job.queue !== queue) continue;
      if (status && job.status !== status) continue;
      if (/status IN.*pending.*scheduled/i.test(sql) && !["pending","scheduled"].includes(String(job.status))) continue;
      if (cutoff && /updated_at/i.test(sql) && Number(job.updated_at ?? 0) > cutoff) continue;
      if (/ttl>0/i.test(sql)) {
        const ttl = Number(job.ttl ?? 0); const created = Number(job.created_at ?? 0);
        const now = Number(p("now") ?? 0);
        if (!(ttl > 0 && (created + ttl) < now)) continue;
      }
      this.jobs.delete(jid);
      changed++;
    }
    return { changes: changed };
  }

  private _getPendingJobs(params: Record<string, unknown>): JobRow[] {
    const p = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const queue = String(p("queue") ?? "");
    const limit = Number(p("limit") ?? 10);
    const now   = Date.now();

    // Promote scheduled jobs whose time has come
    for (const [jid, job] of this.jobs) {
      if (job.queue === queue && job.status === "scheduled" && Number(job.scheduled_at ?? 0) <= now) {
        job.status = "pending"; job.updated_at = now;
        this.jobs.set(jid, job);
      }
    }

    return [...this.jobs.values()]
      .filter(j => j.queue === queue && j.status === "pending")
      .sort((a, b) => {
        const pa = Number(a.priority ?? 2), pb = Number(b.priority ?? 2);
        if (pa !== pb) return pa - pb;
        return Number(a.scheduled_at ?? 0) - Number(b.scheduled_at ?? 0);
      })
      .slice(0, limit);
  }

  private _countByStatus(queue: string): Array<{ status: string; count: number }> {
    const counts: Record<string, number> = {};
    for (const job of this.jobs.values()) {
      if (job.queue !== queue) continue;
      const s = String(job.status); counts[s] = (counts[s] ?? 0) + 1;
    }
    return Object.entries(counts).map(([status, count]) => ({ status, count }));
  }

  private _filterJobs(sql: string, params: Record<string, unknown>): JobRow[] {
    const p      = (k: string): unknown => params[k] ?? params[`$${k}`] ?? null;
    const limit  = Number(p("limit")  ?? 100);
    const offset = Number(p("offset") ?? 0);
    const createdAfter  = p("createdAfter")  != null ? Number(p("createdAfter"))  : null;
    const createdBefore = p("createdBefore") != null ? Number(p("createdBefore")) : null;
    const name   = p("name")  ? String(p("name"))  : null;
    const prio   = p("priority") != null ? Number(p("priority")) : null;

    // Extract queue(s) — params may be q0, q1... or queue
    const queues: string[] = [];
    if (p("queue")) queues.push(String(p("queue")));
    for (const [k, v] of Object.entries(params)) {
      if (/^\$?q\d+$/.test(k)) queues.push(String(v));
    }

    // Extract status(es) — params may be s0, s1... or status
    const statuses: string[] = [];
    if (p("status")) statuses.push(String(p("status")));
    for (const [k, v] of Object.entries(params)) {
      if (/^\$?s\d+$/.test(k)) statuses.push(String(v));
    }

    let rows = [...this.jobs.values()];
    if (queues.length > 0)   rows = rows.filter(j => queues.includes(String(j.queue)));
    if (statuses.length > 0) rows = rows.filter(j => statuses.includes(String(j.status)));
    if (name)   rows = rows.filter(j => j.name     === name);
    if (prio != null) rows = rows.filter(j => Number(j.priority) === prio);
    if (createdAfter  != null) rows = rows.filter(j => Number(j.created_at ?? 0) >= createdAfter);
    if (createdBefore != null) rows = rows.filter(j => Number(j.created_at ?? 0) <= createdBefore);

    const dir = /DESC/i.test(sql) ? -1 : 1;
    if      (/ORDER BY priority/i.test(sql))     rows.sort((a, b) => dir * (Number(a.priority) - Number(b.priority)));
    else if (/ORDER BY scheduled_at/i.test(sql)) rows.sort((a, b) => dir * (Number(a.scheduled_at) - Number(b.scheduled_at)));
    else                                          rows.sort((a, b) => dir * (Number(a.created_at) - Number(b.created_at)));

    return rows.slice(offset, offset + limit);
  }
}
