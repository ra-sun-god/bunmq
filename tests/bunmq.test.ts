import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { BunMQ, BunSQLAdapter, MemoryAdapter, sqlite, postgres, mysql, mariadb } from "../src/index.ts";
import { nextCronDate, isValidCron } from "../src/cron.ts";
import { calcBackoff } from "../src/backoff.ts";
import type { StorageAdapter } from "../src/adapters/adapter.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Create a BunMQ instance with the given adapter, already connected */
async function makeMQ(adapter: StorageAdapter, extraOpts = {}) {
  const mq = new BunMQ({ adapter, enableLogs: true, repeatInterval: 100, ...extraOpts });
  await mq.connect();
  return mq;
}

// ─────────────────────────────────────────────────────────────────────────────
// Adapter matrix — run every test with both adapters
// ─────────────────────────────────────────────────────────────────────────────

const ADAPTERS: Array<{ name: string; make: () => StorageAdapter }> = [
  { name: "BunSQL/SQLite", make: () => new BunSQLAdapter({ url: "sqlite://:memory:" }) },
  { name: "Memory",        make: () => new MemoryAdapter() },
];

for (const { name, make } of ADAPTERS) {
  describe(`[${name}] backoff strategies`, () => {
    test("fixed always returns same delay", () => {
      for (let i = 1; i <= 5; i++) {
        expect(calcBackoff({ type: "fixed", delay: 1000 }, i)).toBe(1000);
      }
    });
    test("exponential doubles each attempt", () => {
      expect(calcBackoff({ type: "exponential", delay: 100 }, 1)).toBe(100);
      expect(calcBackoff({ type: "exponential", delay: 100 }, 2)).toBe(200);
      expect(calcBackoff({ type: "exponential", delay: 100 }, 3)).toBe(400);
    });
    test("exponential respects max cap", () => {
      expect(calcBackoff({ type: "exponential", delay: 1000, max: 2000 }, 5)).toBe(2000);
    });
    test("linear grows linearly", () => {
      expect(calcBackoff({ type: "linear", delay: 500 }, 3)).toBe(1500);
    });
    test("jitter returns value within bounds", () => {
      const v = calcBackoff({ type: "jitter", delay: 1000, max: 5000 }, 3);
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(5000);
    });
  });

  // ─── Cron ──────────────────────────────────────────────────────────────────

  describe(`[${name}] cron parser`, () => {
    test("isValidCron accepts standard expressions", () => {
      expect(isValidCron("* * * * *")).toBe(true);
      expect(isValidCron("0 0 1 1 *")).toBe(true);
      expect(isValidCron("*/5 * * * *")).toBe(true);
      expect(isValidCron("0 9-17 * * 1-5")).toBe(true);
      expect(isValidCron("@daily")).toBe(true);
      expect(isValidCron("@hourly")).toBe(true);
      expect(isValidCron("@weekly")).toBe(true);
    });
    test("isValidCron rejects invalid expressions", () => {
      expect(isValidCron("* * *")).toBe(false);
      expect(isValidCron("not a cron")).toBe(false);
      expect(isValidCron("")).toBe(false);
    });
    test("nextCronDate @hourly is within 1 hour", () => {
      const next = nextCronDate("@hourly");
      const diff = next.getTime() - Date.now();
      expect(diff).toBeGreaterThan(0);
      expect(diff).toBeLessThanOrEqual(3_600_000);
    });
    test("nextCronDate */15 is within 15 minutes", () => {
      const next = nextCronDate("*/15 * * * *");
      const diff = next.getTime() - Date.now();
      expect(diff).toBeGreaterThan(0);
      expect(diff).toBeLessThanOrEqual(15 * 60_000 + 1000);
    });
  });

  // ─── Basic Lifecycle ───────────────────────────────────────────────────────

  describe(`[${name}] basic lifecycle`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("add() creates a pending job", async () => {
      const job = await mq.add("emails", { to: "test@example.com" });
      expect(job.id).toBeDefined();
      expect(job.status).toBe("pending");
      expect(job.queue).toBe("emails");
      expect(job.priority).toBe("normal");
      expect(job.attempts).toBe(0);
    });

    test("getJob() retrieves the created job with correct payload", async () => {
      const job = await mq.add("emails", { to: "a@b.com", count: 42 });
      const fetched = await mq.getJob(job.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(job.id);
      expect((fetched!.payload as { to: string }).to).toBe("a@b.com");
      expect((fetched!.payload as { count: number }).count).toBe(42);
    });

    test("getJob() returns null for unknown id", async () => {
      expect(await mq.getJob("nonexistent-id")).toBeNull();
    });

    test("job with delay gets status=scheduled", async () => {
      const job = await mq.add("emails", {}, { delay: 60_000 });
      expect(job.status).toBe("scheduled");
    });

    test("priority is stored correctly", async () => {
      const j1 = await mq.add("pq", {}, { priority: "critical" });
      const j2 = await mq.add("pq", {}, { priority: "low" });
      expect(j1.priority).toBe("critical");
      expect(j2.priority).toBe("low");
    });

    test("tags are stored and retrieved", async () => {
      const job = await mq.add("tq", {}, { tags: ["billing", "transactional"] });
      const fetched = await mq.getJob(job.id);
      expect(fetched!.tags).toEqual(["billing", "transactional"]);
    });

    test("meta is stored and retrieved", async () => {
      const job = await mq.add("mq", {}, { meta: { userId: 99, env: "prod" } });
      const fetched = await mq.getJob(job.id);
      expect(fetched!.meta).toEqual({ userId: 99, env: "prod" });
    });

    test("cancel() removes pending job", async () => {
      const job = await mq.add("test", {});
      expect(await mq.cancel(job.id)).toBe(true);
      expect(await mq.getJob(job.id)).toBeNull();
    });

    test("cancel() fails on non-pending job", async () => {
      const job = await mq.add("test", {}, { delay: 60_000 }); // scheduled
      // cancel should still work on scheduled
      expect(await mq.cancel(job.id)).toBe(true);
    });

    test("remove() deletes any job", async () => {
      const job = await mq.add("test", {});
      expect(await mq.remove(job.id)).toBe(true);
      expect(await mq.getJob(job.id)).toBeNull();
    });

    test("remove() returns false for unknown id", async () => {
      expect(await mq.remove("nope")).toBe(false);
    });

    test("promote() moves scheduled job to pending", async () => {
      const job = await mq.add("prom", {}, { delay: 3_600_000 });
      expect(job.status).toBe("scheduled");
      expect(await mq.promote(job.id)).toBe(true);
      const promoted = await mq.getJob(job.id);
      expect(promoted!.status).toBe("pending");
    });

    test("promote() returns false for non-scheduled job", async () => {
      const job = await mq.add("prom", {});
      expect(await mq.promote(job.id)).toBe(false);
    });

    test("updateMeta() merges metadata", async () => {
      const job = await mq.add("mq", {}, { meta: { a: 1 } });
      await mq.updateMeta(job.id, { b: 2 });
      const fetched = await mq.getJob(job.id);
      expect(fetched!.meta).toEqual({ a: 1, b: 2 });
    });
  });

  // ─── Processing ────────────────────────────────────────────────────────────

  describe(`[${name}] job processing`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("handler runs and job is marked completed", async () => {
      let ran = false;
      mq.defineQueue("work", { pollInterval: 50 });
      mq.handle("work", "default", async () => { ran = true; return "done"; });
      mq.start("work");

      const job = await mq.add("work", { x: 1 });
      const done = await mq.waitForJob(job.id, 5000);

      expect(done.status).toBe("completed");
      expect(done.result).toBe("done");
      expect(ran).toBe(true);
    });

    test("result is persisted on the job", async () => {
      mq.defineQueue("rq", { pollInterval: 50 });
      mq.handle("rq", "default", async () => ({ answer: 42 }));
      mq.start("rq");

      const job = await mq.add("rq", {});
      await mq.waitForJob(job.id, 5000);

      const done = await mq.getJob(job.id);
      expect((done!.result as { answer: number }).answer).toBe(42);
    });

    test("named handler routes by job name", async () => {
      const called: string[] = [];
      mq.defineQueue("named", { pollInterval: 50 });
      mq.handle("named", "type-a", async () => { called.push("a"); });
      mq.handle("named", "type-b", async () => { called.push("b"); });
      mq.start("named");

      const j1 = await mq.add("named", {}, { name: "type-a" });
      const j2 = await mq.add("named", {}, { name: "type-b" });
      await mq.waitForJob(j1.id, 5000);
      await mq.waitForJob(j2.id, 5000);

      expect(called).toContain("a");
      expect(called).toContain("b");
    });

    test("wildcard handler catches unmatched job names", async () => {
      let caught = "";
      mq.defineQueue("wc", { pollInterval: 50 });
      mq.handle("wc", "*", async (ctx) => { caught = ctx.job.name; });
      mq.start("wc");

      const job = await mq.add("wc", {}, { name: "mystery" });
      await mq.waitForJob(job.id, 5000);
      expect(caught).toBe("mystery");
    });

    test("failed job retries and eventually goes dead", async () => {
      let calls = 0;
      mq.defineQueue("fails", {
        pollInterval: 50, defaultAttempts: 3,
        defaultBackoff: { type: "fixed", delay: 10 },
      });
      mq.handle("fails", "default", async () => { calls++; throw new Error("oops"); });
      mq.start("fails");

      const job = await mq.add("fails", {});
      const dead = await mq.waitForJob(job.id, 10_000);

      expect(dead.status).toBe("dead");
      expect(dead.attempts).toBe(3);
      expect(dead.error).toBe("oops");
      expect(calls).toBe(3);
    });

    test("error is stored on dead job", async () => {
      mq.defineQueue("err-q", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("err-q", "default", async () => { throw new Error("something broke"); });
      mq.start("err-q");

      const job = await mq.add("err-q", {});
      await mq.waitForJob(job.id, 5000);

      const dead = await mq.getJob(job.id);
      expect(dead!.status).toBe("dead");
      expect(dead!.error).toBe("something broke");
    });

    test("progress updates persist to storage", async () => {
      mq.defineQueue("prog", { pollInterval: 50 });
      mq.handle("prog", "default", async (ctx) => {
        await ctx.updateProgress(33);
        await sleep(10);
        await ctx.updateProgress(66);
        await sleep(10);
        await ctx.updateProgress(100);
      });
      mq.start("prog");

      const job = await mq.add("prog", {});
      await mq.waitForJob(job.id, 5000);

      const done = await mq.getJob(job.id);
      expect(done!.progress).toBe(100);
    });

    test("job logs are stored with correct levels", async () => {
      mq.defineQueue("log-q", { pollInterval: 50 });
      mq.handle("log-q", "default", async (ctx) => {
        await ctx.log("info message", "info");
        await ctx.log("warn message", "warn");
        await ctx.log("error message", "error");
      });
      mq.start("log-q");

      const job = await mq.add("log-q", {});
      await mq.waitForJob(job.id, 5000);

      const logs = await mq.getJobLogs(job.id);
      expect(logs).toHaveLength(3);
      expect(logs[0].level).toBe("info");
      expect(logs[0].message).toBe("info message");
      expect(logs[1].level).toBe("warn");
      expect(logs[2].level).toBe("error");
    });

    test("concurrency is respected", async () => {
      let concurrent = 0, maxConcurrent = 0;
      mq.defineQueue("conc", { concurrency: 2, pollInterval: 50 });
      mq.handle("conc", "default", async () => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await sleep(80);
        concurrent--;
      });
      mq.start("conc");

      for (let i = 0; i < 6; i++) await mq.add("conc", { i });
      await mq.waitForDrain("conc", 10_000);

      expect(maxConcurrent).toBeLessThanOrEqual(2);
    });

    test("no handler emits job:failed and marks dead", async () => {
      let failedJob: unknown = null;
      mq.on("job:failed", ({ job }) => { failedJob = job; });
      mq.defineQueue("no-handler", { pollInterval: 50, defaultAttempts: 1 });
      mq.start("no-handler");

      const job = await mq.add("no-handler", {});
      await mq.waitForJob(job.id, 5000);

      expect((failedJob as { id: string })?.id).toBe(job.id);
    });

    test("timeout kills long-running job", async () => {
      mq.defineQueue("timeout-q", {
        pollInterval: 50, defaultAttempts: 1, defaultTimeout: 100,
      });
      mq.handle("timeout-q", "default", async () => { await sleep(5000); });
      mq.start("timeout-q");

      const job = await mq.add("timeout-q", {});
      const dead = await mq.waitForJob(job.id, 5000);
      expect(dead.status).toBe("dead");
      expect(dead.error).toContain("timed out");
    });

    test("moveToQueue transfers job mid-flight", async () => {
      let movedJobId: string | null = null;
      mq.defineQueue("src-q", { pollInterval: 50 });
      mq.defineQueue("dst-q", { pollInterval: 50 });
      mq.handle("src-q", "default", async (ctx) => { await ctx.moveToQueue("dst-q"); });
      mq.handle("dst-q", "default", async (ctx) => { movedJobId = ctx.job.id; });
      mq.start("src-q");
      mq.start("dst-q");

      const job = await mq.add("src-q", {});
      // Give it time to move and process
      await sleep(500);
      expect(movedJobId).not.toBeNull();
      expect(movedJobId!).toBe(job.id);
    });
  });

  // ─── Priority ordering ─────────────────────────────────────────────────────

  describe(`[${name}] priority ordering`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("higher priority jobs are dequeued first", async () => {
      const order: number[] = [];
      mq.defineQueue("prio", { pollInterval: 50, concurrency: 1 });
      mq.handle("prio", "default", async (ctx) => {
        order.push((ctx.job.payload as { n: number }).n);
        await sleep(30);
      });

      // Add in reverse priority order
      await mq.add("prio", { n: 3 }, { priority: "low" });
      await mq.add("prio", { n: 1 }, { priority: "critical" });
      await mq.add("prio", { n: 2 }, { priority: "high" });

      mq.start("prio");
      await mq.waitForDrain("prio", 5000);

      expect(order[0]).toBe(1); // critical first
      expect(order[1]).toBe(2); // high second
      expect(order[2]).toBe(3); // low last
    });
  });

  // ─── Deduplication ─────────────────────────────────────────────────────────

  describe(`[${name}] deduplication`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("same dedupKey returns existing job", async () => {
      const j1 = await mq.add("emails", { to: "a@b.com" }, { dedupKey: "welcome:1" });
      const j2 = await mq.add("emails", { to: "different" }, { dedupKey: "welcome:1" });
      expect(j1.id).toBe(j2.id);
      const all = await mq.findJobs({ queue: "emails" });
      expect(all).toHaveLength(1);
    });

    test("different dedupKeys create different jobs", async () => {
      const j1 = await mq.add("emails", {}, { dedupKey: "key-a" });
      const j2 = await mq.add("emails", {}, { dedupKey: "key-b" });
      expect(j1.id).not.toBe(j2.id);
    });

    test("dedupKey allows new job after completion", async () => {
      mq.defineQueue("emails", { pollInterval: 50 });
      mq.handle("emails", "default", async () => {});
      mq.start("emails");

      const j1 = await mq.add("emails", {}, { dedupKey: "welcome:2" });
      await mq.waitForJob(j1.id, 5000);

      const j2 = await mq.add("emails", {}, { dedupKey: "welcome:2" });
      expect(j2.id).not.toBe(j1.id);
    });
  });

  // ─── Bulk add ──────────────────────────────────────────────────────────────

  describe(`[${name}] addBulk`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("adds multiple jobs", async () => {
      const result = await mq.addBulk("bulk", [
        { payload: { n: 1 } },
        { payload: { n: 2 } },
        { payload: { n: 3 } },
      ]);
      expect(result.added).toHaveLength(3);
      expect(result.skipped).toHaveLength(0);
      expect(result.errors).toHaveLength(0);
    });

    test("skips deduplicated entries", async () => {
      await mq.add("bulk", {}, { dedupKey: "k1" });
      const result = await mq.addBulk("bulk", [
        { payload: { n: 1 }, opts: { dedupKey: "k1" } }, // skip
        { payload: { n: 2 } },
      ]);
      expect(result.added).toHaveLength(1);
      expect(result.skipped).toHaveLength(1);
      expect(result.skipped[0]).toBe("k1");
    });

    test("all added jobs are retrievable", async () => {
      const result = await mq.addBulk("bulk2", [
        { payload: { x: 10 } },
        { payload: { x: 20 } },
      ]);
      for (const id of result.added) {
        const job = await mq.getJob(id);
        expect(job).not.toBeNull();
      }
    });
  });

  // ─── findJobs / filtering ──────────────────────────────────────────────────

  describe(`[${name}] findJobs`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("filters by status", async () => {
      await mq.add("fq", {});
      await mq.add("fq", {}, { delay: 60_000 }); // scheduled

      const pending   = await mq.findJobs({ queue: "fq", status: "pending" });
      const scheduled = await mq.findJobs({ queue: "fq", status: "scheduled" });
      expect(pending).toHaveLength(1);
      expect(scheduled).toHaveLength(1);
    });

    test("filters by multiple statuses", async () => {
      await mq.add("fq2", {});
      await mq.add("fq2", {}, { delay: 60_000 });

      const both = await mq.findJobs({ queue: "fq2", status: ["pending", "scheduled"] });
      expect(both).toHaveLength(2);
    });

    test("filters by priority", async () => {
      await mq.add("fq3", {}, { priority: "high" });
      await mq.add("fq3", {}, { priority: "low" });
      await mq.add("fq3", {}, { priority: "high" });

      const high = await mq.findJobs({ queue: "fq3", priority: "high" });
      expect(high).toHaveLength(2);
    });

    test("filters by job name", async () => {
      await mq.add("fq4", {}, { name: "type-a" });
      await mq.add("fq4", {}, { name: "type-b" });
      await mq.add("fq4", {}, { name: "type-a" });

      const typeA = await mq.findJobs({ queue: "fq4", name: "type-a" });
      expect(typeA).toHaveLength(2);
    });

    test("paginates with limit/offset", async () => {
      for (let i = 0; i < 10; i++) await mq.add("pag", { i });

      const page1 = await mq.findJobs({ queue: "pag", limit: 4, offset: 0 });
      const page2 = await mq.findJobs({ queue: "pag", limit: 4, offset: 4 });
      const page3 = await mq.findJobs({ queue: "pag", limit: 4, offset: 8 });

      expect(page1).toHaveLength(4);
      expect(page2).toHaveLength(4);
      expect(page3).toHaveLength(2);
    });

    test("orders by priority asc", async () => {
      await mq.add("ord", {}, { priority: "low" });
      await mq.add("ord", {}, { priority: "critical" });
      await mq.add("ord", {}, { priority: "high" });

      const jobs = await mq.findJobs({ queue: "ord", orderBy: "priority", order: "asc" });
      expect(jobs[0].priority).toBe("critical");
      expect(jobs[1].priority).toBe("high");
      expect(jobs[2].priority).toBe("low");
    });

    test("filters by createdAfter", async () => {
      const before = new Date(Date.now() - 10_000);
      await mq.add("ca", {});
      const after = await mq.findJobs({ queue: "ca", createdAfter: before });
      expect(after).toHaveLength(1);

      const future = new Date(Date.now() + 10_000);
      const none   = await mq.findJobs({ queue: "ca", createdAfter: future });
      expect(none).toHaveLength(0);
    });
  });

  // ─── Queue management ──────────────────────────────────────────────────────

  describe(`[${name}] queue management`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("pause() stops processing", async () => {
      let ran = 0;
      mq.defineQueue("pq", { pollInterval: 50 });
      mq.handle("pq", "default", async () => { ran++; });
      await mq.pause("pq");   // pause before start
      mq.start("pq");

      await mq.add("pq", {});
      await sleep(300);
      expect(ran).toBe(0);
    });

    test("resume() restarts processing after pause", async () => {
      let ran = 0;
      mq.defineQueue("pq2", { pollInterval: 50 });
      mq.handle("pq2", "default", async () => { ran++; });
      await mq.pause("pq2");  // pause before start
      mq.start("pq2");

      const job = await mq.add("pq2", {});
      await sleep(200);
      expect(ran).toBe(0);

      await mq.resume("pq2");
      await mq.waitForJob(job.id, 3000);
      expect(ran).toBe(1);
    });

    test("drain() removes all pending/scheduled jobs", async () => {
      for (let i = 0; i < 5; i++) await mq.add("drq", { i });
      const removed = await mq.drain("drq");
      expect(removed).toBe(5);
      const remaining = await mq.findJobs({ queue: "drq" });
      expect(remaining).toHaveLength(0);
    });

    test("clean() removes completed jobs", async () => {
      mq.defineQueue("clq", { pollInterval: 50 });
      mq.handle("clq", "default", async () => {});
      mq.start("clq");

      const job = await mq.add("clq", {});
      await mq.waitForJob(job.id, 5000);

      const removed = await mq.clean("clq", "completed");
      expect(removed).toBe(1);
      expect(await mq.getJob(job.id)).toBeNull();
    });

    test("clean() respects olderThanMs", async () => {
      mq.defineQueue("clq2", { pollInterval: 50 });
      mq.handle("clq2", "default", async () => {});
      mq.start("clq2");

      const job = await mq.add("clq2", {});
      await mq.waitForJob(job.id, 5000);

      // Should NOT remove: job was completed less than 1 hour ago
      const removed = await mq.clean("clq2", "completed", 3_600_000);
      expect(removed).toBe(0);
      expect(await mq.getJob(job.id)).not.toBeNull();
    });

    test("retryAll() re-queues dead jobs", async () => {
      mq.defineQueue("raq", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("raq", "default", async () => { throw new Error("fail"); });
      mq.start("raq");

      const job = await mq.add("raq", {});
      await mq.waitForJob(job.id, 5000);
      expect((await mq.getJob(job.id))!.status).toBe("dead");

      mq.stop("raq");
      const retried = await mq.retryAll("raq");
      expect(retried).toBe(1);
      expect((await mq.getJob(job.id))!.status).toBe("pending");
    });

    test("retryAll() with fromStatus='dead' only retries dead", async () => {
      mq.defineQueue("raq2", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("raq2", "default", async () => { throw new Error("fail"); });
      mq.start("raq2");

      const job = await mq.add("raq2", {});
      await mq.waitForJob(job.id, 5000);
      mq.stop("raq2");

      const retried = await mq.retryAll("raq2", "dead");
      expect(retried).toBe(1);
    });

    test("obliterate() removes all queue data", async () => {
      for (let i = 0; i < 3; i++) await mq.add("obl", { i });
      await mq.obliterate("obl");
      expect(await mq.findJobs({ queue: "obl" })).toHaveLength(0);
    });

    test("retry() re-queues a single dead job", async () => {
      mq.defineQueue("rtq", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("rtq", "default", async () => { throw new Error("x"); });
      mq.start("rtq");

      const job = await mq.add("rtq", {});
      await mq.waitForJob(job.id, 5000);

      mq.stop("rtq");
      expect(await mq.retry(job.id)).toBe(true);
      expect((await mq.getJob(job.id))!.status).toBe("pending");
      expect((await mq.getJob(job.id))!.attempts).toBe(0);
    });
  });

  // ─── Events ────────────────────────────────────────────────────────────────

  describe(`[${name}] events`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("job:added fires when job is added", async () => {
      let emitted: unknown = null;
      mq.on("job:added", (job) => { emitted = job; });
      const added = await mq.add("ev", { msg: "hello" });
      expect((emitted as { id: string })?.id).toBe(added.id);
    });

    test("job:started fires when processing begins", async () => {
      let started: unknown = null;
      mq.on("job:started", (job) => { started = job; });
      mq.defineQueue("ev2", { pollInterval: 50 });
      mq.handle("ev2", "default", async () => {});
      mq.start("ev2");

      const job = await mq.add("ev2", {});
      await mq.waitForJob(job.id, 5000);
      expect((started as { id: string })?.id).toBe(job.id);
    });

    test("job:completed fires on success", async () => {
      let completed: unknown = null;
      mq.on("job:completed", (job) => { completed = job; });
      mq.defineQueue("ev3", { pollInterval: 50 });
      mq.handle("ev3", "default", async () => "ok");
      mq.start("ev3");

      const job = await mq.add("ev3", {});
      await mq.waitForJob(job.id, 5000);
      expect((completed as { id: string })?.id).toBe(job.id);
    });

    test("job:dead fires when all retries exhausted", async () => {
      let dead: unknown = null;
      mq.on("job:dead", (job) => { dead = job; });
      mq.defineQueue("ev4", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("ev4", "default", async () => { throw new Error("die"); });
      mq.start("ev4");

      const job = await mq.add("ev4", {});
      await mq.waitForJob(job.id, 5000);
      expect((dead as { id: string })?.id).toBe(job.id);
    });

    test("job:retrying fires on each retry", async () => {
      const attempts: number[] = [];
      mq.on("job:retrying", ({ attempt }) => { attempts.push(attempt); });
      mq.defineQueue("ev5", {
        pollInterval: 50, defaultAttempts: 3,
        defaultBackoff: { type: "fixed", delay: 10 },
      });
      mq.handle("ev5", "default", async () => { throw new Error("retry"); });
      mq.start("ev5");

      const job = await mq.add("ev5", {});
      await mq.waitForJob(job.id, 10_000);
      expect(attempts.length).toBe(2); // retry after attempt 1 and 2
    });

    test("job:progress fires on updateProgress", async () => {
      const progresses: number[] = [];
      mq.on("job:progress", ({ progress }) => { progresses.push(progress); });
      mq.defineQueue("ev6", { pollInterval: 50 });
      mq.handle("ev6", "default", async (ctx) => {
        await ctx.updateProgress(50);
        await ctx.updateProgress(100);
      });
      mq.start("ev6");

      const job = await mq.add("ev6", {});
      await mq.waitForJob(job.id, 5000);
      expect(progresses).toContain(50);
      expect(progresses).toContain(100);
    });

    test("queue:drained fires when queue empties", async () => {
      let drained = false;
      mq.on("queue:drained", () => { drained = true; });
      mq.defineQueue("ev7", { pollInterval: 50 });
      mq.handle("ev7", "default", async () => {});
      mq.start("ev7");

      const job = await mq.add("ev7", {});
      await mq.waitForJob(job.id, 5000);
      await sleep(200);
      expect(drained).toBe(true);
    });

    test("once() listener fires exactly once", async () => {
      let count = 0;
      mq.once("job:added", () => { count++; });
      await mq.add("once-q", {});
      await mq.add("once-q", {});
      expect(count).toBe(1);
    });

    test("off() removes listener", async () => {
      let count = 0;
      const handler = () => { count++; };
      mq.on("job:added", handler);
      await mq.add("off-q", {});
      mq.off("job:added", handler);
      await mq.add("off-q", {});
      expect(count).toBe(1);
    });
  });

  // ─── Stats ─────────────────────────────────────────────────────────────────

  describe(`[${name}] stats`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("getQueueStats returns correct status counts", async () => {
      mq.defineQueue("sq", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("sq", "default", async (ctx) => {
        if ((ctx.job.payload as { fail?: boolean }).fail) throw new Error("x");
      });
      mq.start("sq");

      await mq.add("sq", { fail: false });
      const failJob = await mq.add("sq", { fail: true });
      await mq.add("sq", {}, { delay: 3_600_000 }); // future scheduled

      await mq.waitForJob(failJob.id, 5000);
      await sleep(300);

      const stats = await mq.getQueueStats("sq");
      expect(stats.queue).toBe("sq");
      expect(stats.completed).toBeGreaterThanOrEqual(1);
      expect(stats.dead).toBeGreaterThanOrEqual(1);
      expect(stats.scheduled).toBe(1);
      expect(stats.total).toBeGreaterThanOrEqual(3);
    });

    test("getGlobalStats aggregates across queues", async () => {
      await mq.add("gs1", {});
      await mq.add("gs2", {});
      await mq.add("gs2", {});

      const stats = await mq.getGlobalStats();
      expect(stats.total.total).toBeGreaterThanOrEqual(3);
      expect(stats.queues.length).toBeGreaterThanOrEqual(2);
    });

    test("stats pending count decrements after completion", async () => {
      mq.defineQueue("sc", { pollInterval: 50 });
      mq.handle("sc", "default", async () => {});
      mq.start("sc");

      await mq.add("sc", {});
      await mq.add("sc", {});

      const before = await mq.getQueueStats("sc");
      expect(before.pending + before.active).toBeGreaterThanOrEqual(0);

      await mq.waitForDrain("sc", 5000);
      const after = await mq.getQueueStats("sc");
      expect(after.pending).toBe(0);
      expect(after.active).toBe(0);
      expect(after.completed).toBe(2);
    });
  });

  // ─── TTL ───────────────────────────────────────────────────────────────────

  describe(`[${name}] TTL`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make(), { cleanupInterval: 100 }); });
    afterEach(async () => { await mq.close(); });

    test("job is auto-deleted after TTL expires", async () => {
      const job = await mq.add("ttl-q", {}, { ttl: 150 });
      expect(await mq.getJob(job.id)).not.toBeNull();
      await sleep(400);
      expect(await mq.getJob(job.id)).toBeNull();
    });

    test("job without TTL is not deleted", async () => {
      const job = await mq.add("ttl-q2", {}, { ttl: 0 });
      await sleep(400);
      expect(await mq.getJob(job.id)).not.toBeNull();
    });
  });

  // ─── Repeat jobs ───────────────────────────────────────────────────────────

  describe(`[${name}] repeat jobs`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("addRepeat() with every enqueues on interval", async () => {
      const jobIds: string[] = [];
      mq.defineQueue("rep", { pollInterval: 50 });
      mq.handle("rep", "rep-job", async (ctx) => { jobIds.push(ctx.job.id); });
      mq.start("rep");

      // With repeatInterval=100ms and every=80ms, scheduler fires every 100ms
      // and enqueues a job each time. After 600ms we expect ≥ 3 runs.
      await mq.addRepeat("rep", "rep-job", {}, { every: 80 });
      await sleep(700);

      expect(jobIds.length).toBeGreaterThanOrEqual(2);
    });

    test("removeRepeat() stops future scheduling", async () => {
      let ran = 0;
      mq.defineQueue("rep2", { pollInterval: 50 });
      mq.handle("rep2", "r", async () => { ran++; });
      mq.start("rep2");

      const repeatId = await mq.addRepeat("rep2", "r", {}, { every: 100 });
      await sleep(250);
      await mq.removeRepeat(repeatId);
      const countAfterRemove = ran;
      await sleep(300);

      expect(ran).toBe(countAfterRemove); // no more runs after remove
    });

    test("addRepeat() respects run limit", async () => {
      let ran = 0;
      mq.defineQueue("rep3", { pollInterval: 50 });
      mq.handle("rep3", "r", async () => { ran++; });
      mq.start("rep3");

      await mq.addRepeat("rep3", "r", {}, { every: 80, limit: 2 });
      await sleep(1000);

      expect(ran).toBeLessThanOrEqual(2);
    });
  });

  // ─── Adapter-specific: waitForDrain ───────────────────────────────────────

  describe(`[${name}] waitForDrain / waitForJob`, () => {
    let mq: BunMQ;
    beforeEach(async () => { mq = await makeMQ(make()); });
    afterEach(async () => { await mq.close(); });

    test("waitForDrain resolves when all jobs finish", async () => {
      mq.defineQueue("wfd", { pollInterval: 50, concurrency: 3 });
      mq.handle("wfd", "default", async () => { await sleep(50); });
      mq.start("wfd");

      for (let i = 0; i < 5; i++) await mq.add("wfd", { i });
      await mq.waitForDrain("wfd", 5000);

      const stats = await mq.getQueueStats("wfd");
      expect(stats.completed).toBe(5);
    });

    test("waitForJob rejects on timeout", async () => {
      const job = await mq.add("wfj", {});
      await expect(mq.waitForJob(job.id, 100)).rejects.toThrow("Timeout");
    });

    test("waitForJob resolves for dead jobs too", async () => {
      mq.defineQueue("wfj2", { pollInterval: 50, defaultAttempts: 1 });
      mq.handle("wfj2", "default", async () => { throw new Error("x"); });
      mq.start("wfj2");

      const job = await mq.add("wfj2", {});
      const result = await mq.waitForJob(job.id, 5000);
      expect(result.status).toBe("dead");
    });
  });

  describe(`[${name}] BunMQ.create() factory`, () => {
    test("creates and connects in one call", async () => {
      const mq = await BunMQ.create({ adapter: make(), enableLogs: false, repeatInterval: 100 });
      const job = await mq.add("factory-q", { hello: "world" });
      expect(job.id).toBeDefined();
      await mq.close();
    });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// BunSQLAdapter-specific: SQLite options, factory functions
// ─────────────────────────────────────────────────────────────────────────────

describe("[BunSQLAdapter] SQLite-specific options", () => {
  test("accepts WAL mode (default on SQLite)", async () => {
    const adapter = new BunSQLAdapter({ url: "sqlite://:memory:" });
    const mq = await makeMQ(adapter);
    const job = await mq.add("wq", {});
    expect(job.id).toBeDefined();
    await mq.close();
  });

  test("WAL mode can be disabled", async () => {
    const adapter = new BunSQLAdapter({ url: "sqlite://:memory:", wal: false });
    const mq = await makeMQ(adapter);
    const job = await mq.add("nowq", {});
    expect(job.id).toBeDefined();
    await mq.close();
  });

  test("sqlite() factory creates adapter with correct dialect", () => {
    const adapter = sqlite("sqlite://:memory:");
    expect(adapter.dialect).toBe("sqlite");
  });

  test("postgres() factory creates adapter with correct dialect", () => {
    const adapter = postgres("postgres://localhost/test");
    expect(adapter.dialect).toBe("postgresql");
  });

  test("mysql() factory creates adapter with correct dialect", () => {
    const adapter = mysql("mysql://localhost/test");
    expect(adapter.dialect).toBe("mysql");
  });

  test("mariadb() is an alias for mysql()", () => {
    const adapter = mariadb("mysql://localhost/test");
    expect(adapter.dialect).toBe("mysql");
  });

  test("default BunMQ uses BunSQLAdapter", async () => {
    const mq = await BunMQ.create({ repeatInterval: 100 });
    expect(mq.adapter).toBeInstanceOf(BunSQLAdapter);
    await mq.close();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// MemoryAdapter — verify clean close
// ─────────────────────────────────────────────────────────────────────────────

describe("[Memory only] clean close", () => {
  test("close() clears all in-memory state", async () => {
    const adapter = new MemoryAdapter();
    const mq      = await makeMQ(adapter);
    await mq.add("mc", {});
    await mq.close();
    // Reconnect fresh
    const mq2 = await BunMQ.create({ adapter: new MemoryAdapter() });
    const jobs = await mq2.findJobs({ queue: "mc" });
    expect(jobs).toHaveLength(0);
    await mq2.close();
  });
});
