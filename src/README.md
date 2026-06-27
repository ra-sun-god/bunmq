# bunmq

**Advanced message queue for Bun** — SQLite, PostgreSQL, MySQL, and MariaDB all through Bun's built-in SQL client. Zero external SQL driver dependencies. In-memory adapter included for tests.

```sh
bun add @rasungod/bunmq
```

---

## Backends

All SQL backends use `import { SQL } from "bun"` — no extra packages needed.

| Backend    | URL scheme              | Notes                             |
|------------|-------------------------|-----------------------------------|
| **SQLite** | `sqlite://:memory:`     | Default. File or in-memory.       |
| **PostgreSQL** | `postgres://...`    | Pooled connections.               |
| **MySQL**  | `mysql://...`           | Also covers **MariaDB**.          |
| **Memory** | *(no URL)*              | In-process Map store, for tests.  |

---

## Quick Start

```ts
import { BunMQ } from "bunmq";

// Default: in-memory SQLite, no config needed
const mq = await BunMQ.create();

// Define a queue and handler
mq.defineQueue("emails", { concurrency: 5 })
  .handle("emails", "send-welcome", async (ctx) => {
    const { to } = ctx.job.payload as { to: string };
    await ctx.log(`Sending to ${to}`);
    await ctx.updateProgress(50);
    await sendEmail(to);
    await ctx.updateProgress(100);
    return { sent: true };
  });

mq.start("emails");

const job = await mq.add("emails", { to: "alice@example.com" }, {
  name:     "send-welcome",
  priority: "high",
  attempts: 3,
});

const done = await mq.waitForJob(job.id);
console.log(done.result); // { sent: true }

await mq.close();
```

---

## Choosing an Adapter

```ts
import { BunMQ, BunSQLAdapter, MemoryAdapter, sqlite, postgres, mysql, mariadb } from "bunmq";

// ── SQLite (default) ─────────────────────────────────────────────────────────
const mq = await BunMQ.create();
// or with a file:
const mq = await BunMQ.create({ adapter: sqlite("sqlite:///path/to/queue.db") });
// or full URL:
const mq = await BunMQ.create({ adapter: new BunSQLAdapter({ url: "sqlite:///queue.db" }) });

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const mq = await BunMQ.create({ adapter: postgres("postgres://user:pass@localhost:5432/mydb") });

// ── MySQL ─────────────────────────────────────────────────────────────────────
const mq = await BunMQ.create({ adapter: mysql("mysql://user:pass@localhost:3306/mydb") });

// ── MariaDB (same wire protocol as MySQL) ─────────────────────────────────────
const mq = await BunMQ.create({ adapter: mariadb("mysql://user:pass@localhost:3306/mydb") });

// ── In-memory (unit tests) ────────────────────────────────────────────────────
const mq = await BunMQ.create({ adapter: new MemoryAdapter() });
```

---

## BunMQ Options

```ts
await BunMQ.create({
  adapter:             new BunSQLAdapter(),  // defaults to in-memory SQLite
  defaultQueueOptions: { concurrency: 2 },   // applies to all queues
  migrate:             true,                  // auto-create schema (default: true)
  cleanupInterval:     60_000,                // TTL/metrics pruning ms
  enableLogs:          false,                 // per-job structured log table
  repeatInterval:      5_000,                 // repeat job scheduler interval ms
});
```

---

## BunSQLAdapter Options

```ts
new BunSQLAdapter({
  url:     "sqlite://:memory:",   // connection URL (see schemes above)
  dialect: "sqlite",              // optional — inferred from URL
  max:     10,                    // pool size (PostgreSQL / MySQL only)
  wal:     true,                  // SQLite only: WAL mode (default: true)
})
```

---

## Queue Options

```ts
mq.defineQueue("my-queue", {
  concurrency:      4,
  defaultAttempts:  5,
  defaultTimeout:   30_000,
  defaultBackoff:   { type: "exponential", delay: 1000, max: 30_000 },
  defaultPriority:  "normal",
  pollInterval:     500,
  removeOnComplete: 3_600_000,
  removeOnFail:     0,
  rateLimit:        { max: 100, window: 60_000 },
  stalledTimeout:   60_000,
  paused:           false,
});
```

---

## Adding Jobs

```ts
await mq.add("queue", payload);

await mq.add("queue", payload, {
  name:      "job-type",      // routes to the matching handler
  priority:  "critical",      // critical | high | normal | low
  delay:     5_000,           // ms before first run
  attempts:  5,               // max retries
  backoff:   { type: "exponential", delay: 500, max: 10_000 },
  timeout:   15_000,          // ms — 0 = unlimited
  tags:      ["billing"],
  meta:      { userId: 42 },
  ttl:       86_400_000,      // auto-delete if not run within 24h
  dedupKey:  "welcome:user:42",
  jobId:     "my-custom-id",
});

// Bulk add
const result = await mq.addBulk("queue", [
  { payload: { n: 1 } },
  { payload: { n: 2 }, opts: { priority: "high" } },
]);

// Repeating — cron
await mq.addRepeat("reports", "daily-digest", {}, {
  cron:    "0 9 * * 1-5",    // 9am Mon–Fri
  limit:   52,
});

// Repeating — interval
await mq.addRepeat("health", "ping", {}, { every: 30_000 });
```

---

## Job Context

```ts
mq.handle("queue", "job-name", async (ctx) => {
  ctx.job.payload          // your payload
  ctx.job.attempts         // 0-based attempt number
  ctx.job.meta             // metadata

  await ctx.updateProgress(50)              // 0–100
  await ctx.log("doing thing", "info")      // "info" | "warn" | "error"
  await ctx.extendLock(30_000)              // prevent stall detection
  await ctx.moveToQueue("other-queue")      // transfer job mid-flight

  return { anything: "you want" }           // stored as job.result
});

// Wildcard — catches any unmatched name
mq.handle("queue", "*", async (ctx) => { ... });
```

---

## Backoff Strategies

```ts
{ type: "fixed",       delay: 1000 }
{ type: "exponential", delay: 1000, max: 30_000 }   // 1s, 2s, 4s…
{ type: "linear",      delay: 500 }                 // 500ms, 1s, 1.5s…
{ type: "jitter",      delay: 1000, max: 10_000 }   // random 0–exp(n)
```

---

## Cron Expressions

5-field syntax: `minute hour dom month dow`

```
*/5 * * * *          every 5 minutes
0 9 * * 1-5          9am weekdays
0 0 1 * *            midnight on the 1st of every month
@daily / @hourly / @weekly / @monthly / @yearly
```

---

## Queue Management

```ts
await mq.pause("queue")                             // stop processing (still accepts jobs)
await mq.resume("queue")
await mq.drain("queue")                             // delete all pending/scheduled
await mq.clean("queue", "completed", 3_600_000)     // delete completed > 1h old
await mq.retryAll("queue", "dead")                  // re-queue all dead jobs
await mq.obliterate("queue")                        // delete everything for this queue

await mq.retry("job-id")                            // re-queue a single dead job
await mq.promote("job-id")                          // run a scheduled job immediately
await mq.cancel("job-id")                           // delete a pending/scheduled job
await mq.remove("job-id")                           // delete any job
await mq.updateMeta("job-id", { key: "val" })       // merge metadata
```

---

## Events

```ts
mq.on("job:added",     (job)               => {})
mq.on("job:started",   (job)               => {})
mq.on("job:completed", (job)               => {})
mq.on("job:failed",    ({ job, error })    => {})
mq.on("job:retrying",  ({ job, attempt })  => {})
mq.on("job:dead",      (job)               => {})
mq.on("job:progress",  ({ job, progress }) => {})
mq.on("job:stalled",   (job)               => {})
mq.on("queue:paused",  ({ queue })         => {})
mq.on("queue:resumed", ({ queue })         => {})
mq.on("queue:drained", ({ queue })         => {})
mq.on("error",         (err)               => {})

mq.once("queue:drained", handler)
mq.off("job:completed",  handler)
```

---

## Querying

```ts
await mq.findJobs({
  queue:         "emails",
  status:        ["pending", "scheduled"],
  name:          "send-welcome",
  priority:      "high",
  tags:          ["transactional"],
  createdAfter:  new Date("2024-01-01"),
  createdBefore: new Date(),
  orderBy:       "priority",    // createdAt | scheduledAt | priority | attempts
  order:         "asc",
  limit:         50,
  offset:        0,
});
```

---

## Stats

```ts
const stats = await mq.getQueueStats("emails");
// {
//   queue, pending, active, completed, failed, dead, scheduled, total,
//   throughput: { completed_last_minute, failed_last_minute, completed_last_hour, failed_last_hour },
//   avgProcessingTime,   // ms
//   oldestPendingAge,    // ms
// }

const global = await mq.getGlobalStats();
// { queues: [...], total: { pending, active, completed, ... }, db: { size, pageSize, pageCount } }

const logs = await mq.getJobLogs("job-id");  // requires enableLogs: true
```

---

## Waiting

```ts
const job = await mq.waitForJob("job-id", 30_000);
await mq.waitForDrain("emails", 60_000);
```

---

## Architecture

```
src/
├── index.ts                  Public exports
├── mq.ts                     BunMQ core — async, adapter-agnostic
├── types.ts                  All types and interfaces
├── events.ts                 Typed event emitter
├── cron.ts                   5-field cron parser + next-date calculator
├── backoff.ts                Retry delay strategies (fixed/exp/linear/jitter)
├── serializer.ts             Row ↔ Job mapping, ID generation
└── adapters/
    ├── adapter.ts            StorageAdapter interface
    ├── bun-sql.ts            BunSQLAdapter — SQLite/PostgreSQL/MySQL/MariaDB
    ├── memory.ts             MemoryAdapter — in-process, zero deps
    ├── schema.ts             Dialect-aware DDL + SQL helpers
    └── index.ts              Adapter barrel
```

### How `BunSQLAdapter` works

Bun's `SQL` class accepts a URL and auto-selects the right native driver. `BunSQLAdapter` wraps it with three operations:

- **`db.unsafe(sql, values[])`** — runs parameterised queries. All MQ SQL uses `$name` placeholders; `namedToPositional()` rewrites these to `?` and extracts values in order (one `?` per occurrence, no deduplication — required because SQLite/MySQL bind exactly N values for N placeholders).
- **`db.transaction(async tx => {...})`** — Bun handles `BEGIN`/`COMMIT`/`ROLLBACK` automatically. The `tx` object also has `.unsafe()`, so the same rewriting path is used inside transactions.
- **SQLite PRAGMAs** — WAL mode goes in the URL (`?journal_mode=WAL`); performance settings (`synchronous`, `cache_size`, `mmap_size`) are applied via `db.unsafe("PRAGMA ...")` after connect.

### Writing a custom adapter

```ts
import type { StorageAdapter, AdapterRow, MutationResult } from "bunmq";
import { applySchema } from "bunmq";

class MyAdapter implements StorageAdapter {
  readonly dialect = "sqlite" as const;  // closest match

  async connect()  { /* open connection */ }
  async run(sql, params = {})  { return { changes: 0 }; }
  async get(sql, params = {})  { return null; }
  async all(sql, params = {})  { return []; }
  async transaction(fn)        { return fn(this); }
  async migrate(enableLogs)    { await applySchema(this, enableLogs); }
  async close()                { /* teardown */ }
}
```

---

## License

MIT
