# bunmq

**Advanced multi-adapter message queue for Bun** — SQLite (default), libSQL/Turso, PostgreSQL, MySQL, Redis, and in-memory. Zero required dependencies beyond Bun itself.

```
bun add bunmq
```

---

## Adapters

| Adapter | Import | Extra dep |
|---|---|---|
| **BunSQLite** (default) | `new BunSQLiteAdapter()` | none — uses `bun:sqlite` |
| **libSQL / Turso** | `new LibSQLAdapter({ url })` | `bun add @libsql/client` |
| **PostgreSQL** | `new PostgreSQLAdapter({ url })` | `bun add postgres` |
| **MySQL** | `new MySQLAdapter({ host })` | `bun add mysql2` |
| **Redis** | `new RedisAdapter({ url })` | Bun ≥ 1.2 built-in |
| **Memory** | `new MemoryAdapter()` | none — ideal for testing |

---

## Quick Start

```ts
import { BunMQ, BunSQLiteAdapter } from "bunmq";

// Default: in-memory SQLite
const mq = await BunMQ.create();

// Or pick your adapter:
const mq = await BunMQ.create({
  adapter: new BunSQLiteAdapter({ path: "./queue.db" }),
  enableLogs: true,
});

// Define queue + handler
mq.defineQueue("emails", { concurrency: 5 })
  .handle("emails", "send-welcome", async (ctx) => {
    const { to } = ctx.job.payload as { to: string };
    ctx.log(`Sending to ${to}`);
    ctx.updateProgress(50);
    await sendEmail(to);
    ctx.updateProgress(100);
    return { sent: true };
  });

mq.start("emails");

// Enqueue
const job = await mq.add("emails", { to: "alice@example.com" }, {
  name:     "send-welcome",
  priority: "high",
  attempts: 3,
});

// Wait for result
const done = await mq.waitForJob(job.id, 30_000);
console.log(done.result); // { sent: true }

await mq.close();
```

---

## All Adapter Examples

```ts
import {
  BunMQ,
  BunSQLiteAdapter,
  LibSQLAdapter,
  PostgreSQLAdapter,
  MySQLAdapter,
  RedisAdapter,
  MemoryAdapter,
} from "bunmq";

// bun:sqlite — local file
await BunMQ.create({ adapter: new BunSQLiteAdapter({ path: "./q.db", wal: true }) });

// libSQL — local or Turso remote
await BunMQ.create({ adapter: new LibSQLAdapter({ url: "file:./q.db" }) });
await BunMQ.create({ adapter: new LibSQLAdapter({
  url: "libsql://my-db.turso.io", authToken: process.env.TURSO_TOKEN,
}) });

// PostgreSQL (postgres.js)
await BunMQ.create({ adapter: new PostgreSQLAdapter({ url: "postgres://user:pass@localhost/bunmq" }) });

// MySQL (mysql2)
await BunMQ.create({ adapter: new MySQLAdapter({ host: "localhost", database: "bunmq", user: "root" }) });

// Redis (Bun built-in)
await BunMQ.create({ adapter: new RedisAdapter({ url: "redis://localhost:6379" }) });

// In-memory (zero persistence, great for tests)
await BunMQ.create({ adapter: new MemoryAdapter() });
```

---

## BunMQ Options

```ts
new BunMQ({
  adapter:             new BunSQLiteAdapter(),  // default
  defaultQueueOptions: { concurrency: 2 },      // apply to all queues
  migrate:             true,                     // auto-create schema
  cleanupInterval:     60_000,                   // TTL/metrics pruning ms
  enableLogs:          false,                    // per-job log table
  repeatInterval:      5_000,                    // repeat scheduler poll ms
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
  removeOnComplete: 3_600_000,   // auto-delete completed jobs after 1h
  removeOnFail:     0,           // keep failed jobs forever
  rateLimit:        { max: 100, window: 60_000 },
  stalledTimeout:   60_000,
  paused:           false,
});
```

---

## Adding Jobs

```ts
// Simple
await mq.add("queue", payload);

// With options
await mq.add("queue", payload, {
  name:      "job-type",
  priority:  "critical",
  delay:     5_000,
  attempts:  5,
  backoff:   { type: "exponential", delay: 500, max: 10_000 },
  timeout:   15_000,
  tags:      ["billing"],
  meta:      { userId: 42 },
  ttl:       86_400_000,
  dedupKey:  "welcome:user:42",
  jobId:     "my-custom-id",
});

// Bulk (serial with dedup checking)
const result = await mq.addBulk("queue", [
  { payload: { n: 1 } },
  { payload: { n: 2 }, opts: { priority: "high" } },
]);
// result = { added: [...ids], skipped: [...dedupKeys], errors: [] }

// Cron repeat
await mq.addRepeat("reports", "daily", {}, {
  cron:    "0 9 * * 1-5",   // 9am Mon-Fri
  limit:   52,
  endDate: new Date("2025-12-31"),
});

// Interval repeat
await mq.addRepeat("health", "ping", {}, { every: 30_000 });
```

---

## Handlers & Job Context

```ts
mq.handle("queue", "job-name", async (ctx) => {
  ctx.job.payload        // your payload
  ctx.job.attempts       // attempt number (0 = first)
  ctx.job.meta           // metadata

  await ctx.updateProgress(42)          // 0–100
  await ctx.log("doing thing", "info")  // "info"|"warn"|"error"
  await ctx.extendLock(30_000)          // prevent stall detection
  await ctx.moveToQueue("other")        // transfer mid-flight

  return { anything: "you want" }       // stored as job.result
});

// Wildcard — catches any unmatched name
mq.handle("queue", "*", async (ctx) => { ... });
```

---

## Backoff Strategies

```ts
{ type: "fixed",       delay: 1000 }
{ type: "exponential", delay: 1000, max: 30_000 }   // 1s, 2s, 4s, 8s…
{ type: "linear",      delay: 500 }                 // 500ms, 1s, 1.5s…
{ type: "jitter",      delay: 1000, max: 10_000 }   // random 0–exp(n)
```

---

## Cron Expressions

5-field syntax: `minute hour dom month dow`

```
*/5 * * * *          every 5 minutes
0 9 * * 1-5          9am weekdays
0 0 1 * *            midnight on the 1st
@daily / @hourly / @weekly / @monthly / @yearly
```

---

## Queue Control

```ts
await mq.pause("queue")                            // accept but don't process
await mq.resume("queue")
await mq.drain("queue")                            // delete pending/scheduled
await mq.clean("queue", "completed", 3_600_000)   // delete completed > 1h old
await mq.retryAll("queue", "dead")                // re-queue dead jobs
await mq.obliterate("queue")                       // nuke everything

await mq.retry("job-id")                           // re-queue single dead job
await mq.promote("job-id")                         // run scheduled job now
await mq.cancel("job-id")                          // delete pending job
await mq.remove("job-id")                          // delete any job
await mq.updateMeta("job-id", { key: "v" })
```

---

## Events

```ts
mq.on("job:added",     (job)              => {})
mq.on("job:started",   (job)              => {})
mq.on("job:completed", (job)              => {})
mq.on("job:failed",    ({ job, error })   => {})
mq.on("job:retrying",  ({ job, attempt }) => {})
mq.on("job:dead",      (job)              => {})
mq.on("job:progress",  ({ job, progress })=> {})
mq.on("job:stalled",   (job)              => {})
mq.on("queue:paused",  ({ queue })        => {})
mq.on("queue:resumed", ({ queue })        => {})
mq.on("queue:drained", ({ queue })        => {})
mq.on("error",         (err)              => {})

mq.once("queue:drained", handler)
mq.off("job:completed",  handler)
```

---

## Querying

```ts
await mq.findJobs({
  queue:         "emails",
  status:        ["pending","scheduled"],
  name:          "send-welcome",
  priority:      "high",
  tags:          ["transactional"],
  createdAfter:  new Date("2024-01-01"),
  createdBefore: new Date(),
  orderBy:       "priority",  // createdAt | scheduledAt | priority | attempts
  order:         "asc",
  limit:         50,
  offset:        0,
});
```

---

## Stats

```ts
const stats = await mq.getQueueStats("emails");
// { queue, pending, active, completed, failed, dead, scheduled, total,
//   throughput: { completed_last_minute, failed_last_minute, ... },
//   avgProcessingTime, oldestPendingAge }

const global = await mq.getGlobalStats();
// { queues: [...], total: {...}, db: { size, pageSize, pageCount } }

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
├── index.ts            Public exports
├── mq.ts               BunMQ core (async, adapter-agnostic)
├── types.ts            All interfaces & types
├── events.ts           Typed event emitter
├── cron.ts             5-field cron parser + next-date calculator
├── backoff.ts          Retry delay strategies
├── serializer.ts       Row ↔ Job mapping, ID generation
└── adapters/
    ├── adapter.ts      StorageAdapter interface + param rewriters
    ├── schema.ts       Dialect-aware DDL builder
    ├── sqlite.ts       bun:sqlite (BEGIN IMMEDIATE transactions)
    ├── libsql.ts       @libsql/client (libsql:// + Turso)
    ├── postgresql.ts   postgres.js (pooled, $1 params)
    ├── mysql.ts        mysql2 (pooled, ? positional)
    ├── redis.ts        Bun built-in Redis (sorted sets + hashes)
    └── memory.ts       In-process Map store (zero deps)
```

### How the adapter interface works

Every adapter implements `StorageAdapter`:

```ts
interface StorageAdapter {
  readonly dialect: "sqlite" | "libsql" | "mysql" | "postgresql" | "redis" | "memory";
  connect(): Promise<void>;
  run(sql: string, params?: Record<string, unknown>): Promise<MutationResult>;
  get<T>(sql: string, params?: Record<string, unknown>): Promise<T | null>;
  all<T>(sql: string, params?: Record<string, unknown>): Promise<T[]>;
  transaction<T>(fn: (adapter: StorageAdapter) => Promise<T>): Promise<T>;
  migrate(enableLogs: boolean): Promise<void>;
  close(): Promise<void>;
}
```

SQL uses `$name` placeholders throughout. Each adapter translates these to its native format — `$name` (bun:sqlite), `:name` (libsql), `$1` (postgres.js), `?` (mysql2), or custom pattern matching (Redis/Memory).

### Writing a custom adapter

```ts
import type { StorageAdapter, AdapterRow, MutationResult } from "bunmq";

class MyAdapter implements StorageAdapter {
  readonly dialect = "sqlite"; // or any supported value

  async connect() { /* open connection */ }
  async run(sql, params = {}) { /* execute mutation */ return { changes: 0 }; }
  async get<T>(sql, params = {}) { /* return first row or null */ return null; }
  async all<T>(sql, params = {}) { /* return all rows */ return []; }
  async transaction<T>(fn) { return fn(this); }
  async migrate(enableLogs) { /* create tables */ }
  async close() { /* teardown */ }
}

const mq = await BunMQ.create({ adapter: new MyAdapter() });
```

---

## License

MIT
