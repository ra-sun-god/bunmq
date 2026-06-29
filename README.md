# @rasungod/bunmq

**Advanced message queue for Bun** — SQLite, PostgreSQL, MySQL, MariaDB, Redis, and in-memory. All SQL backends use Bun's built-in `SQL` client; Redis uses Bun's built-in `RedisClient`. Zero external driver dependencies.

[![npm](https://img.shields.io/npm/v/@rasungod/bunmq)](https://www.npmjs.com/package/@rasungod/bunmq)

```sh
bun add @rasungod/bunmq
```

---

## Backends

| Backend        | Adapter              | URL scheme            | Requires              |
|----------------|----------------------|-----------------------|-----------------------|
| **SQLite**     | `BunSQLAdapter`      | `sqlite://`           | Bun built-in SQL      |
| **PostgreSQL** | `BunSQLAdapter`      | `postgres://`         | Bun built-in SQL      |
| **MySQL**      | `BunSQLAdapter`      | `mysql://`            | Bun built-in SQL      |
| **MariaDB**    | `BunSQLAdapter`      | `mysql://`            | Bun built-in SQL      |
| **Redis**      | `BunRedisAdapter`    | `redis://`            | Bun built-in RedisClient |
| **Memory**     | `MemoryAdapter`      | *(none)*              | Nothing — for tests   |

---

## Quick Start

```ts
import { BunMQ } from "@rasungod/bunmq";

// Default: in-memory SQLite — zero config
const mq = await BunMQ.create();

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
import {
  BunMQ,
  BunSQLAdapter,
  BunRedisAdapter,
  MemoryAdapter,
  sqlite, postgres, mysql, mariadb,
} from "@rasungod/bunmq";

// SQLite — in-memory (default)
const mq = await BunMQ.create();

// SQLite — file
const mq = await BunMQ.create({
  adapter: sqlite("sqlite:///path/to/queue.db"),
});

// SQLite — file with lock retry options (use when multiple processes share the DB)
const mq = await BunMQ.create({
  adapter: new BunSQLAdapter({
    url:         "sqlite:///queue.db",
    busyTimeout: 15_000,   // SQLite waits up to 15s for a lock
    maxRetries:  10,        // app-level retries on SQLITE_BUSY
    retryDelay:  100,       // base ms for exponential backoff
  }),
});

// PostgreSQL
const mq = await BunMQ.create({
  adapter: postgres("postgres://user:pass@localhost:5432/mydb"),
});

// MySQL
const mq = await BunMQ.create({
  adapter: mysql("mysql://user:pass@localhost:3306/mydb"),
});

// MariaDB (same wire protocol as MySQL)
const mq = await BunMQ.create({
  adapter: mariadb("mysql://user:pass@localhost:3306/mydb"),
});

// Redis — Bun's built-in RedisClient
const mq = await BunMQ.create({
  adapter: new BunRedisAdapter({ url: "redis://localhost:6379" }),
});

// Memory — in-process, zero persistence, great for unit tests
const mq = await BunMQ.create({
  adapter: new MemoryAdapter(),
});
```

---

## SQLite Lock Handling

When multiple processes (e.g. a web server + a background indexer) share the same SQLite file, writes can collide and produce `SQLiteError: database is locked`. `BunSQLAdapter` handles this automatically:

1. **`busyTimeout`** — passed to SQLite itself via the URL (`?busy_timeout=N`). SQLite will spin-wait inside the engine for up to N ms before returning `SQLITE_BUSY`.
2. **`maxRetries` + `retryDelay`** — application-level retry wrapper around every `run()`, `get()`, `all()`, and `transaction()`. On `SQLITE_BUSY` / `SQLITE_LOCKED`, it waits `retryDelay * 2^attempt + jitter` ms and retries, up to `maxRetries` times.

```ts
new BunSQLAdapter({
  url:         "sqlite:///shared.db",
  wal:         true,         // WAL mode reduces lock contention (default: true)
  busyTimeout: 10_000,       // SQLite-level wait (default: 10s)
  maxRetries:  5,            // App-level retries (default: 5)
  retryDelay:  50,           // Base retry delay ms (default: 50)
})
```

For high-contention multi-process setups, consider PostgreSQL or Redis instead.

---

## BunMQ Options

```ts
await BunMQ.create({
  adapter:             new BunSQLAdapter(),  // default: in-memory SQLite
  defaultQueueOptions: { concurrency: 2 },   // global defaults for all queues
  migrate:             true,                  // auto-create schema (default: true)
  cleanupInterval:     60_000,                // TTL/metrics pruning interval ms
  enableLogs:          false,                 // structured per-job log table
  repeatInterval:      5_000,                 // repeat job scheduler poll ms
});
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
  removeOnFail:     0,           // keep failed/dead jobs forever
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
  name:      "job-type",       // routes to the matching handler
  priority:  "critical",       // critical | high | normal | low
  delay:     5_000,            // ms before first run
  attempts:  5,                // max retries
  backoff:   { type: "exponential", delay: 500, max: 10_000 },
  timeout:   15_000,           // ms per attempt — 0 = unlimited
  tags:      ["billing"],
  meta:      { userId: 42 },
  ttl:       86_400_000,       // auto-delete if not run within 24h
  dedupKey:  "welcome:user:42",
  jobId:     "my-custom-id",
});

// Bulk
const result = await mq.addBulk("queue", [
  { payload: { n: 1 } },
  { payload: { n: 2 }, opts: { priority: "high" } },
]);

// Cron repeat
await mq.addRepeat("reports", "daily-digest", {}, {
  cron:  "0 9 * * 1-5",   // 9am Mon–Fri
  limit: 52,
});

// Interval repeat
await mq.addRepeat("health", "ping", {}, { every: 30_000 });
```

---

## Job Context

```ts
mq.handle("queue", "job-name", async (ctx) => {
  ctx.job.payload           // your typed payload
  ctx.job.attempts          // 0-based attempt number
  ctx.job.meta              // metadata dict

  await ctx.updateProgress(50)              // 0–100
  await ctx.log("doing thing", "info")      // "info" | "warn" | "error"
  await ctx.extendLock(30_000)              // prevent stall detection
  await ctx.moveToQueue("other-queue")      // transfer mid-flight

  return { anything: "you want" }           // stored as job.result
});

// Wildcard — catches any unmatched job name
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
*/5 * * * *        every 5 minutes
0 9 * * 1-5        9am weekdays
0 0 1 * *          midnight on the 1st
@daily / @hourly / @weekly / @monthly / @yearly
```

---

## Queue Management

```ts
await mq.pause("queue")                            // accept jobs but don't process
await mq.resume("queue")
await mq.drain("queue")                            // delete all pending/scheduled
await mq.clean("queue", "completed", 3_600_000)    // delete completed > 1h old
await mq.retryAll("queue", "dead")                 // re-queue all dead jobs
await mq.obliterate("queue")                       // delete everything for this queue

await mq.retry("job-id")                           // re-queue a single dead job
await mq.promote("job-id")                         // run a scheduled job now
await mq.cancel("job-id")                          // delete a pending/scheduled job
await mq.remove("job-id")                          // delete any job
await mq.updateMeta("job-id", { key: "val" })      // merge metadata
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

## Stats & Monitoring

```ts
const stats = await mq.getQueueStats("emails");
// {
//   queue, pending, active, completed, failed, dead, scheduled, total,
//   throughput: { completed_last_minute, failed_last_minute,
//                 completed_last_hour,   failed_last_hour },
//   avgProcessingTime,   // ms | null
//   oldestPendingAge,    // ms | null
// }

const global = await mq.getGlobalStats();
// { queues: [...], total: {...}, db: { size, pageSize, pageCount } }

const logs = await mq.getJobLogs("job-id");  // requires enableLogs: true
```

---

## Querying Jobs

```ts
await mq.findJobs({
  queue:         "emails",
  status:        ["pending", "scheduled"],
  name:          "send-welcome",
  priority:      "high",
  tags:          ["transactional"],
  createdAfter:  new Date("2024-01-01"),
  createdBefore: new Date(),
  orderBy:       "priority",   // createdAt | scheduledAt | priority | attempts
  order:         "asc",
  limit:         50,
  offset:        0,
});
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
├── index.ts                 Public exports
├── mq.ts                    BunMQ core — async, adapter-agnostic
├── types.ts                 All types and interfaces
├── events.ts                Typed event emitter
├── cron.ts                  5-field cron parser + next-date calculator
├── backoff.ts               Retry delay strategies
├── serializer.ts            Row ↔ Job mapping, ID generation
└── adapters/
    ├── adapter.ts           StorageAdapter interface
    ├── bun-sql.ts           BunSQLAdapter — SQLite/PostgreSQL/MySQL/MariaDB
    ├── redis.ts             BunRedisAdapter — Bun built-in RedisClient
    ├── memory.ts            MemoryAdapter — in-process, zero deps
    ├── schema.ts            Dialect-aware DDL + SQL helpers
    └── index.ts             Adapter barrel
```

---

## Links

- **npm:** https://www.npmjs.com/package/@rasungod/bunmq
- **GitHub:** https://github.com/ra-sun-god/bunmq
- **Issues:** https://github.com/ra-sun-god/bunmq/issues

---

## License

MIT © Razak
