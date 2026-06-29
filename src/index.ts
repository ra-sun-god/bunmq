// ─────────────────────────────────────────────────────────────────────────────
// @rasungod/bunmq — Advanced message queue for Bun
//
// Supported backends:
//   SQLite     new BunSQLAdapter()                            default, zero config
//   PostgreSQL new BunSQLAdapter({ url: "postgres://..." })
//   MySQL      new BunSQLAdapter({ url: "mysql://..." })
//   MariaDB    new BunSQLAdapter({ url: "mysql://..." })      same wire protocol
//   Redis      new BunRedisAdapter({ url: "redis://..." })    Bun built-in RedisClient
//   Memory     new MemoryAdapter()                            in-process, for tests
// ─────────────────────────────────────────────────────────────────────────────

// Core
export { BunMQ } from "./mq.ts";
export type { BunMQOptions } from "./mq.ts";

// SQL adapter (SQLite / PostgreSQL / MySQL / MariaDB)
export { BunSQLAdapter, sqlite, postgres, mysql, mariadb } from "./adapters/bun-sql.ts";
export type { BunSQLOptions, BunSQLDialect } from "./adapters/bun-sql.ts";

// Redis adapter (Bun built-in RedisClient)
export { BunRedisAdapter } from "./adapters/redis.ts";
export type { BunRedisOptions } from "./adapters/redis.ts";

// In-process adapter
export { MemoryAdapter } from "./adapters/memory.ts";

// Adapter interface
export type { StorageAdapter, AdapterRow, MutationResult } from "./adapters/adapter.ts";

// Schema helpers
export {
  applySchema,
  insertOrIgnoreSQL,
  upsertSQL,
  rateBucketUpsertSQL,
  throughputSQL,
  jsonArrayContainsSQL,
} from "./adapters/schema.ts";

// Utilities
export { MQEventEmitter } from "./events.ts";
export { nextCronDate, isValidCron } from "./cron.ts";
export { calcBackoff } from "./backoff.ts";

// Types
export type {
  Job,
  JobStatus,
  JobPriority,
  JobHandler,
  JobContext,
  BackoffConfig,
  BackoffType,
  RepeatConfig,
  QueueOptions,
  AddJobOptions,
  QueueStats,
  GlobalStats,
  BatchResult,
  JobFilter,
  JobLog,
  QueueEvent,
  QueueEventMap,
} from "./types.ts";
export { PRIORITY_VALUES } from "./types.ts";
