// ─────────────────────────────────────────────────────────────────────────────
// bunmq — Advanced message queue for Bun
//
// Supported backends (all via Bun's built-in SQL client):
//   SQLite     new BunSQLAdapter()                           — default, zero config
//   PostgreSQL new BunSQLAdapter({ url: "postgres://..." })
//   MySQL      new BunSQLAdapter({ url: "mysql://..." })
//   MariaDB    new BunSQLAdapter({ url: "mysql://..." })     — same wire protocol
//   Memory     new MemoryAdapter()                           — in-process, for tests
// ─────────────────────────────────────────────────────────────────────────────

// Core
export { BunMQ } from "./mq.ts";
export type { BunMQOptions } from "./mq.ts";

// Adapters
export { BunSQLAdapter, sqlite, postgres, mysql, mariadb } from "./adapters/bun-sql.ts";
export type { BunSQLOptions, BunSQLDialect } from "./adapters/bun-sql.ts";
export { MemoryAdapter } from "./adapters/memory.ts";
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
