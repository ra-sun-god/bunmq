// ─────────────────────────────────────────────────────────────────────────────
// bunmq adapters — public barrel
// ─────────────────────────────────────────────────────────────────────────────

// Primary SQL adapter — SQLite, PostgreSQL, MySQL, MariaDB via Bun SQL
export { BunSQLAdapter, sqlite, postgres, mysql, mariadb } from "./bun-sql.ts";
export type { BunSQLOptions, BunSQLDialect } from "./bun-sql.ts";

// Redis adapter — Bun's built-in RedisClient
export { BunRedisAdapter } from "./redis.ts";
export type { BunRedisOptions } from "./redis.ts";

// In-process adapter — zero deps, ideal for unit tests
export { MemoryAdapter } from "./memory.ts";

// Shared interface
export type { StorageAdapter, AdapterRow, MutationResult } from "./adapter.ts";

// Schema helpers — useful for custom adapter authors
export {
  applySchema,
  insertOrIgnoreSQL,
  upsertSQL,
  rateBucketUpsertSQL,
  throughputSQL,
  jsonArrayContainsSQL,
} from "./schema.ts";
