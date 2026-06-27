// ─────────────────────────────────────────────────────────────────────────────
// Schema builder — dialect-aware DDL for bunmq
// All dialects: sqlite | postgresql | mysql (covers MariaDB) | memory
// ─────────────────────────────────────────────────────────────────────────────

import type { StorageAdapter } from "./adapter.ts";

type Dialect = StorageAdapter["dialect"];

// ─── Column type helpers ──────────────────────────────────────────────────────

const ts   = (d: Dialect) => d === "sqlite" || d === "memory" ? "INTEGER" : "BIGINT";
const json = (d: Dialect) => d === "postgresql" ? "JSONB" : d === "mysql" ? "JSON" : "TEXT";
const bool = (d: Dialect) => d === "postgresql" ? "BOOLEAN" : d === "mysql" ? "TINYINT(1)" : "INTEGER";
const boolF = (d: Dialect) => d === "postgresql" ? "false" : "0";
const autoPK = (d: Dialect) =>
  d === "postgresql" ? "BIGSERIAL PRIMARY KEY" :
  d === "mysql"      ? "BIGINT PRIMARY KEY AUTO_INCREMENT" :
                       "INTEGER PRIMARY KEY AUTOINCREMENT";

// ─── Table DDL ────────────────────────────────────────────────────────────────

export function jobsTableSQL(d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_jobs (
  id            TEXT        NOT NULL,
  queue         TEXT        NOT NULL,
  name          TEXT        NOT NULL DEFAULT 'default',
  payload       ${json(d)}  NOT NULL,
  status        TEXT        NOT NULL DEFAULT 'pending',
  priority      INTEGER     NOT NULL DEFAULT 2,
  attempts      INTEGER     NOT NULL DEFAULT 0,
  max_attempts  INTEGER     NOT NULL DEFAULT 3,
  delay         INTEGER     NOT NULL DEFAULT 0,
  backoff       ${json(d)}  NOT NULL,
  timeout       INTEGER     NOT NULL DEFAULT 0,
  scheduled_at  ${ts(d)}    NOT NULL,
  started_at    ${ts(d)},
  completed_at  ${ts(d)},
  failed_at     ${ts(d)},
  dead_at       ${ts(d)},
  next_run_at   ${ts(d)},
  created_at    ${ts(d)}    NOT NULL,
  updated_at    ${ts(d)}    NOT NULL,
  lock_until    ${ts(d)},
  progress      INTEGER     NOT NULL DEFAULT 0,
  result        ${json(d)},
  error         TEXT,
  stack_trace   TEXT,
  tags          ${json(d)}  NOT NULL,
  meta          ${json(d)}  NOT NULL,
  repeat_config ${json(d)},
  repeat_job_id TEXT,
  ttl           INTEGER     NOT NULL DEFAULT 0,
  dedup_key     TEXT,
  PRIMARY KEY (id)
)`.trim();
}

export function rateLimitTableSQL(_d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_rate_buckets (
  queue      TEXT   NOT NULL,
  window_key BIGINT NOT NULL,
  count      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, window_key)
)`.trim();
}

export function repeatJobsTableSQL(d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_repeat_jobs (
  id          TEXT        NOT NULL PRIMARY KEY,
  queue       TEXT        NOT NULL,
  name        TEXT        NOT NULL,
  payload     ${json(d)}  NOT NULL,
  options     ${json(d)}  NOT NULL,
  repeat_cfg  ${json(d)}  NOT NULL,
  next_run_at ${ts(d)}    NOT NULL,
  last_run_at ${ts(d)},
  run_count   INTEGER     NOT NULL DEFAULT 0,
  created_at  ${ts(d)}    NOT NULL,
  updated_at  ${ts(d)}    NOT NULL,
  paused      ${bool(d)}  NOT NULL DEFAULT ${boolF(d)}
)`.trim();
}

export function queuePauseTableSQL(d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_queue_pause (
  queue      TEXT       NOT NULL PRIMARY KEY,
  paused     ${bool(d)} NOT NULL DEFAULT ${boolF(d)},
  updated_at ${ts(d)}   NOT NULL
)`.trim();
}

export function metricsTableSQL(d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_metrics (
  id          ${autoPK(d)},
  queue       TEXT    NOT NULL,
  status      TEXT    NOT NULL,
  duration    INTEGER,
  recorded_at ${ts(d)} NOT NULL
)`.trim();
}

export function jobLogsTableSQL(d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_job_logs (
  id         ${autoPK(d)},
  job_id     TEXT NOT NULL,
  queue      TEXT NOT NULL,
  level      TEXT NOT NULL DEFAULT 'info',
  message    TEXT NOT NULL,
  created_at ${ts(d)} NOT NULL
)`.trim();
}

export function metaTableSQL(_d: Dialect): string {
  return `
CREATE TABLE IF NOT EXISTS bunmq_meta (
  key   TEXT NOT NULL PRIMARY KEY,
  value TEXT NOT NULL
)`.trim();
}

// ─── Index DDL ────────────────────────────────────────────────────────────────

export function indexesSQL(d: Dialect): string[] {
  // MySQL doesn't support partial indexes (WHERE clause on CREATE INDEX)
  const partial = (expr: string) => d === "mysql" ? "" : ` WHERE ${expr}`;
  const ine     = d === "mysql" ? "" : "IF NOT EXISTS ";

  return [
    `CREATE INDEX ${ine}bunmq_jobs_queue_status ON bunmq_jobs (queue, status, priority, scheduled_at)`,
    `CREATE INDEX ${ine}bunmq_jobs_status_next  ON bunmq_jobs (status, next_run_at)`,
    `CREATE INDEX ${ine}bunmq_jobs_scheduled    ON bunmq_jobs (scheduled_at)${partial("status = 'scheduled'")}`,
    `CREATE INDEX ${ine}bunmq_jobs_dedup        ON bunmq_jobs (dedup_key)${partial("dedup_key IS NOT NULL")}`,
    `CREATE INDEX ${ine}bunmq_jobs_lock         ON bunmq_jobs (lock_until)${partial("status = 'active'")}`,
    `CREATE INDEX ${ine}bunmq_jobs_queue_idx    ON bunmq_jobs (queue)`,
    `CREATE INDEX ${ine}bunmq_jobs_repeat       ON bunmq_jobs (repeat_job_id)${partial("repeat_job_id IS NOT NULL")}`,
  ];
}

// ─── Dialect-aware statement builders ────────────────────────────────────────

/** INSERT … ON CONFLICT DO NOTHING — idempotent seed (e.g. schema version) */
export function insertOrIgnoreSQL(d: Dialect, table: string, cols: string[], params: string[]): string {
  const c = cols.join(", ");
  const v = params.map(p => `$${p}`).join(", ");
  if (d === "mysql")      return `INSERT IGNORE INTO ${table} (${c}) VALUES (${v})`;
  if (d === "postgresql") return `INSERT INTO ${table} (${c}) VALUES (${v}) ON CONFLICT DO NOTHING`;
  return                         `INSERT OR IGNORE INTO ${table} (${c}) VALUES (${v})`;
}

/** Upsert — INSERT … ON CONFLICT (col) DO UPDATE */
export function upsertSQL(
  d:            Dialect,
  table:        string,
  cols:         string[],
  paramNames:   string[],
  conflictCol:  string,
  updateCols:   string[],
  updateParams: string[]
): string {
  const c  = cols.join(", ");
  const v  = paramNames.map(p => `$${p}`).join(", ");
  const up = updateCols.map((col, i) => `${col}=$${updateParams[i]}`).join(", ");
  if (d === "mysql") {
    return `INSERT INTO ${table} (${c}) VALUES (${v}) ON DUPLICATE KEY UPDATE ${up}`;
  }
  if (d === "postgresql") {
    return `INSERT INTO ${table} (${c}) VALUES (${v}) ON CONFLICT (${conflictCol}) DO UPDATE SET ${up}`;
  }
  return `INSERT INTO ${table} (${c}) VALUES (${v}) ON CONFLICT (${conflictCol}) DO UPDATE SET ${up}`;
}

/** Rate-bucket increment upsert */
export function rateBucketUpsertSQL(d: Dialect): string {
  if (d === "mysql") {
    return `INSERT INTO bunmq_rate_buckets (queue, window_key, count) VALUES ($q, $wk, 1)
            ON DUPLICATE KEY UPDATE count = count + 1`;
  }
  if (d === "postgresql") {
    return `INSERT INTO bunmq_rate_buckets (queue, window_key, count) VALUES ($q, $wk, 1)
            ON CONFLICT (queue, window_key) DO UPDATE SET count = bunmq_rate_buckets.count + 1`;
  }
  return `INSERT INTO bunmq_rate_buckets (queue, window_key, count) VALUES ($q, $wk, 1)
          ON CONFLICT (queue, window_key) DO UPDATE SET count = count + 1`;
}

/** Throughput subquery — MySQL needs an alias on the derived table */
export function throughputSQL(d: Dialect): string {
  const inner = `SELECT status,
    CASE WHEN recorded_at >= $oneMin THEN 'minute' ELSE 'hour' END as period
    FROM bunmq_metrics WHERE queue = $queue AND recorded_at >= $oneHour`;
  const alias = d === "mysql" ? "AS t" : "t";
  return `SELECT status, COUNT(*) as count, period FROM (${inner}) ${alias} GROUP BY status, period`;
}

/** JSON array contains check — each dialect uses a different operator */
export function jsonArrayContainsSQL(d: Dialect, col: string, paramName: string): string {
  if (d === "postgresql") return `${col} @> jsonb_build_array($${paramName}::text)`;
  if (d === "mysql")      return `JSON_CONTAINS(${col}, JSON_QUOTE($${paramName}))`;
  // SQLite / memory
  return `EXISTS (SELECT 1 FROM json_each(${col}) WHERE value = $${paramName})`;
}

// ─── Full migration ───────────────────────────────────────────────────────────

export async function applySchema(adapter: StorageAdapter, enableLogs: boolean): Promise<void> {
  const d = adapter.dialect;

  const tables = [
    metaTableSQL(d),
    jobsTableSQL(d),
    rateLimitTableSQL(d),
    repeatJobsTableSQL(d),
    queuePauseTableSQL(d),
    metricsTableSQL(d),
  ];
  if (enableLogs) tables.push(jobLogsTableSQL(d));

  for (const sql of tables) {
    await adapter.run(sql);
  }

  for (const sql of indexesSQL(d)) {
    try { await adapter.run(sql); } catch { /* already exists */ }
  }

  await adapter.run(
    insertOrIgnoreSQL(d, "bunmq_meta", ["key", "value"], ["key", "value"]),
    { key: "schema_version", value: "1" }
  );
}
