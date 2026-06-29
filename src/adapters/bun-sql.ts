import { SQL } from "bun";
import type { StorageAdapter, AdapterRow, MutationResult } from "./adapter.ts";
import { applySchema } from "./schema.ts";

// ─────────────────────────────────────────────────────────────────────────────
// BunSQLAdapter — unified SQL adapter using Bun's built-in SQL client.
//
// Supports SQLite, PostgreSQL, MySQL, and MariaDB through one code path.
//
// URL schemes:
//   sqlite://:memory:                 in-memory SQLite (default)
//   sqlite:///path/to/queue.db        file SQLite
//   postgres://user:pass@host:5432/db PostgreSQL
//   mysql://user:pass@host:3306/db    MySQL / MariaDB
//
// SQLite locking:
//   WAL mode + busy_timeout handle concurrent readers, but when multiple
//   processes share the same file, writes can still collide. The adapter
//   automatically retries SQLITE_BUSY / "database is locked" errors with
//   exponential backoff up to `maxRetries` attempts.
// ─────────────────────────────────────────────────────────────────────────────

export type BunSQLDialect = "sqlite" | "postgresql" | "mysql" | "mariadb";

export interface BunSQLOptions {
  /**
   * Connection URL (default: "sqlite://:memory:")
   *
   *   sqlite://:memory:              in-memory SQLite
   *   sqlite:///absolute/queue.db    file-based SQLite
   *   postgres://user:pass@host/db   PostgreSQL
   *   mysql://user:pass@host/db      MySQL / MariaDB
   */
  url?: string;

  /** Explicit dialect override — inferred from URL scheme when omitted */
  dialect?: BunSQLDialect;

  /** Connection pool size for PostgreSQL / MySQL (default: 10) */
  max?: number;

  /** SQLite only: disable WAL mode (default: true = WAL on) */
  wal?: boolean;

  /**
   * SQLite only: busy_timeout in ms — how long SQLite itself waits for a
   * lock before returning SQLITE_BUSY (default: 10_000).
   * Set higher when many processes share the same file.
   */
  busyTimeout?: number;

  /**
   * SQLite only: max retries on "database is locked" errors (default: 5).
   * Retries use exponential backoff starting at `retryDelay` ms.
   */
  maxRetries?: number;

  /**
   * SQLite only: base retry delay ms (default: 50).
   * Actual delay = retryDelay * 2^attempt + random jitter.
   */
  retryDelay?: number;
}

// ─── Bun SQL instance shape ───────────────────────────────────────────────────

type BunSQLInstance = {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<SQLResult>;
  unsafe(sql: string, values?: unknown[]): Promise<SQLResult>;
  transaction<T>(fn: (tx: BunSQLInstance) => Promise<T>): Promise<T>;
  close(): Promise<void>;
  readonly closed: boolean;
};

type SQLResult = unknown[] & {
  count: number;
  command: string;
  lastInsertRowid: number | bigint | null;
  affectedRows: number | null;
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Rewrite $name → ? (one ? per occurrence, no deduplication) */
function namedToPositional(
  sql: string,
  params: Record<string, unknown>
): { sql: string; values: unknown[] } {
  const values: unknown[] = [];
  const out = sql.replace(/\$([A-Za-z_][A-Za-z0-9_]*)/g, (_m, name) => {
    values.push(params[name] ?? params[`$${name}`] ?? null);
    return "?";
  });
  return { sql: out, values };
}

function inferDialect(url: string): "sqlite" | "postgresql" | "mysql" {
  if (url.startsWith("postgres:") || url.startsWith("postgresql:")) return "postgresql";
  if (url.startsWith("mysql:")    || url.startsWith("mariadb:"))    return "mysql";
  return "sqlite";
}

function toMutationResult(res: SQLResult): MutationResult {
  return {
    changes:         res.affectedRows ?? res.count ?? 0,
    lastInsertRowid: res.lastInsertRowid ?? undefined,
  };
}

/** Returns true if the error is a SQLite "database is locked" error */
function isSQLiteLocked(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return (
    msg.includes("database is locked") ||
    msg.includes("sqlite_busy") ||
    msg.includes("sqlite_locked") ||
    (err as { code?: string }).code === "SQLITE_BUSY" ||
    (err as { code?: string }).code === "SQLITE_LOCKED"
  );
}

/** Exponential backoff with jitter */
async function sleep(baseMs: number, attempt: number): Promise<void> {
  const jitter = Math.random() * baseMs;
  const delay  = Math.min(baseMs * Math.pow(2, attempt) + jitter, 5_000);
  await new Promise(resolve => setTimeout(resolve, delay));
}

// ─── txAdapter factory ────────────────────────────────────────────────────────

function buildTxAdapter(tx: BunSQLInstance, outer: StorageAdapter): StorageAdapter {
  const txAdapter: StorageAdapter = {
    dialect:  outer.dialect,
    connect:  outer.connect.bind(outer),
    close:    outer.close.bind(outer),
    migrate:  outer.migrate.bind(outer),

    run: async (sql, params = {}) => {
      const { sql: rw, values } = namedToPositional(sql, params);
      return toMutationResult(await tx.unsafe(rw, values));
    },
    get: async <T extends AdapterRow = AdapterRow>(
      sql: string,
      params: Record<string, unknown> = {}
    ): Promise<T | null> => {
      const { sql: rw, values } = namedToPositional(sql, params);
      const res = await tx.unsafe(rw, values);
      return ((res as unknown as T[])[0]) ?? null;
    },
    all: async <T extends AdapterRow = AdapterRow>(
      sql: string,
      params: Record<string, unknown> = {}
    ): Promise<T[]> => {
      const { sql: rw, values } = namedToPositional(sql, params);
      return (await tx.unsafe(rw, values)) as unknown as T[];
    },
    transaction: (fn2) => fn2(txAdapter),
  };
  return txAdapter;
}

// ─────────────────────────────────────────────────────────────────────────────
// BunSQLAdapter
// ─────────────────────────────────────────────────────────────────────────────

export class BunSQLAdapter implements StorageAdapter {
  readonly dialect: "sqlite" | "postgresql" | "mysql";
  private readonly opts: BunSQLOptions;
  private _db!: BunSQLInstance;

  // SQLite lock-retry config
  private readonly _maxRetries: number;
  private readonly _retryDelay: number;

  constructor(opts: BunSQLOptions = {}) {
    this.opts        = opts;
    const url        = opts.url ?? "sqlite://:memory:";
    const infer      = opts.dialect ?? inferDialect(url);
    this.dialect     = infer === "mariadb" ? "mysql" : infer;
    this._maxRetries = opts.maxRetries ?? 5;
    this._retryDelay = opts.retryDelay ?? 50;
  }

  async connect(): Promise<void> {
    let url = this.opts.url ?? "sqlite://:memory:";

    if (this.dialect === "sqlite") {
      const sep         = url.includes("?") ? "&" : "?";
      const busyTimeout = this.opts.busyTimeout ?? 10_000;

      if (this.opts.wal !== false && !url.includes("journal_mode")) {
        url += `${sep}journal_mode=WAL`;
      }
      if (!url.includes("busy_timeout")) {
        url += `&busy_timeout=${busyTimeout}`;
      }
    }

    const sqlOpts: Record<string, unknown> = {};
    if (this.opts.max) sqlOpts.max = this.opts.max;

    this._db = new SQL(url, sqlOpts) as unknown as BunSQLInstance;

    if (this.dialect === "sqlite") {
      await this._db.unsafe("PRAGMA synchronous = NORMAL");
      await this._db.unsafe("PRAGMA cache_size = -32000");
      await this._db.unsafe("PRAGMA temp_store = MEMORY");
      await this._db.unsafe("PRAGMA mmap_size = 134217728");
      await this._db.unsafe("PRAGMA foreign_keys = ON");
      await this._db.unsafe("PRAGMA auto_vacuum = INCREMENTAL");
      await this._db.unsafe("PRAGMA wal_autocheckpoint = 1000");
    }
  }

  // ─── Retry wrapper ────────────────────────────────────────────────────────
  // Only SQLite can return "database is locked". For other dialects we pass
  // through without any retry overhead.

  private async _withRetry<T>(fn: () => Promise<T>): Promise<T> {
    if (this.dialect !== "sqlite") return fn();

    let lastErr: unknown;
    for (let attempt = 0; attempt <= this._maxRetries; attempt++) {
      try {
        return await fn();
      } catch (err) {
        if (isSQLiteLocked(err) && attempt < this._maxRetries) {
          lastErr = err;
          await sleep(this._retryDelay, attempt);
          continue;
        }
        throw err;
      }
    }
    throw lastErr;
  }

  // ─── Core query methods ───────────────────────────────────────────────────

  async run(sql: string, params: Record<string, unknown> = {}): Promise<MutationResult> {
    return this._withRetry(async () => {
      const { sql: rw, values } = namedToPositional(sql, params);
      return toMutationResult(await this._db.unsafe(rw, values));
    });
  }

  async get<T extends AdapterRow = AdapterRow>(
    sql: string,
    params: Record<string, unknown> = {}
  ): Promise<T | null> {
    return this._withRetry(async () => {
      const { sql: rw, values } = namedToPositional(sql, params);
      const res = await this._db.unsafe(rw, values);
      return ((res as unknown as T[])[0]) ?? null;
    });
  }

  async all<T extends AdapterRow = AdapterRow>(
    sql: string,
    params: Record<string, unknown> = {}
  ): Promise<T[]> {
    return this._withRetry(async () => {
      const { sql: rw, values } = namedToPositional(sql, params);
      return (await this._db.unsafe(rw, values)) as unknown as T[];
    });
  }

  async transaction<T>(fn: (adapter: StorageAdapter) => Promise<T>): Promise<T> {
    return this._withRetry(() =>
      this._db.transaction((tx) => fn(buildTxAdapter(tx, this)))
    );
  }

  // ─── Schema ───────────────────────────────────────────────────────────────

  async migrate(enableLogs: boolean): Promise<void> {
    await applySchema(this, enableLogs);
  }

  // ─── Close ────────────────────────────────────────────────────────────────

  async close(): Promise<void> {
    if (this._db && !this._db.closed) {
      await this._db.close();
    }
  }

  /** Direct access to the underlying Bun SQL instance */
  get sql(): BunSQLInstance {
    return this._db;
  }
}

// ─── Convenience factories ────────────────────────────────────────────────────

/** In-memory or file SQLite */
export function sqlite(url = "sqlite://:memory:", opts: Omit<BunSQLOptions, "url"> = {}): BunSQLAdapter {
  return new BunSQLAdapter({ ...opts, url, dialect: "sqlite" });
}

/** PostgreSQL */
export function postgres(url: string, opts: Omit<BunSQLOptions, "url" | "dialect"> = {}): BunSQLAdapter {
  return new BunSQLAdapter({ ...opts, url, dialect: "postgresql" });
}

/** MySQL */
export function mysql(url: string, opts: Omit<BunSQLOptions, "url" | "dialect"> = {}): BunSQLAdapter {
  return new BunSQLAdapter({ ...opts, url, dialect: "mysql" });
}

/** MariaDB (same wire protocol as MySQL) */
export const mariadb = mysql;
