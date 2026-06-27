import { SQL } from "bun";
import type { StorageAdapter, AdapterRow, MutationResult } from "./adapter.ts";
import { applySchema } from "./schema.ts";
 
// ─────────────────────────────────────────────────────────────────────────────
// BunSQLAdapter — the single SQL adapter for bunmq, powered by Bun's built-in
// SQL client. Supports SQLite, PostgreSQL, MySQL, and MariaDB through one
// unified code path.
//
// URL schemes:
//   sqlite://:memory:                   in-memory SQLite (default)
//   sqlite:///path/to/queue.db          file-based SQLite
//   postgres://user:pass@host:5432/db   PostgreSQL
//   mysql://user:pass@host:3306/db      MySQL
//   mysql://user:pass@host:3306/db      MariaDB (same wire protocol)
//
// How it works:
//   All SQL uses $name placeholders. namedToPositional() rewrites them to ?
//   and extracts values in order. db.unsafe(sql, values[]) executes them.
//   db.transaction(async tx => {...}) provides atomic dequeue across all backends.
// ─────────────────────────────────────────────────────────────────────────────

export type BunSQLDialect = "sqlite" | "postgresql" | "mysql" | "mariadb";

export interface BunSQLOptions {
  /**
   * Connection URL (default: "sqlite://:memory:")
   *
   *   sqlite://:memory:                 — in-memory SQLite
   *   sqlite:///absolute/queue.db       — file SQLite
   *   postgres://user:pass@host/db      — PostgreSQL
   *   mysql://user:pass@host/db         — MySQL
   *   mysql://user:pass@host/db         — MariaDB
   */
  url?: string;

  /** Explicit dialect override — inferred from URL scheme when omitted */
  dialect?: BunSQLDialect;

  /** Connection pool size for PostgreSQL / MySQL (default: 10) */
  max?: number;

  /** SQLite only: disable WAL mode (WAL is on by default) */
  wal?: boolean;
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

// ─── $name → ? rewriting ─────────────────────────────────────────────────────
// Every occurrence of $name becomes its own ? — SQLite and MySQL require
// exactly as many bound values as there are ? placeholders.

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

// ─── Dialect inference ────────────────────────────────────────────────────────

function inferDialect(url: string): "sqlite" | "postgresql" | "mysql" {
  if (url.startsWith("postgres:") || url.startsWith("postgresql:")) return "postgresql";
  if (url.startsWith("mysql:")    || url.startsWith("mariadb:"))    return "mysql";
  return "sqlite";
}

// ─── Result helper ────────────────────────────────────────────────────────────

function toMutationResult(res: SQLResult): MutationResult {
  return {
    changes:         res.affectedRows ?? res.count ?? 0,
    lastInsertRowid: res.lastInsertRowid ?? undefined,
  };
}

// ─── Build txAdapter helper ───────────────────────────────────────────────────
// Wraps a Bun SQL tx object with the StorageAdapter interface so the MQ core
// can call tx.run / tx.get / tx.all / tx.transaction inside a transaction block.

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
    get: async <T extends AdapterRow = AdapterRow>(sql: string, params: Record<string, unknown> = {}): Promise<T | null> => {
      const { sql: rw, values } = namedToPositional(sql, params);
      const res = await tx.unsafe(rw, values);
      return ((res as unknown as T[])[0]) ?? null;
    },
    all: async <T extends AdapterRow = AdapterRow>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> => {
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

  constructor(opts: BunSQLOptions = {}) {
    this.opts    = opts;
    const url    = opts.url ?? "sqlite://:memory:";
    const infer  = opts.dialect ?? inferDialect(url);
    this.dialect = infer === "mariadb" ? "mysql" : infer;
  }

  async connect(): Promise<void> {
    let url = this.opts.url ?? "sqlite://:memory:";

    // Append SQLite performance options as URL query params
    if (this.dialect === "sqlite") {
      const sep = url.includes("?") ? "&" : "?";
      if (this.opts.wal !== false && !url.includes("journal_mode")) {
        url += `${sep}journal_mode=WAL`;
      }
      if (!url.includes("busy_timeout")) {
        url += `&busy_timeout=5000`;
      }
    }

    const sqlOpts: Record<string, unknown> = {};
    if (this.opts.max) sqlOpts.max = this.opts.max;

    this._db = new SQL(url, sqlOpts) as unknown as BunSQLInstance;

    // SQLite-only PRAGMAs that can't go in the URL
    if (this.dialect === "sqlite") {
      await this._db.unsafe("PRAGMA synchronous = NORMAL");
      await this._db.unsafe("PRAGMA cache_size = -32000");
      await this._db.unsafe("PRAGMA temp_store = MEMORY");
      await this._db.unsafe("PRAGMA mmap_size = 134217728");
      await this._db.unsafe("PRAGMA foreign_keys = ON");
      await this._db.unsafe("PRAGMA auto_vacuum = INCREMENTAL");
    }
  }

  async run(sql: string, params: Record<string, unknown> = {}): Promise<MutationResult> {
    const { sql: rw, values } = namedToPositional(sql, params);
    return toMutationResult(await this._db.unsafe(rw, values));
  }

  async get<T extends AdapterRow = AdapterRow>(
    sql: string,
    params: Record<string, unknown> = {}
  ): Promise<T | null> {
    const { sql: rw, values } = namedToPositional(sql, params);
    const res = await this._db.unsafe(rw, values);
    return (res as T[])[0] ?? null;
  }

  async all<T extends AdapterRow = AdapterRow>(
    sql: string,
    params: Record<string, unknown> = {}
  ): Promise<T[]> {
    const { sql: rw, values } = namedToPositional(sql, params);
    return (await this._db.unsafe(rw, values)) as T[];
  }

  async transaction<T>(fn: (adapter: StorageAdapter) => Promise<T>): Promise<T> {
    return this._db.transaction((tx) => fn(buildTxAdapter(tx, this)));
  }

  async migrate(enableLogs: boolean): Promise<void> {
    await applySchema(this, enableLogs);
  }

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

/** In-memory or file SQLite via Bun SQL */
export function sqlite(url = "sqlite://:memory:", opts: Omit<BunSQLOptions, "url"> = {}): BunSQLAdapter {
  return new BunSQLAdapter({ ...opts, url, dialect: "sqlite" });
}

/** PostgreSQL via Bun SQL */
export function postgres(url: string, opts: Omit<BunSQLOptions, "url" | "dialect"> = {}): BunSQLAdapter {
  return new BunSQLAdapter({ ...opts, url, dialect: "postgresql" });
}

/** MySQL via Bun SQL */
export function mysql(url: string, opts: Omit<BunSQLOptions, "url" | "dialect"> = {}): BunSQLAdapter {
  return new BunSQLAdapter({ ...opts, url, dialect: "mysql" });
}

/** MariaDB via Bun SQL (same wire protocol as MySQL) */
export const mariadb = mysql;
