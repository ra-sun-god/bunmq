// ─────────────────────────────────────────────────────────────────────────────
// StorageAdapter — the contract every backend must satisfy
// ─────────────────────────────────────────────────────────────────────────────

export interface AdapterRow {
  [key: string]: unknown;
}

/** Result of a mutation (INSERT / UPDATE / DELETE) */
export interface MutationResult {
  changes: number;
  lastInsertRowid?: number | bigint;
}

/**
 * All adapters must implement this interface.
 * SQL uses $name placeholders — each adapter rewrites to its native format.
 *
 * Supported dialects:
 *   "sqlite"     — Bun SQL with sqlite:// URL
 *   "postgresql" — Bun SQL with postgres:// URL
 *   "mysql"      — Bun SQL with mysql:// URL (also covers MariaDB)
 *   "memory"     — In-process Map store, zero dependencies
 */
export interface StorageAdapter {
  readonly dialect: "sqlite" | "postgresql" | "mysql" | "memory";

  connect(): Promise<void>;
  run(sql: string, params?: Record<string, unknown>): Promise<MutationResult>;
  get<T extends AdapterRow = AdapterRow>(sql: string, params?: Record<string, unknown>): Promise<T | null>;
  all<T extends AdapterRow = AdapterRow>(sql: string, params?: Record<string, unknown>): Promise<T[]>;
  transaction<T>(fn: (adapter: StorageAdapter) => Promise<T>): Promise<T>;
  migrate(enableLogs: boolean): Promise<void>;
  close(): Promise<void>;
}
