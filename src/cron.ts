// ─────────────────────────────────────────────────────────────────────────────
// Minimal cron parser — supports standard 5-field cron expressions
// Fields: minute hour day-of-month month day-of-week
// Supports: *, ranges (1-5), lists (1,3,5), step (*/5, 1-10/2), ?
// ─────────────────────────────────────────────────────────────────────────────

type CronField = number[];

interface ParsedCron {
  minute: CronField;
  hour: CronField;
  dayOfMonth: CronField;
  month: CronField;
  dayOfWeek: CronField;
}

function parseField(expr: string, min: number, max: number): CronField {
  if (expr === "*" || expr === "?") {
    return range(min, max);
  }

  const result = new Set<number>();

  for (const part of expr.split(",")) {
    if (part.includes("/")) {
      const [rangeExpr, stepStr] = part.split("/");
      const step = parseInt(stepStr, 10);
      let from = min;
      let to = max;
      if (rangeExpr !== "*" && rangeExpr !== "?") {
        if (rangeExpr.includes("-")) {
          [from, to] = rangeExpr.split("-").map(Number);
        } else {
          from = parseInt(rangeExpr, 10);
        }
      }
      for (let i = from; i <= to; i += step) {
        result.add(i);
      }
    } else if (part.includes("-")) {
      const [from, to] = part.split("-").map(Number);
      for (let i = from; i <= to; i++) {
        result.add(i);
      }
    } else {
      result.add(parseInt(part, 10));
    }
  }

  return Array.from(result).sort((a, b) => a - b);
}

function range(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => i + from);
}

// Named shortcuts
const SHORTCUTS: Record<string, string> = {
  "@yearly":   "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly":  "0 0 1 * *",
  "@weekly":   "0 0 * * 0",
  "@daily":    "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@hourly":   "0 * * * *",
};

export function parseCron(expr: string): ParsedCron {
  const resolved = SHORTCUTS[expr.toLowerCase()] ?? expr;
  const parts = resolved.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: "${expr}" — expected 5 fields`);
  }
  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
  return {
    minute:     parseField(minute,     0, 59),
    hour:       parseField(hour,       0, 23),
    dayOfMonth: parseField(dayOfMonth, 1, 31),
    month:      parseField(month,      1, 12),
    dayOfWeek:  parseField(dayOfWeek,  0,  6),
  };
}

/** Returns the next Date after `from` that matches the cron expression */
export function nextCronDate(expr: string, from: Date = new Date()): Date {
  const cron = parseCron(expr);

  // Start at next minute
  const d = new Date(from);
  d.setSeconds(0, 0);
  d.setMinutes(d.getMinutes() + 1);

  const limit = new Date(d.getTime() + 4 * 365 * 24 * 60 * 60 * 1000); // 4 years max

  while (d < limit) {
    // Check month (1-based in JS: getMonth() returns 0-11)
    if (!cron.month.includes(d.getMonth() + 1)) {
      d.setMonth(d.getMonth() + 1, 1);
      d.setHours(0, 0, 0, 0);
      continue;
    }
    // Check day of month AND day of week
    const domMatch = cron.dayOfMonth.includes(d.getDate());
    const dowMatch = cron.dayOfWeek.includes(d.getDay());
    const dayMatch = domMatch && dowMatch;
    if (!dayMatch) {
      d.setDate(d.getDate() + 1);
      d.setHours(0, 0, 0, 0);
      continue;
    }
    // Check hour
    if (!cron.hour.includes(d.getHours())) {
      d.setHours(d.getHours() + 1, 0, 0, 0);
      continue;
    }
    // Check minute
    const nextMinute = cron.minute.find(m => m >= d.getMinutes());
    if (nextMinute === undefined) {
      d.setHours(d.getHours() + 1, 0, 0, 0);
      continue;
    }
    if (nextMinute !== d.getMinutes()) {
      d.setMinutes(nextMinute, 0, 0);
    }
    return d;
  }

  throw new Error(`No next date found for cron: "${expr}"`);
}

/** Validate a cron expression without throwing */
export function isValidCron(expr: string): boolean {
  try {
    parseCron(expr);
    return true;
  } catch {
    return false;
  }
}
