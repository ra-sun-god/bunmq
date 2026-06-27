import type { QueueEvent, QueueEventMap } from "./types.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Typed Event Emitter
// ─────────────────────────────────────────────────────────────────────────────

type Listener<T> = (data: T) => void | Promise<void>;

export class MQEventEmitter {
  private listeners = new Map<string, Set<Listener<unknown>>>();
  private onceSet    = new WeakSet<Listener<unknown>>();

  on<K extends QueueEvent>(
    event: K,
    listener: Listener<QueueEventMap[K]>
  ): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener as Listener<unknown>);
    return this;
  }

  once<K extends QueueEvent>(
    event: K,
    listener: Listener<QueueEventMap[K]>
  ): this {
    const wrapper: Listener<unknown> = (data) => {
      this.off(event, wrapper as Listener<QueueEventMap[K]>);
      return listener(data as QueueEventMap[K]);
    };
    this.onceSet.add(wrapper);
    return this.on(event, wrapper as Listener<QueueEventMap[K]>);
  }

  off<K extends QueueEvent>(
    event: K,
    listener: Listener<QueueEventMap[K]>
  ): this {
    this.listeners.get(event)?.delete(listener as Listener<unknown>);
    return this;
  }

  emit<K extends QueueEvent>(event: K, data: QueueEventMap[K]): void {
    const set = this.listeners.get(event);
    if (!set) return;
    for (const listener of set) {
      try {
        const result = listener(data);
        if (result instanceof Promise) {
          result.catch(err => {
            console.error(`[sqlite-mq] Async listener error on "${event}":`, err);
          });
        }
      } catch (err) {
        console.error(`[sqlite-mq] Listener error on "${event}":`, err);
      }
    }
  }

  removeAllListeners(event?: QueueEvent): this {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
    return this;
  }

  listenerCount(event: QueueEvent): number {
    return this.listeners.get(event)?.size ?? 0;
  }
}
