/**
 * RunStore — central state for run lifecycle and event streams.
 *
 * Plain TS class with tiny per-key subscription tables so React components
 * can attach via `useSyncExternalStore` and only re-render when *their*
 * slice changes. We deliberately avoid a global event-emitter on every
 * frame: a 30fps signal would re-render every component listening to the
 * run state.
 *
 * Three subscription scopes:
 *
 * - `subscribeState(listener)`     — RunState transitions
 * - `subscribeNode(node_id, ...)`  — flash a node when any of its signals fire
 * - `subscribeSignal(key, ...)`    — full ValueRecord stream + history
 */

import {
  type ClientMessage,
  type EventMeta,
  type RunState,
  type ServerMessage,
  type ValueRecord,
  WSReader,
} from "./protocol.ts";

const SIGNAL_BUFFER_LIMIT = 500;
const HISTORY_PAGE_DEFAULT = 50;

export type SignalKey = string; // `${node_id}.${signal}`

export function signalKey(node_id: string, signal: string): SignalKey {
  return `${node_id}.${signal}`;
}

export interface SignalBuffer {
  /**
   * Newest-last; capped at SIGNAL_BUFFER_LIMIT.
   *
   * MUST be replaced (new array) on every mutation, not appended in place —
   * `useSyncExternalStore` does an Object.is comparison on the snapshot, and
   * an in-place push would keep the same reference and skip re-renders.
   */
  records: ValueRecord[];
  /** Subscriber count for this signal — store opens/closes the WS sub when this transitions 0<->1. */
  refcount: number;
  /** Outstanding history requests. */
  pendingHistory: Map<
    string,
    {
      collected: ValueRecord[];
      remaining: number;
      resolve: (records: ValueRecord[]) => void;
      reject: (err: Error) => void;
    }
  >;
}

export interface StatusSnapshot {
  state: RunState;
  tube_id: string | null;
  error: string | null;
}

export class RunStore {
  private ws: WebSocket | null = null;
  private reader: WSReader;
  private outbox: ClientMessage[] = [];

  status: StatusSnapshot = {
    state: "uninitialized",
    tube_id: null,
    error: null,
  };

  /** Last activity timestamp (ms since epoch) per node. */
  private nodeActivity = new Map<string, number>();
  private signalBuffers = new Map<SignalKey, SignalBuffer>();

  private stateListeners = new Set<() => void>();
  private nodeListeners = new Map<string, Set<() => void>>();
  private signalListeners = new Map<SignalKey, Set<() => void>>();

  /**
   * Snapshot of "events seen since connect" + last-event metadata, used by
   * the toolbar debug pill. Replaced (new object) on each event so
   * useSyncExternalStore's Object.is check actually triggers a re-render.
   */
  telemetry: {
    count: number;
    last: { node_id: string; signal: string; ts: number } | null;
  } = { count: 0, last: null };
  private telemetryListeners = new Set<() => void>();

  tubeId: string;

  constructor(tubeId: string) {
    this.tubeId = tubeId;
    this.reader = new WSReader((msg) => this.dispatch(msg));
  }

  // ---- connection ----------------------------------------------------

  connect(): void {
    if (this.ws) return;
    const url = `${location.protocol === "https:" ? "wss" : "ws"}://${
      location.host
    }/run/${encodeURIComponent(this.tubeId)}/events`;
    console.info("[noob] events WS connecting:", url);
    const ws = new WebSocket(url);
    ws.binaryType = "arraybuffer";
    ws.addEventListener("open", () => {
      console.info("[noob] events WS open");
      this.flushOutbox();
    });
    ws.addEventListener("message", (ev) => {
      if (typeof ev.data === "string") {
        this.reader.onText(ev.data);
      } else {
        this.reader.onBinary(ev.data);
      }
    });
    ws.addEventListener("close", (ev) => {
      console.info("[noob] events WS closed", ev.code, ev.reason);
      this.ws = null;
    });
    ws.addEventListener("error", (e) => {
      console.error("[noob] events WS error", e);
    });
    this.ws = ws;
  }

  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  private send(msg: ClientMessage): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    } else {
      this.outbox.push(msg);
    }
  }

  private flushOutbox(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    for (const msg of this.outbox) {
      this.ws.send(JSON.stringify(msg));
    }
    this.outbox = [];
  }

  // ---- REST control --------------------------------------------------

  async runInit(): Promise<void> {
    await this.post(`init`);
  }
  async runDeinit(): Promise<void> {
    await this.post(`deinit`);
  }
  async runStart(): Promise<void> {
    await this.post(`start`);
  }
  async runStop(): Promise<void> {
    await this.post(`stop`);
  }

  private async post(action: string): Promise<void> {
    const res = await fetch(
      `/run/${encodeURIComponent(this.tubeId)}/${action}`,
      { method: "POST" },
    );
    if (!res.ok) {
      const detail = await res.text().catch(() => res.statusText);
      this.setStatus({
        ...this.status,
        state: "error",
        error: `${action} failed: ${detail}`,
      });
      return;
    }
    const body = (await res.json()) as StatusSnapshot;
    this.setStatus(body);
  }

  // ---- subscriptions ------------------------------------------------

  subscribeState(listener: () => void): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  subscribeNode(node_id: string, listener: () => void): () => void {
    let set = this.nodeListeners.get(node_id);
    if (!set) {
      set = new Set();
      this.nodeListeners.set(node_id, set);
    }
    set.add(listener);
    return () => {
      set!.delete(listener);
      if (set!.size === 0) this.nodeListeners.delete(node_id);
    };
  }

  getNodeActivity(node_id: string): number {
    return this.nodeActivity.get(node_id) ?? 0;
  }

  subscribeSignal(
    node_id: string,
    signal: string,
    listener: () => void,
  ): () => void {
    const key = signalKey(node_id, signal);
    let buf = this.signalBuffers.get(key);
    if (!buf) {
      buf = { records: [], refcount: 0, pendingHistory: new Map() };
      this.signalBuffers.set(key, buf);
    }
    let set = this.signalListeners.get(key);
    if (!set) {
      set = new Set();
      this.signalListeners.set(key, set);
    }
    set.add(listener);

    // first subscriber: ask the server to start sending values
    if (buf.refcount === 0) {
      this.send({ op: "subscribe_values", node_id, signal });
    }
    buf.refcount += 1;

    return () => {
      set!.delete(listener);
      if (set!.size === 0) this.signalListeners.delete(key);
      buf!.refcount -= 1;
      if (buf!.refcount === 0) {
        this.send({ op: "unsubscribe_values", node_id, signal });
      }
    };
  }

  getSignalBuffer(node_id: string, signal: string): SignalBuffer | undefined {
    return this.signalBuffers.get(signalKey(node_id, signal));
  }

  async requestHistory(
    node_id: string,
    signal: string,
    before_id: number | null,
    limit = HISTORY_PAGE_DEFAULT,
  ): Promise<ValueRecord[]> {
    const key = signalKey(node_id, signal);
    let buf = this.signalBuffers.get(key);
    if (!buf) {
      buf = { records: [], refcount: 0, pendingHistory: new Map() };
      this.signalBuffers.set(key, buf);
    }
    const request_id = `h${Date.now()}${Math.random().toString(36).slice(2, 8)}`;
    return new Promise((resolve, reject) => {
      buf!.pendingHistory.set(request_id, {
        collected: [],
        remaining: 0,
        resolve,
        reject,
      });
      this.send({
        op: "history",
        request_id,
        node_id,
        signal,
        before_id,
        limit,
      });
    });
  }

  // ---- dispatch -----------------------------------------------------

  private dispatch(msg: ServerMessage): void {
    switch (msg.type) {
      case "status":
        this.setStatus({
          state: msg.state,
          tube_id: msg.tube_id,
          error: msg.error,
        });
        break;
      case "event":
        this.onEvent(msg);
        break;
      case "value":
        this.onValue(msg);
        break;
      case "history_begin":
        this.onHistoryBegin(msg);
        break;
      case "history_value":
        this.onHistoryValue(msg);
        break;
      case "history_end":
        this.onHistoryEnd(msg);
        break;
      case "error":
        console.error("server error:", msg.detail);
        break;
    }
  }

  private setStatus(s: StatusSnapshot): void {
    this.status = s;
    for (const l of this.stateListeners) l();
  }

  private onEvent(ev: EventMeta): void {
    const now = Date.now();
    this.nodeActivity.set(ev.node_id, now);
    this.telemetry = {
      count: this.telemetry.count + 1,
      last: { node_id: ev.node_id, signal: ev.signal, ts: now },
    };
    const set = this.nodeListeners.get(ev.node_id);
    if (set) for (const l of set) l();
    for (const l of this.telemetryListeners) l();
  }

  subscribeTelemetry(listener: () => void): () => void {
    this.telemetryListeners.add(listener);
    return () => this.telemetryListeners.delete(listener);
  }

  private onValue(rec: ValueRecord): void {
    const key = signalKey(rec.node_id, rec.signal);
    const buf = this.signalBuffers.get(key);
    if (!buf) return; // server raced our unsubscribe — drop
    const next = buf.records.length >= SIGNAL_BUFFER_LIMIT
      ? [...buf.records.slice(buf.records.length - SIGNAL_BUFFER_LIMIT + 1), rec]
      : [...buf.records, rec];
    buf.records = next;
    const set = this.signalListeners.get(key);
    if (set) for (const l of set) l();
  }

  private onHistoryBegin(msg: {
    request_id: string;
    node_id: string;
    signal: string;
    count: number;
  }): void {
    const buf = this.signalBuffers.get(signalKey(msg.node_id, msg.signal));
    const pending = buf?.pendingHistory.get(msg.request_id);
    if (!pending) return;
    pending.remaining = msg.count;
    if (msg.count === 0) {
      buf!.pendingHistory.delete(msg.request_id);
      pending.resolve([]);
    }
  }

  private onHistoryValue(rec: ValueRecord & { request_id: string }): void {
    const buf = this.signalBuffers.get(signalKey(rec.node_id, rec.signal));
    const pending = buf?.pendingHistory.get(rec.request_id);
    if (!pending) return;
    pending.collected.push(rec);
  }

  private onHistoryEnd(msg: { request_id: string }): void {
    for (const buf of this.signalBuffers.values()) {
      const pending = buf.pendingHistory.get(msg.request_id);
      if (pending) {
        buf.pendingHistory.delete(msg.request_id);
        pending.resolve(pending.collected);
        return;
      }
    }
  }
}
