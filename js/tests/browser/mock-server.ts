/**
 * Mock noob server — a vite plugin that speaks the same HTTP + WebSocket
 * protocol as the real Python (Litestar) backend, so the page under test
 * connects via plain `new WebSocket(...)` / `fetch(...)` with no test-only
 * code paths in the production source.
 *
 * Tests drive state through Vitest browser-mode {@link commands}, which run
 * in the Node process where this plugin lives and the mock server's state
 * is held.
 *
 * Protocol surface mirrored (minimal):
 *  - WS  /spec/{tube_id}         : server pushes TubeSpecification JSON
 *  - WS  /run/{tube_id}/events   : server pushes {type:"status"|"event"|"value"} frames
 *                                   + responds to {op:"subscribe_values"|...}
 *  - POST /run/{tube_id}/init|deinit|start|stop : returns status JSON
 *  - GET  /run/{tube_id}/status                  : returns status JSON
 */

import type { IncomingMessage, ServerResponse } from "node:http";
import type { Plugin, ViteDevServer } from "vite";
import { WebSocket, WebSocketServer } from "ws";

export interface ValueEnvelope {
  kind: "json" | "ndarray" | "bytes" | "repr";
  data?: unknown;
  shape?: number[];
  dtype?: string;
  size?: number;
}

export interface EventFrame {
  type: "event";
  id: number;
  node_id: string;
  signal: string;
  epoch: [string, number][];
  timestamp: string;
}

export interface ValueFrame {
  type: "value";
  id: number;
  node_id: string;
  signal: string;
  epoch: [string, number][];
  timestamp: string;
  value: ValueEnvelope;
}

export interface StatusFrame {
  type: "status";
  state: "uninitialized" | "initialized" | "running" | "stopped" | "error";
  tube_id: string | null;
  error: string | null;
}

interface Subscriber {
  ws: WebSocket;
  signals: Set<string>;
}

export class MockServer {
  spec: unknown = null;
  status: StatusFrame = {
    type: "status",
    state: "uninitialized",
    tube_id: null,
    error: null,
  };

  private specSubs = new Set<WebSocket>();
  private eventSubs = new Set<Subscriber>();
  private nextEventId = 1;

  setSpec(spec: unknown): void {
    this.spec = spec;
    const payload = JSON.stringify(spec);
    for (const ws of this.specSubs) {
      if (ws.readyState === WebSocket.OPEN) ws.send(payload);
    }
  }

  setStatus(s: Partial<Omit<StatusFrame, "type">>): void {
    this.status = { ...this.status, ...s };
    const payload = JSON.stringify(this.status);
    for (const sub of this.eventSubs) {
      if (sub.ws.readyState === WebSocket.OPEN) sub.ws.send(payload);
    }
  }

  pushEvent(node_id: string, signal: string, opts: { epoch?: number } = {}): EventFrame {
    const epoch_num = opts.epoch ?? this.nextEventId;
    const frame: EventFrame = {
      type: "event",
      id: this.nextEventId++,
      node_id,
      signal,
      epoch: [["tube", epoch_num]],
      timestamp: new Date().toISOString(),
    };
    const payload = JSON.stringify(frame);
    for (const sub of this.eventSubs) {
      if (sub.ws.readyState === WebSocket.OPEN) sub.ws.send(payload);
    }
    return frame;
  }

  pushValue(
    node_id: string,
    signal: string,
    value: ValueEnvelope,
    opts: { epoch?: number; binary?: Uint8Array | Buffer; eventId?: number } = {},
  ): ValueFrame {
    const id = opts.eventId ?? this.nextEventId++;
    const epoch_num = opts.epoch ?? id;
    const frame: ValueFrame = {
      type: "value",
      id,
      node_id,
      signal,
      epoch: [["tube", epoch_num]],
      timestamp: new Date().toISOString(),
      value,
    };
    const text = JSON.stringify(frame);
    const key = `${node_id}.${signal}`;
    for (const sub of this.eventSubs) {
      if (sub.ws.readyState !== WebSocket.OPEN) continue;
      if (!sub.signals.has(key)) continue;
      sub.ws.send(text);
      if (opts.binary) sub.ws.send(opts.binary);
    }
    return frame;
  }

  /** Push an event + paired numeric value frame (same id + epoch). */
  pushNumeric(
    node_id: string,
    signal: string,
    value: number,
    opts: { epoch?: number } = {},
  ): { id: number; epoch: number } {
    const id = this.nextEventId++;
    const epoch_num = opts.epoch ?? id;
    const evt: EventFrame = {
      type: "event",
      id,
      node_id,
      signal,
      epoch: [["tube", epoch_num]],
      timestamp: new Date().toISOString(),
    };
    const evtText = JSON.stringify(evt);
    for (const sub of this.eventSubs) {
      if (sub.ws.readyState === WebSocket.OPEN) sub.ws.send(evtText);
    }
    this.pushValue(node_id, signal, { kind: "json", data: value }, {
      epoch: epoch_num,
      eventId: id,
    });
    return { id, epoch: epoch_num };
  }

  signalSubscriberCount(node_id: string, signal: string): number {
    const key = `${node_id}.${signal}`;
    let n = 0;
    for (const sub of this.eventSubs) if (sub.signals.has(key)) n++;
    return n;
  }

  eventClientCount(): number {
    return this.eventSubs.size;
  }

  reset(): void {
    this.spec = null;
    this.status = {
      type: "status",
      state: "uninitialized",
      tube_id: null,
      error: null,
    };
    this.nextEventId = 1;
    for (const ws of this.specSubs) {
      try { ws.close(); } catch { /* ignore */ }
    }
    this.specSubs.clear();
    for (const sub of this.eventSubs) {
      try { sub.ws.close(); } catch { /* ignore */ }
    }
    this.eventSubs.clear();
  }

  _addSpecSub(ws: WebSocket): void {
    this.specSubs.add(ws);
    if (this.spec != null && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(this.spec));
    }
    ws.on("close", () => this.specSubs.delete(ws));
  }

  _addEventSub(ws: WebSocket): Subscriber {
    const sub: Subscriber = { ws, signals: new Set() };
    this.eventSubs.add(sub);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(this.status));
    }
    ws.on("close", () => this.eventSubs.delete(sub));
    ws.on("message", (raw) => {
      let msg: unknown;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }
      this._handleClientMessage(sub, msg as Record<string, unknown>);
    });
    return sub;
  }

  private _handleClientMessage(sub: Subscriber, msg: Record<string, unknown>): void {
    const op = msg.op as string | undefined;
    if (op === "subscribe_values") {
      sub.signals.add(`${msg.node_id}.${msg.signal}`);
    } else if (op === "unsubscribe_values") {
      sub.signals.delete(`${msg.node_id}.${msg.signal}`);
    } else if (op === "history") {
      sub.ws.send(
        JSON.stringify({
          type: "history_begin",
          request_id: msg.request_id,
          node_id: msg.node_id,
          signal: msg.signal,
          count: 0,
        }),
      );
      sub.ws.send(
        JSON.stringify({ type: "history_end", request_id: msg.request_id }),
      );
    }
  }
}

declare global {
  // eslint-disable-next-line no-var
  var __noobMockServer: MockServer | undefined;
}

export function getMockServer(): MockServer {
  if (!globalThis.__noobMockServer) {
    globalThis.__noobMockServer = new MockServer();
  }
  return globalThis.__noobMockServer;
}

function jsonResponse(res: ServerResponse, body: unknown, status = 200): void {
  res.statusCode = status;
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(body));
}

const REST_RE = /^\/run\/([^/]+)\/(init|deinit|start|stop|status)$/;
const SPEC_WS_RE = /^\/spec\/([^/]+)$/;
const EVENTS_WS_RE = /^\/run\/([^/]+)\/events$/;

/** Vite plugin: mount mock server REST + WS routes on the dev server. */
export function noobMockServerPlugin(): Plugin {
  return {
    name: "noob-mock-server",
    configureServer(server: ViteDevServer) {
      const mock = getMockServer();

      server.middlewares.use((req, res, next) => {
        const url = req.url ?? "";
        const path = url.split("?")[0];
        const m = REST_RE.exec(path);
        if (!m) return next();
        const [, tube_id, action] = m;
        if (req.method === "POST" && action !== "status") {
          const transitions: Record<string, StatusFrame["state"]> = {
            init: "initialized",
            deinit: "uninitialized",
            start: "running",
            stop: "initialized",
          };
          if (action === "start" && mock.status.state === "uninitialized") {
            return jsonResponse(res, { detail: "tube not initialized" }, 409);
          }
          mock.setStatus({
            state: transitions[action],
            tube_id: action === "deinit" ? null : tube_id,
          });
          return jsonResponse(res, mock.status);
        }
        if (req.method === "GET" && action === "status") {
          return jsonResponse(res, mock.status);
        }
        return jsonResponse(res, { detail: "method not allowed" }, 405);
      });

      const wss = new WebSocketServer({ noServer: true });
      const httpServer = server.httpServer;
      if (!httpServer) return;
      httpServer.on("upgrade", (req: IncomingMessage, socket, head) => {
        const url = req.url ?? "";
        if (SPEC_WS_RE.test(url) || EVENTS_WS_RE.test(url)) {
          wss.handleUpgrade(req, socket, head, (ws) => {
            if (SPEC_WS_RE.test(url)) {
              mock._addSpecSub(ws);
            } else {
              mock._addEventSub(ws);
            }
          });
        }
      });
    },
  };
}
