/**
 * Shared types and helpers for the runner WebSocket protocol.
 *
 * The server speaks a small framed protocol:
 *
 *  - A text frame is always a JSON envelope.
 *  - If the envelope describes a value whose `kind` is `ndarray` or `bytes`,
 *    a binary follow-up frame immediately follows in the same WS.
 *
 * The `WSReader` class below pairs text + optional binary frames back together
 * for callers, since browsers deliver them as separate events.
 */

export type RunState =
  | "uninitialized"
  | "initialized"
  | "running"
  | "stopped"
  | "error";

export interface EventMeta {
  id: number;
  node_id: string;
  signal: string;
  epoch: [string, number][];
  timestamp: string;
}

export type ValueEnvelope =
  | { kind: "json"; data: unknown }
  | { kind: "ndarray"; shape: number[]; dtype: string }
  | { kind: "bytes"; size: number }
  | { kind: "repr"; data: string };

export interface ValueRecord extends EventMeta {
  value: ValueEnvelope;
  /** Present iff value.kind is "ndarray" or "bytes". */
  payload?: Uint8Array;
}

export type ServerMessage =
  | ({ type: "status" } & {
      state: RunState;
      tube_id: string | null;
      error: string | null;
    })
  | ({ type: "event" } & EventMeta)
  | ({ type: "value" } & ValueRecord)
  | {
      type: "history_begin";
      request_id: string;
      node_id: string;
      signal: string;
      count: number;
    }
  | ({ type: "history_value"; request_id: string } & ValueRecord)
  | { type: "history_end"; request_id: string }
  | { type: "error"; detail: string };

export type ClientMessage =
  | { op: "subscribe_values"; node_id: string; signal: string }
  | { op: "unsubscribe_values"; node_id: string; signal: string }
  | {
      op: "history";
      request_id: string;
      node_id: string;
      signal: string;
      before_id: number | null;
      limit: number;
    };

function needsBinary(msg: { value?: ValueEnvelope }): boolean {
  const k = msg.value?.kind;
  return k === "ndarray" || k === "bytes";
}

/**
 * Pair text and binary WS frames back together.
 *
 * Server contract: when a message announces an ndarray/bytes value, the
 * binary payload is the very next frame on the wire. Anything else is
 * dispatched immediately.
 */
export class WSReader {
  private pending: ServerMessage | null = null;
  private dispatch: (msg: ServerMessage) => void;

  constructor(dispatch: (msg: ServerMessage) => void) {
    this.dispatch = dispatch;
  }

  onText(text: string): void {
    let parsed: ServerMessage;
    try {
      parsed = JSON.parse(text) as ServerMessage;
    } catch (e) {
      console.error("Bad WS text frame", e, text);
      return;
    }

    if (
      (parsed.type === "value" || parsed.type === "history_value") &&
      needsBinary(parsed)
    ) {
      this.pending = parsed;
      return;
    }
    this.dispatch(parsed);
  }

  onBinary(buf: ArrayBuffer | Blob): void {
    const apply = (bytes: Uint8Array) => {
      if (!this.pending) {
        console.warn("Unexpected binary frame with no pending text");
        return;
      }
      const msg = this.pending as ValueRecord & {
        type: "value" | "history_value";
      };
      msg.payload = bytes;
      this.pending = null;
      this.dispatch(msg as ServerMessage);
    };

    if (buf instanceof ArrayBuffer) {
      apply(new Uint8Array(buf));
    } else {
      // Blob path — Litestar / browsers may default to this.
      buf.arrayBuffer().then((ab) => apply(new Uint8Array(ab)));
    }
  }
}
