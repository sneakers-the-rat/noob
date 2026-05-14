/**
 * Raw display — JSON-stringify the latest value, or describe its shape if
 * it isn't JSON-encodable (ndarray, bytes, repr fallback).
 */

import type { ValueRecord } from "../../../run/protocol.ts";
import type { DisplayHandle, DisplayProps } from "./types.ts";

export const handles: DisplayHandle[] = [{ id: "in", label: "" }];

export const shortLabel = "RAW";
export const title = "raw value";

export function canRender(_rec: ValueRecord | undefined): boolean {
  // raw can show anything
  return true;
}

export default function RawDisplay({ records }: DisplayProps) {
  const recs = records.in ?? [];
  if (recs.length === 0) {
    return <div className="runner-display-raw">(no events yet)</div>;
  }
  const latest = recs[recs.length - 1];
  let body: string;
  if (latest.value.kind === "json") {
    body = JSON.stringify(latest.value.data, null, 2);
  } else if (latest.value.kind === "repr") {
    body = latest.value.data;
  } else if (latest.value.kind === "ndarray") {
    body = `ndarray shape=${JSON.stringify(latest.value.shape)} dtype=${latest.value.dtype} bytes=${latest.payload?.byteLength ?? 0}`;
  } else {
    body = `bytes size=${latest.value.size}`;
  }
  return (
    <div className="viewer-raw">
      <div className="runner-display-raw">{body}</div>
      <div className="runner-display-info">
        id={latest.id} · {new Date(latest.timestamp).toLocaleTimeString()}
      </div>
    </div>
  );
}
