import type { ValueRecord } from "../protocol.ts";

export function RawDisplay({ records }: { records: ValueRecord[] }) {
  if (records.length === 0) {
    return <div className="runner-display-raw">(no events yet)</div>;
  }
  const latest = records[records.length - 1];
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
    <>
      <div className="runner-display-raw">{body}</div>
      <div className="runner-display-info">
        id={latest.id} · {new Date(latest.timestamp).toLocaleTimeString()} ·{" "}
        epoch={JSON.stringify(latest.epoch)}
      </div>
    </>
  );
}

export function canRenderRaw(): boolean {
  return true;
}
